#include "proxy-student.h"
#include "gfserver.h"
#include <time.h>



#define MAX_REQUEST_N 512
#define BUFSIZE (6426)

/* How long proxy_shutdown() waits for in-progress requests to stop. */
#define SHUTDOWN_WAIT_MS 2000

/*
 * Shared between the workers and the signal handler, so only accessed with
 * __atomic builtins (lock-free, hence also safe to use in the handler).
 */
static int shutting_down;	/* set once by proxy_shutdown() */
static int in_flight;		/* workers currently inside proxy_request() */

/*
 * How the body of the HTTP response is being handled. The mode is decided
 * on the first call to write_callback, once the response headers are in.
 */
typedef enum {
	BODY_UNDECIDED,	/* no body bytes seen yet */
	BODY_STREAM,	/* Content-Length known: header sent, forward chunks as they arrive */
	BODY_BUFFER,	/* Content-Length unknown: collect the body, send it after the transfer */
	BODY_DISCARD	/* not a 2xx response: drop the error page */
} body_mode_t;

typedef struct {
	gfcontext_t *ctx;
	CURL *curl;
	body_mode_t mode;
	size_t file_len;		/* length announced in the Getfile header (BODY_STREAM) */
	size_t bytes_transferred;	/* bytes forwarded to the client (BODY_STREAM) */
	char *buf;			/* body collected in BODY_BUFFER mode */
	size_t buf_len;
	size_t buf_cap;
} transfer_t;

static int is_success(long http_code){
	return http_code >= 200 && http_code < 300;
}

static int is_shutting_down(void){
	return __atomic_load_n(&shutting_down, __ATOMIC_SEQ_CST);
}

/*
 * libcurl calls this at least once a second during a transfer (and after
 * every chunk received); returning nonzero aborts the transfer.
 */
static int progress_callback(void *clientp, curl_off_t dltotal, curl_off_t dlnow,
		curl_off_t ultotal, curl_off_t ulnow){
	return is_shutting_down();
}

/*
 * Builds server + path, dropping a trailing '/' on server so that
 * "http://host/" + "/a.jpg" doesn't become "http://host//a.jpg".
 * Returns a malloc'd string the caller frees, or NULL on failure.
 */
static char *build_url(const char *server, const char *path){
	size_t server_len = strlen(server);
	size_t path_len = strlen(path);
	char *url;

	if (server_len > 0 && server[server_len - 1] == '/' && path[0] == '/')
		server_len--;

	if (NULL == (url = malloc(server_len + path_len + 1)))
		return NULL;

	memcpy(url, server, server_len);
	memcpy(url + server_len, path, path_len + 1);
	return url;
}

/*
 * libcurl calls this with each piece of the response body. By the first
 * call the status line and headers have been received, so the response
 * code and Content-Length can be queried to decide how to handle the body.
 * Returning anything other than size * nmemb aborts the transfer.
 */
static size_t write_callback(char *data, size_t size, size_t nmemb, void *userp){
	transfer_t *t = userp;
	size_t len = size * nmemb;
	long http_code = 0;
	curl_off_t content_len = -1;
	ssize_t write_len;
	char *new_buf;
	size_t new_cap;

	if (t->mode == BODY_UNDECIDED) {
		curl_easy_getinfo(t->curl, CURLINFO_RESPONSE_CODE, &http_code);
		curl_easy_getinfo(t->curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &content_len);

		if (!is_success(http_code)) {
			t->mode = BODY_DISCARD;
		} else if (content_len >= 0) {
			t->file_len = (size_t) content_len;
			if (gfs_sendheader(t->ctx, GF_OK, t->file_len) < 0)
				return 0;
			t->mode = BODY_STREAM;
		} else {
			t->mode = BODY_BUFFER;
		}
	}

	switch (t->mode) {
	case BODY_STREAM:
		write_len = gfs_send(t->ctx, data, len);
		if (write_len < 0 || (size_t) write_len != len) {
			fprintf(stderr, "handle_with_curl write error\n");
			return 0;
		}
		t->bytes_transferred += len;
		return len;

	case BODY_BUFFER:
		if (t->buf_len + len > t->buf_cap) {
			new_cap = t->buf_cap ? t->buf_cap : BUFSIZE;
			while (new_cap < t->buf_len + len)
				new_cap *= 2;
			if (NULL == (new_buf = realloc(t->buf, new_cap)))
				return 0;
			t->buf = new_buf;
			t->buf_cap = new_cap;
		}
		memcpy(t->buf + t->buf_len, data, len);
		t->buf_len += len;
		return len;

	default:
		return len;
	}
}

/*
 * Fetches server + path over HTTP(S) and forwards it to the Getfile client.
 */
static ssize_t proxy_request(gfcontext_t *ctx, proxy_worker_t *worker, const char *path){
	transfer_t t = { .ctx = ctx, .curl = worker->curl, .mode = BODY_UNDECIDED };
	CURLcode res;
	long http_code = 0;
	ssize_t ret = SERVER_FAILURE;
	ssize_t write_len;
	char *url;

	if (NULL == (url = build_url(worker->server, path)))
		return SERVER_FAILURE;

	/* Clears the previous request's options but keeps open connections
	 * and the DNS cache, so repeat requests skip the TCP/TLS handshake. */
	curl_easy_reset(t.curl);
	curl_easy_setopt(t.curl, CURLOPT_URL, url);
	curl_easy_setopt(t.curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(t.curl, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(t.curl, CURLOPT_MAXREDIRS, 10L);
	/* Signal-based timeouts aren't safe with multiple threads. */
	curl_easy_setopt(t.curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(t.curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(t.curl, CURLOPT_WRITEDATA, &t);
	/* Lets proxy_shutdown() stop a transfer that's in progress. */
	curl_easy_setopt(t.curl, CURLOPT_XFERINFOFUNCTION, progress_callback);
	curl_easy_setopt(t.curl, CURLOPT_NOPROGRESS, 0L);

	res = curl_easy_perform(t.curl);
	curl_easy_getinfo(t.curl, CURLINFO_RESPONSE_CODE, &http_code);

	if (http_code == 404) {
		/* No Getfile header has gone out: the 404 body was discarded. */
		ret = gfs_sendheader(ctx, GF_FILE_NOT_FOUND, 0);
	} else if (res != CURLE_OK) {
		/* Connection failures, or a transfer cut short (possibly after the
		 * header went out, in which case the client sees a short file). */
		fprintf(stderr, "handle_with_curl %s: %s\n", url, curl_easy_strerror(res));
	} else if (!is_success(http_code)) {
		fprintf(stderr, "handle_with_curl %s: HTTP %ld\n", url, http_code);
	} else if (t.mode == BODY_STREAM) {
		if (t.bytes_transferred == t.file_len)
			ret = t.bytes_transferred;
	} else {
		/* Either the body was buffered (no Content-Length) or the
		 * response was empty, in which case the callback never ran. */
		if (gfs_sendheader(ctx, GF_OK, t.buf_len) >= 0) {
			write_len = t.buf_len ? gfs_send(ctx, t.buf, t.buf_len) : 0;
			if (write_len >= 0 && (size_t) write_len == t.buf_len)
				ret = write_len;
		}
	}

	free(t.buf);
	free(url);
	return ret;
}

ssize_t handle_with_curl(gfcontext_t *ctx, const char *path, void* arg){
	int old_cancel_state;
	ssize_t ret = SERVER_FAILURE;

	/*
	 * gfserver_stop() cancels the worker threads. Being cancelled inside
	 * libcurl would leak this request's buffers and leave the reused handle
	 * mid-transfer, and unwinding through these frames trips AddressSanitizer
	 * at thread exit. Defer cancellation until the request is done; it then
	 * takes effect back in the gfserver worker loop.
	 */
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &old_cancel_state);

	/* Count ourselves in before checking the flag, so proxy_shutdown()
	 * either sees this request or this request sees the flag. */
	__atomic_add_fetch(&in_flight, 1, __ATOMIC_SEQ_CST);
	if (!is_shutting_down())
		ret = proxy_request(ctx, arg, path);
	__atomic_sub_fetch(&in_flight, 1, __ATOMIC_SEQ_CST);

	/*
	 * Once shutdown has begun, don't return into gfserver: gfserver_stop()
	 * frees the worker contexts without waiting for the threads, so touching
	 * them afterwards is a use-after-free. Idle here (still non-cancellable)
	 * until the process exits.
	 */
	while (is_shutting_down())
		pause();

	pthread_setcancelstate(old_cancel_state, NULL);
	return ret;
}

void proxy_shutdown(void){
	struct timespec delay = { 0, 10 * 1000 * 1000 };	/* 10ms */
	int waited_ms;

	/* A second signal handled on another thread while the first is still
	 * waiting: let the first one finish the shutdown. */
	if (__atomic_exchange_n(&shutting_down, 1, __ATOMIC_SEQ_CST))
		for (;;)
			pause();

	for (waited_ms = 0; waited_ms < SHUTDOWN_WAIT_MS; waited_ms += 10) {
		if (__atomic_load_n(&in_flight, __ATOMIC_SEQ_CST) == 0)
			return;
		nanosleep(&delay, NULL);
	}
}

/*
 * We provide a dummy version of handle_with_file that invokes handle_with_curl as a convenience for linking!
 */
ssize_t handle_with_file(gfcontext_t *ctx, const char *path, void* arg){
	return handle_with_curl(ctx, path, arg);
}
