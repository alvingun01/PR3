#include "gfserver.h"
#include "cache-student.h"
#include "shm_channel.h"
#include <sys/mman.h>
#include <sys/msg.h>
#include <time.h>

/* How long cache_proxy_shutdown() waits for in-progress requests to stop. */
#define SHUTDOWN_WAIT_MS 2000

typedef struct {
	char name[SHM_NAME_MAX];
	shm_segment_t *seg;	/* NULL if it couldn't be replaced (segment_recycle) */
} proxy_segment_t;

/*
 * The pool, created by cache_proxy_init(). A request takes a segment off
 * the free queue for its whole duration, so at most nsegments requests are
 * with the cache at a time and the rest wait here.
 */
static proxy_segment_t *segments;
static unsigned int nsegments;
static size_t segsize;
static steque_t free_segments;
static pthread_mutex_t pool_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t segment_freed = PTHREAD_COND_INITIALIZER;

static uint64_t last_req_id;	/* atomic */

/*
 * Shared between the workers and the signal handler, so only accessed with
 * __atomic builtins (lock-free, hence also safe to use in the handler).
 */
static int shutting_down;	/* set once by cache_proxy_shutdown() */
static int in_flight;		/* workers currently inside fetch_from_cache() */
static int recycling;		/* workers currently inside segment_recycle() */

typedef enum {
	WAIT_OK,		/* the cache posted data_ready */
	WAIT_CACHE_GONE,	/* the command queue was removed: the cache exited or restarted */
	WAIT_FAILED,		/* the cache stopped responding mid-transfer, or the wait failed */
	WAIT_SHUTDOWN		/* the proxy is shutting down */
} wait_result_t;

static int is_shutting_down(void){
	return __atomic_load_n(&shutting_down, __ATOMIC_SEQ_CST);
}

static void sleep_ms(unsigned int ms){
	struct timespec delay = { ms / 1000, (long) (ms % 1000) * 1000000L };

	nanosleep(&delay, NULL);
}

/*
 * Takes a free segment, waiting for one if all are in use.
 * Returns NULL on shutdown.
 */
static proxy_segment_t *segment_acquire(void){
	proxy_segment_t *s = NULL;
	struct timespec deadline;

	pthread_mutex_lock(&pool_lock);
	while (!is_shutting_down()) {
		if (!steque_isempty(&free_segments)) {
			s = steque_pop(&free_segments);
			break;
		}
		/* Timed, since the signal handler can't signal the condition */
		shm_deadline(&deadline, SHM_POLL_MS);
		pthread_cond_timedwait(&segment_freed, &pool_lock, &deadline);
	}
	pthread_mutex_unlock(&pool_lock);
	return s;
}

static void segment_release(proxy_segment_t *s){
	pthread_mutex_lock(&pool_lock);
	steque_enqueue(&free_segments, s);
	pthread_cond_signal(&segment_freed);
	pthread_mutex_unlock(&pool_lock);
}

/*
 * Replaces the segment's shared memory with a new object of the same name.
 * Used when a transfer ends before the cache's last post, so the cache may
 * still write to the segment: it keeps its mapping of the old object and so
 * can't touch the next request's. Returns -1 if the segment couldn't be
 * replaced (or on shutdown), in which case it must not be used again.
 */
static int segment_recycle(proxy_segment_t *s){
	int ret = -1;

	/*
	 * cache_proxy_shutdown() waits for `recycling` to drop to 0 before it
	 * unlinks the segments, or it could unlink the name between our unlink
	 * and create and leak the new object. (It never runs on this thread and
	 * waits on itself: workers keep SIGINT/SIGTERM blocked.)
	 */
	__atomic_add_fetch(&recycling, 1, __ATOMIC_SEQ_CST);

	if (!is_shutting_down()) {
		shm_segment_destroy(s->name, s->seg, segsize);
		if (NULL != (s->seg = shm_segment_create(s->name, segsize)))
			ret = 0;
		else
			fprintf(stderr, "Unable to replace shared memory segment %s: %s\n",
				s->name, strerror(errno));
	}

	__atomic_sub_fetch(&recycling, 1, __ATOMIC_SEQ_CST);
	return ret;
}

/*
 * Ends a request whose transfer stopped before the cache's last post.
 */
static void segment_abandon(proxy_segment_t *s){
	if (segment_recycle(s) == 0)
		segment_release(s);
}

/*
 * Returns the cache's command queue. If the cache isn't running (yet, or
 * because it's restarting), keeps trying until it is. Returns -1 only on
 * shutdown.
 */
static int cache_connect(void){
	int qid;

	while (!is_shutting_down()) {
		if (0 <= (qid = cmd_queue_open()))
			return qid;
		sleep_ms(SHM_POLL_MS);
	}
	return -1;
}

/*
 * Waits for the cache to post data_ready: for up to timeout_ms, or as long
 * as the cache keeps running if timeout_ms is 0 (a new request may be
 * queued behind many others at the cache).
 */
static wait_result_t wait_for_cache(shm_segment_t *seg, int qid, unsigned int timeout_ms){
	unsigned int waited_ms = 0;

	for (;;) {
		if (shm_wait(&seg->data_ready, SHM_POLL_MS) == 0)
			return WAIT_OK;
		if (errno != ETIMEDOUT)
			return WAIT_FAILED;
		if (is_shutting_down())
			return WAIT_SHUTDOWN;
		if (!cmd_queue_exists(qid))
			return WAIT_CACHE_GONE;
		waited_ms += SHM_POLL_MS;
		if (timeout_ms > 0 && waited_ms >= timeout_ms)
			return WAIT_FAILED;
	}
}

/*
 * Asks the cache for path and relays its response to the Getfile client.
 */
static ssize_t fetch_from_cache(gfcontext_t *ctx, const char *path){
	size_t capacity = segsize - SHM_HEADER_SIZE;
	size_t file_len, chunk_len, received = 0;
	cache_request_t req;
	proxy_segment_t *s;
	shm_segment_t *seg;
	wait_result_t res;
	int qid, client_ok;

	if (strlen(path) >= sizeof(req.path))
		return SERVER_FAILURE;
	if (NULL == (s = segment_acquire()))
		return SERVER_FAILURE;

	/* Send the request, and send it again if the cache goes away before
	 * answering (if it's restarted, say). */
	for (;;) {
		if (0 > (qid = cache_connect()))
			return SERVER_FAILURE;

		seg = s->seg;
		memset(&req, 0, sizeof(req));
		req.mtype = CACHE_CMD_MTYPE;
		req.req_id = __atomic_add_fetch(&last_req_id, 1, __ATOMIC_SEQ_CST);
		strcpy(req.shm_name, s->name);
		strcpy(req.path, path);
		shm_segment_reset(seg, req.req_id);

		if (msgsnd(qid, &req, CACHE_REQUEST_SIZE, 0) < 0) {
			/* Not sent. Try again if the cache went away (EIDRM/EINVAL)
			 * or a signal interrupted us. */
			if (errno == EINTR || !cmd_queue_exists(qid))
				continue;
			perror("msgsnd");
			segment_release(s);
			return SERVER_FAILURE;
		}

		res = wait_for_cache(seg, qid, 0);
		if (res == WAIT_OK)
			break;
		if (res == WAIT_SHUTDOWN)
			return SERVER_FAILURE;
		/* The cache may have been part-way through writing the response */
		if (segment_recycle(s) < 0)
			return SERVER_FAILURE;
		if (res == WAIT_FAILED) {
			segment_release(s);
			return SERVER_FAILURE;
		}
	}

	switch (seg->status) {
	case SHM_STATUS_OK:
		break;
	case SHM_STATUS_NOT_FOUND:
		segment_release(s);
		return gfs_sendheader(ctx, GF_FILE_NOT_FOUND, 0);
	default:
		/* gfserver sends the client GF_ERROR */
		segment_release(s);
		return SERVER_FAILURE;
	}

	file_len = seg->file_len;
	client_ok = gfs_sendheader(ctx, GF_OK, file_len) >= 0;

	/*
	 * From here on, never return a negative value. gfserver answers one by
	 * writing a GF_ERROR header into the middle of the body and then waiting
	 * for the client to hang up, while the client waits for the rest of the
	 * body, so both hang. Given a short transfer and a non-negative value, it
	 * zero-fills the rest of the body instead and the client can finish.
	 *
	 * Relay the chunks. If the client goes away, keep taking them anyway so
	 * the cache worker finishes normally instead of waiting on us.
	 */
	for (;;) {
		chunk_len = seg->chunk_len;
		if (seg->status != SHM_STATUS_OK) {
			/* The cache gave up on the transfer; that was its last post */
			segment_release(s);
			break;
		}
		if (chunk_len > capacity || chunk_len > file_len - received ||
		    (chunk_len == 0 && received < file_len)) {
			fprintf(stderr, "Invalid chunk from the cache for %s\n", path);
			segment_abandon(s);
			break;
		}

		if (client_ok && chunk_len > 0 &&
		    gfs_send(ctx, seg->data, chunk_len) != (ssize_t) chunk_len)
			client_ok = 0;
		received += chunk_len;
		if (received == file_len) {
			segment_release(s);
			break;
		}

		sem_post(&seg->space_ready);
		res = wait_for_cache(seg, qid, SHM_PEER_TIMEOUT_MS);
		if (res == WAIT_SHUTDOWN)
			break;
		if (res != WAIT_OK) {
			fprintf(stderr, "Lost the cache while sending %s\n", path);
			segment_abandon(s);
			break;
		}
	}

	return (ssize_t) received;
}

ssize_t handle_with_cache(gfcontext_t *ctx, const char *path, void* arg){
	int old_cancel_state;
	sigset_t sigs;
	ssize_t ret = SERVER_FAILURE;

	/*
	 * As in Part 1: make this worker non-cancellable for the rest of its
	 * life, so gfserver_stop() can't cancel it part-way through a request
	 * (holding pool_lock, say). Workers are told to stop through
	 * shutting_down instead.
	 *
	 * Likewise block SIGINT/SIGTERM here for good, leaving them to the main
	 * thread (or a worker still idle), so cache_proxy_shutdown() never runs
	 * on a thread part-way through a request and then waits for it.
	 */
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &old_cancel_state);
	sigemptyset(&sigs);
	sigaddset(&sigs, SIGINT);
	sigaddset(&sigs, SIGTERM);
	pthread_sigmask(SIG_BLOCK, &sigs, NULL);

	/* Count ourselves in before checking the flag, so cache_proxy_shutdown()
	 * either sees this request or this request sees the flag. */
	__atomic_add_fetch(&in_flight, 1, __ATOMIC_SEQ_CST);
	if (!is_shutting_down())
		ret = fetch_from_cache(ctx, path);
	__atomic_sub_fetch(&in_flight, 1, __ATOMIC_SEQ_CST);

	/*
	 * Once shutdown has begun, don't return into gfserver: gfserver_stop()
	 * frees the worker contexts without waiting for the threads, so touching
	 * them afterwards is a use-after-free. Idle here until the process exits.
	 */
	while (is_shutting_down())
		pause();

	return ret;
}

int cache_proxy_init(unsigned int count, size_t size){
	sigset_t sigs, old_sigs;
	unsigned int i;
	int ret = 0;

	/* Hold off SIGINT/SIGTERM until the pool is complete, so that
	 * cache_proxy_shutdown() never sees it half-built. */
	sigemptyset(&sigs);
	sigaddset(&sigs, SIGINT);
	sigaddset(&sigs, SIGTERM);
	pthread_sigmask(SIG_BLOCK, &sigs, &old_sigs);

	steque_init(&free_segments);
	segsize = size;
	if (NULL == (segments = calloc(count, sizeof(*segments)))) {
		ret = -1;
		goto out;
	}

	for (i = 0; i < count; i++) {
		snprintf(segments[i].name, SHM_NAME_MAX, "/gios-pr3-%d-%u", (int) getpid(), i);
		if (NULL == (segments[i].seg = shm_segment_create(segments[i].name, size))) {
			fprintf(stderr, "Unable to create shared memory segment %s: %s\n",
				segments[i].name, strerror(errno));
			break;
		}
		steque_enqueue(&free_segments, &segments[i]);
	}

	if (i < count) {
		while (i-- > 0)
			shm_segment_destroy(segments[i].name, segments[i].seg, size);
		steque_destroy(&free_segments);
		free(segments);
		segments = NULL;
		ret = -1;
		goto out;
	}
	nsegments = count;

out:
	pthread_sigmask(SIG_SETMASK, &old_sigs, NULL);
	return ret;
}

void cache_proxy_shutdown(void){
	unsigned int i;
	int waited_ms;

	/* A second signal handled on another thread while the first is still
	 * in progress: let the first one finish the shutdown. */
	if (__atomic_exchange_n(&shutting_down, 1, __ATOMIC_SEQ_CST))
		for (;;)
			pause();

	/* Replacing a segment takes a few syscalls and never blocks */
	while (__atomic_load_n(&recycling, __ATOMIC_SEQ_CST) > 0)
		sleep_ms(1);

	for (i = 0; i < nsegments; i++)
		shm_unlink(segments[i].name);

	/* The segments stay mapped until exit, so requests still running can't
	 * fault; give them a moment to notice the flag and stop. */
	for (waited_ms = 0; waited_ms < SHUTDOWN_WAIT_MS; waited_ms += 10) {
		if (__atomic_load_n(&in_flight, __ATOMIC_SEQ_CST) == 0)
			return;
		sleep_ms(10);
	}
}
