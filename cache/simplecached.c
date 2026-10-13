#include <stdio.h>
#include <unistd.h>
#include <printf.h>
#include <string.h>
#include <signal.h>
#include <limits.h>
#include <sys/signal.h>
#include <stdlib.h>
#include <curl/curl.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <pthread.h>
#include <sys/msg.h>
#include <sys/stat.h>
#include "cache-student.h"
#include "shm_channel.h"
#include "simplecache.h"
#include "gfserver.h"

// CACHE_FAILURE
#if !defined(CACHE_FAILURE)
    #define CACHE_FAILURE (-1)
#endif

#define MAX_CACHE_REQUEST_LEN 6112
#define MAX_SIMPLE_CACHE_QUEUE_SIZE 783

unsigned long int cache_delay;

/*
 * The command queue. Only the boss thread sets it, with SIGINT/SIGTERM
 * blocked, and only the boss handles those signals (the workers keep them
 * blocked), so the handler always sees a complete value.
 */
static int cmd_qid = -1;

/* Requests received by the boss, waiting for a worker */
static steque_t requests;
static pthread_mutex_t requests_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t request_added = PTHREAD_COND_INITIALIZER;
static pthread_cond_t request_taken = PTHREAD_COND_INITIALIZER;

static void _sig_handler(int signo){
	if (signo == SIGTERM || signo == SIGINT){
		// This is where your IPC clean up should occur
		cmd_queue_remove(cmd_qid);
		exit(signo);
	}
}

/*
 * (Re)creates the command queue, with SIGINT/SIGTERM blocked so the handler
 * can't miss a queue that was just created.
 */
static void open_command_queue(void){
	sigset_t sigs, old_sigs;

	sigemptyset(&sigs);
	sigaddset(&sigs, SIGINT);
	sigaddset(&sigs, SIGTERM);
	pthread_sigmask(SIG_BLOCK, &sigs, &old_sigs);
	cmd_qid = cmd_queue_create();
	pthread_sigmask(SIG_SETMASK, &old_sigs, NULL);

	if (cmd_qid < 0) {
		perror("Unable to create the command queue");
		exit(CACHE_FAILURE);
	}
}

/*
 * Reads exactly len bytes at offset. Uses pread because workers serving the
 * same file share its descriptor, and so would share a file offset.
 */
static int read_chunk(int fd, char *buf, size_t len, off_t offset){
	ssize_t n;

	while (len > 0) {
		n = pread(fd, buf, len, offset);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return -1;	/* error, or the file shrank */
		buf += n;
		len -= n;
		offset += n;
	}
	return 0;
}

/*
 * Waits for the proxy to hand the segment back. Gives up after
 * SHM_PEER_TIMEOUT_MS, or as soon as the proxy removes the segment (it
 * exited or abandoned the transfer), so a dead or stuck proxy can't hold
 * on to a worker for long.
 */
static int wait_for_proxy(const shm_mapping_t *m){
	unsigned int waited_ms;

	for (waited_ms = 0; waited_ms < SHM_PEER_TIMEOUT_MS; waited_ms += SHM_POLL_MS) {
		if (shm_wait(&m->seg->space_ready, SHM_POLL_MS) == 0)
			return 0;
		if (errno != ETIMEDOUT || shm_segment_removed(m))
			return -1;
	}
	return -1;
}

/*
 * Ends the cache's part in a transfer with a final status for the proxy.
 */
static void finish(shm_segment_t *seg, shm_status_t status){
	seg->status = status;
	seg->chunk_len = 0;
	sem_post(&seg->data_ready);
}

/*
 * Looks up the requested file and writes it to the proxy's segment, one
 * segment-full at a time.
 */
static void serve_request(cache_request_t *req){
	size_t capacity, file_len, len, offset = 0;
	shm_mapping_t m;
	shm_segment_t *seg;
	struct stat st;
	int fd;

	/* Don't count on the proxy to have terminated the strings */
	req->shm_name[SHM_NAME_MAX - 1] = '\0';
	req->path[CACHE_PATH_MAX - 1] = '\0';

	/* Fails if the proxy has removed the segment since; nobody's waiting */
	if (shm_segment_attach(req->shm_name, &m) < 0)
		return;
	seg = m.seg;
	capacity = m.size - SHM_HEADER_SIZE;

	/* A request left over from before the proxy replaced the segment: the
	 * segment now belongs to a different request. */
	if (seg->req_id != req->req_id)
		goto out;

	if (0 > (fd = simplecache_get(req->path))) {
		finish(seg, SHM_STATUS_NOT_FOUND);
		goto out;
	}
	if (fstat(fd, &st) < 0) {
		finish(seg, SHM_STATUS_ERROR);
		goto out;
	}
	file_len = (size_t) st.st_size;

	seg->status = SHM_STATUS_OK;
	seg->file_len = file_len;
	for (;;) {
		len = file_len - offset < capacity ? file_len - offset : capacity;
		if (read_chunk(fd, seg->data, len, (off_t) offset) < 0) {
			finish(seg, SHM_STATUS_ERROR);
			break;
		}
		seg->chunk_len = len;
		offset += len;
		sem_post(&seg->data_ready);
		if (offset == file_len)
			break;

		if (wait_for_proxy(&m) < 0) {
			/* In case the proxy is only slow, tell it the transfer is over */
			finish(seg, SHM_STATUS_ERROR);
			break;
		}
	}

out:
	shm_segment_detach(&m);
}

static void *worker_main(void *arg){
	cache_request_t *req;

	for (;;) {
		pthread_mutex_lock(&requests_lock);
		while (steque_isempty(&requests))
			pthread_cond_wait(&request_added, &requests_lock);
		req = steque_pop(&requests);
		pthread_cond_signal(&request_taken);
		pthread_mutex_unlock(&requests_lock);

		serve_request(req);
		free(req);
	}
	return NULL;
}

#define USAGE                                                                 \
"usage:\n"                                                                    \
"  simplecached [options]\n"                                                  \
"options:\n"                                                                  \
"  -c [cachedir]       Path to static files (Default: ./)\n"                  \
"  -t [thread_count]   Thread count for work queue (Default is 8, Range is 1-100)\n"      \
"  -d [delay]          Delay in simplecache_get (Default is 0, Range is 0-2500000 (microseconds)\n "	\
"  -h                  Show this help message\n"

//OPTIONS
static struct option gLongOptions[] = {
  {"cachedir",           required_argument,      NULL,           'c'},
  {"nthreads",           required_argument,      NULL,           't'},
  {"help",               no_argument,            NULL,           'h'},
  {"hidden",			 no_argument,			 NULL,			 'i'}, /* server side */
  {"delay", 			 required_argument,		 NULL, 			 'd'}, // delay.
  {NULL,                 0,                      NULL,             0}
};

void Usage() {
  fprintf(stdout, "%s", USAGE);
}

int main(int argc, char **argv) {
	int nthreads = 6;
	char *cachedir = "locals.txt";
	char option_char;
	struct sigaction sa;
	sigset_t sigs;
	cache_request_t *req;
	pthread_t thread;
	ssize_t len;
	int i;

	/* disable buffering to stdout */
	setbuf(stdout, NULL);

	while ((option_char = getopt_long(argc, argv, "d:ic:hlt:x", gLongOptions, NULL)) != -1) {
		switch (option_char) {
			default:
				Usage();
				exit(1);
			case 't': // thread-count
				nthreads = atoi(optarg);
				break;
			case 'h': // help
				Usage();
				exit(0);
				break;
            case 'c': //cache directory
				cachedir = optarg;
				break;
            case 'd':
				cache_delay = (unsigned long int) atoi(optarg);
				break;
			case 'i': // server side usage
			case 'o': // do not modify
			case 'a': // experimental
				break;
		}
	}

	if (cache_delay > 2500000) {
		fprintf(stderr, "Cache delay must be less than 2500000 (us)\n");
		exit(__LINE__);
	}

	if ((nthreads>100) || (nthreads < 1)) {
		fprintf(stderr, "Invalid number of threads must be in between 1-100\n");
		exit(__LINE__);
	}

	// Block both signals while the handler runs, so a SIGINT and a SIGTERM in
	// quick succession can't interrupt the cleanup
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = _sig_handler;
	sigemptyset(&sa.sa_mask);
	sigaddset(&sa.sa_mask, SIGINT);
	sigaddset(&sa.sa_mask, SIGTERM);

	if (sigaction(SIGINT, &sa, NULL) < 0){
		fprintf(stderr,"Unable to catch SIGINT...exiting.\n");
		exit(CACHE_FAILURE);
	}
	if (sigaction(SIGTERM, &sa, NULL) < 0){
		fprintf(stderr,"Unable to catch SIGTERM...exiting.\n");
		exit(CACHE_FAILURE);
	}

	// Keep SIGINT/SIGTERM blocked until the command queue exists. The workers
	// inherit the blocked mask, which leaves the boss to handle them.
	sigemptyset(&sigs);
	sigaddset(&sigs, SIGINT);
	sigaddset(&sigs, SIGTERM);
	pthread_sigmask(SIG_BLOCK, &sigs, NULL);

	/*Initialize cache*/
	simplecache_init(cachedir);

	// Cache should go here
	steque_init(&requests);
	for (i = 0; i < nthreads; i++) {
		if (0 != pthread_create(&thread, NULL, worker_main, NULL)) {
			fprintf(stderr, "Unable to create worker threads\n");
			exit(CACHE_FAILURE);
		}
	}

	// The cache owns the command channel
	open_command_queue();
	pthread_sigmask(SIG_UNBLOCK, &sigs, NULL);

	// Boss: pass each request from the proxies on to a worker
	for (;;) {
		if (NULL == (req = malloc(sizeof(*req)))) {
			fprintf(stderr, "Out of memory\n");
			cmd_queue_remove(cmd_qid);
			exit(CACHE_FAILURE);
		}

		// MSG_NOERROR truncates an oversized message instead of leaving it
		// stuck at the head of the queue; it's dropped below
		len = msgrcv(cmd_qid, req, CACHE_REQUEST_SIZE, 0, MSG_NOERROR);
		if (len < 0) {
			free(req);
			if (errno == EIDRM || errno == EINVAL) {
				// Something else removed the queue; make a new one
				open_command_queue();
			} else if (errno != EINTR) {
				perror("msgrcv");
				cmd_queue_remove(cmd_qid);
				exit(CACHE_FAILURE);
			}
			continue;
		}
		if (req->mtype != CACHE_CMD_MTYPE || (size_t) len != CACHE_REQUEST_SIZE) {
			free(req);
			continue;
		}

		pthread_mutex_lock(&requests_lock);
		while (steque_size(&requests) >= MAX_SIMPLE_CACHE_QUEUE_SIZE)
			pthread_cond_wait(&request_taken, &requests_lock);
		steque_enqueue(&requests, req);
		pthread_cond_signal(&request_added);
		pthread_mutex_unlock(&requests_lock);
	}

	// Line never reached
	return -1;
}
