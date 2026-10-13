// In case you want to implement the shared memory IPC as a library
// You may use this file. It is optional. It does help with code reuse
//
#ifndef __SHM_CHANNEL_H__
#define __SHM_CHANNEL_H__

#include <semaphore.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>
#include <sys/types.h>

/*
 * Protocol between webproxy and simplecached.
 *
 * Command channel (owned by the cache): a System V message queue with a
 * hard-coded key. For each request the proxy sends a cache_request_t naming
 * the file and the shared memory segment the response should be written to.
 *
 * Data channel (owned by the proxy): a pool of POSIX shared memory segments,
 * each used by one request at a time. A segment is an shm_segment_t header
 * followed by a data buffer, and the two process-shared semaphores in the
 * header pass it back and forth:
 *
 *   proxy: shm_segment_reset(), send the request
 *   cache: write status, file_len and the first chunk; post data_ready
 *   proxy: wait data_ready, send the chunk to the client; post space_ready
 *   cache: wait space_ready, write the next chunk; post data_ready
 *   ... until file_len bytes have been sent.
 *
 * The cache's last post (the final chunk, or status NOT_FOUND or ERROR) ends
 * its use of the segment: it never touches the segment after that.
 */

/* Key of the cache's command queue, hard-coded so the proxy can find it. */
#define CACHE_CMD_KEY ((key_t) 0x6200c0de)
#define CACHE_CMD_MTYPE 1L

#define SHM_NAME_MAX 64
#define CACHE_PATH_MAX 256

/* Timed waits wake up this often to check whether to give up. */
#define SHM_POLL_MS 100
/* How long either side waits for the other in the middle of a transfer. */
#define SHM_PEER_TIMEOUT_MS 10000

/* Command channel message, proxy -> cache. */
typedef struct {
	long mtype;			/* CACHE_CMD_MTYPE */
	uint64_t req_id;		/* must match the segment's req_id */
	char shm_name[SHM_NAME_MAX];	/* segment to write the response to */
	char path[CACHE_PATH_MAX];	/* requested file (the cache key) */
} cache_request_t;

/* Size of a cache_request_t as msgsnd/msgrcv count it (without mtype). */
#define CACHE_REQUEST_SIZE (sizeof(cache_request_t) - sizeof(long))

typedef enum {
	SHM_STATUS_OK,		/* file found: file_len is set, data holds a chunk */
	SHM_STATUS_NOT_FOUND,	/* not in the cache */
	SHM_STATUS_ERROR	/* the cache failed or gave up: the transfer is over */
} shm_status_t;

/* Header at the start of every shared memory segment. */
typedef struct {
	sem_t data_ready;	/* cache -> proxy: a response or chunk is ready */
	sem_t space_ready;	/* proxy -> cache: chunk consumed, write the next */
	uint64_t req_id;	/* set by the proxy for each request */
	int status;		/* shm_status_t, set by the cache */
	size_t file_len;	/* set by the cache with SHM_STATUS_OK */
	size_t chunk_len;	/* bytes of data[] in the current chunk */
	char data[];		/* the rest of the segment */
} shm_segment_t;

#define SHM_HEADER_SIZE (offsetof(shm_segment_t, data))

/* The cache's view of a proxy's segment, mapped for one request. */
typedef struct {
	shm_segment_t *seg;
	size_t size;	/* actual size of the segment, from fstat */
	int fd;		/* kept open to notice the proxy removing the segment */
} shm_mapping_t;

/* Sets deadline to timeout_ms from now, as sem_timedwait and
 * pthread_cond_timedwait expect (absolute, CLOCK_REALTIME). */
void shm_deadline(struct timespec *deadline, unsigned int timeout_ms);

/* Waits up to timeout_ms for sem. Returns 0, or -1 with errno set
 * (ETIMEDOUT if the time ran out). */
int shm_wait(sem_t *sem, unsigned int timeout_ms);

/* Proxy side: the proxy creates and removes the segments. */
shm_segment_t *shm_segment_create(const char *name, size_t size);
void shm_segment_destroy(const char *name, shm_segment_t *seg, size_t size);
void shm_segment_reset(shm_segment_t *seg, uint64_t req_id);

/* Cache side: maps a proxy's segment for the duration of one request. */
int shm_segment_attach(const char *name, shm_mapping_t *m);
void shm_segment_detach(shm_mapping_t *m);
int shm_segment_removed(const shm_mapping_t *m);

/* Command queue: the cache creates and removes it, the proxy opens it. */
int cmd_queue_create(void);
int cmd_queue_open(void);
int cmd_queue_exists(int qid);
void cmd_queue_remove(int qid);

#endif // __SHM_CHANNEL_H__
