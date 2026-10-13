// In case you want to implement the shared memory IPC as a library
// This is optional but may help with code reuse
//
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/msg.h>
#include <sys/stat.h>

#include "shm_channel.h"

void shm_deadline(struct timespec *deadline, unsigned int timeout_ms){
	clock_gettime(CLOCK_REALTIME, deadline);
	deadline->tv_sec += timeout_ms / 1000;
	deadline->tv_nsec += (long) (timeout_ms % 1000) * 1000000L;
	if (deadline->tv_nsec >= 1000000000L) {
		deadline->tv_sec++;
		deadline->tv_nsec -= 1000000000L;
	}
}

int shm_wait(sem_t *sem, unsigned int timeout_ms){
	struct timespec deadline;

	shm_deadline(&deadline, timeout_ms);
	while (sem_timedwait(sem, &deadline) < 0) {
		if (errno != EINTR)
			return -1;
	}
	return 0;
}

/*
 * Creates a segment of size bytes (header included) and maps it.
 * Returns NULL with errno set on failure.
 */
shm_segment_t *shm_segment_create(const char *name, size_t size){
	shm_segment_t *seg;
	int fd, saved_errno;

	fd = shm_open(name, O_RDWR | O_CREAT | O_EXCL, 0600);
	if (fd < 0 && errno == EEXIST) {
		/* Left behind by a killed proxy that had the same pid */
		shm_unlink(name);
		fd = shm_open(name, O_RDWR | O_CREAT | O_EXCL, 0600);
	}
	if (fd < 0)
		return NULL;

	if (ftruncate(fd, size) < 0 ||
	    MAP_FAILED == (seg = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0))) {
		saved_errno = errno;
		close(fd);
		shm_unlink(name);
		errno = saved_errno;
		return NULL;
	}
	close(fd);

	/* The rest of the header is zero-filled; no request has req_id 0 */
	sem_init(&seg->data_ready, 1, 0);
	sem_init(&seg->space_ready, 1, 0);
	return seg;
}

void shm_segment_destroy(const char *name, shm_segment_t *seg, size_t size){
	if (seg)
		munmap(seg, size);
	shm_unlink(name);
}

/*
 * Prepares an idle segment (no cache worker using it) for a new request.
 */
void shm_segment_reset(shm_segment_t *seg, uint64_t req_id){
	/* Discard posts left over from a transfer that ended early */
	while (sem_trywait(&seg->data_ready) == 0)
		;
	while (sem_trywait(&seg->space_ready) == 0)
		;
	seg->req_id = req_id;
	seg->status = SHM_STATUS_ERROR;
	seg->file_len = 0;
	seg->chunk_len = 0;
}

/*
 * Opens and maps a proxy's segment. Returns 0, or -1 if it doesn't exist
 * (any more) or is too small to carry data.
 */
int shm_segment_attach(const char *name, shm_mapping_t *m){
	struct stat st;
	void *addr;

	if (0 > (m->fd = shm_open(name, O_RDWR, 0)))
		return -1;

	/* Map what the segment actually holds rather than any size the proxy
	 * claims, so the cache never writes past its end. */
	if (fstat(m->fd, &st) < 0 || st.st_size <= (off_t) SHM_HEADER_SIZE ||
	    MAP_FAILED == (addr = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, m->fd, 0))) {
		close(m->fd);
		return -1;
	}
	m->seg = addr;
	m->size = (size_t) st.st_size;
	return 0;
}

void shm_segment_detach(shm_mapping_t *m){
	munmap(m->seg, m->size);
	close(m->fd);
}

/*
 * Whether the proxy has unlinked the segment: it exited, or gave up on the
 * transfer and replaced the segment with a new one.
 */
int shm_segment_removed(const shm_mapping_t *m){
	struct stat st;

	return fstat(m->fd, &st) < 0 || st.st_nlink == 0;
}

int cmd_queue_create(void){
	int qid;

	/* Start from an empty queue: one left behind by a cache that was killed
	 * without cleaning up could hold requests nobody is waiting for.
	 * Removing it also tells proxies still using it that that cache is gone. */
	if (0 <= (qid = msgget(CACHE_CMD_KEY, 0)))
		msgctl(qid, IPC_RMID, NULL);
	return msgget(CACHE_CMD_KEY, IPC_CREAT | IPC_EXCL | 0600);
}

/*
 * Returns the queue id, or -1 (errno ENOENT) if the cache isn't running.
 */
int cmd_queue_open(void){
	return msgget(CACHE_CMD_KEY, 0);
}

/*
 * Whether the queue qid still exists. Once it's removed this stays false
 * even if a restarted cache has created a new one: that gets a new id.
 */
int cmd_queue_exists(int qid){
	struct msqid_ds ds;

	return msgctl(qid, IPC_STAT, &ds) == 0;
}

void cmd_queue_remove(int qid){
	if (qid >= 0)
		msgctl(qid, IPC_RMID, NULL);
}
