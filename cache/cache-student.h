/*
 You can use this however you want.
 */
 #ifndef __CACHE_STUDENT_H__844

 #define __CACHE_STUDENT_H__844

 #include <stddef.h>
 #include "steque.h"

/*
 * Proxy side of the cache IPC, implemented in handle_with_cache.c.
 *
 * Creates the pool of nsegments shared memory segments of segsize bytes
 * each (the data channel). Call before gfserver_serve(). Returns 0, or -1
 * after removing any segments it had created.
 */
int cache_proxy_init(unsigned int nsegments, size_t segsize);

/*
 * Called from the SIGINT/SIGTERM handler before gfserver_stop(). Removes
 * every shared memory segment, then waits (bounded) for in-progress
 * requests to stop using their gfserver contexts. Workers don't return
 * into gfserver after this. Uses no locks, so it's safe in the handler.
 */
void cache_proxy_shutdown(void);

 #endif // __CACHE_STUDENT_H__844
