/*
 Optional file to help with organization 
 */
 #ifndef __SERVER_STUDENT_H__846
 #define __SERVER_STUDENT_H__846

#include <curl/curl.h>

/*
 * Per-worker-thread state, created in webproxy.c and registered as each
 * thread's GFS_WORKER_ARG. Only its own thread touches it, so no locking.
 */
typedef struct {
	const char *server;	/* base URL that request paths are appended to */
	CURL *curl;		/* reused across requests to keep connections alive */
} proxy_worker_t;

/*
 * Called from the SIGINT/SIGTERM handler before gfserver_stop(). Makes
 * in-progress transfers abort and waits (bounded) for every worker to leave
 * libcurl, so exit() doesn't tear down libcurl/OpenSSL under a running
 * transfer. Workers don't return into gfserver after this. Uses only
 * async-signal-safe calls.
 */
void proxy_shutdown(void);

 #endif // __SERVER_STUDENT_H__846
