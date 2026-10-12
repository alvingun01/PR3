#include "gfserver.h"
#include "proxy-student.h"

#define USAGE                                                                         \
"usage:\n"                                                                            \
"  webproxy [options]\n"                                                              \
"options:\n"                                                                          \
"  -s [server]         The server to connect to (Default: GitHub test data)\n"        \
"  -h                  Show this help message\n"                                      \
"  -p [listen_port]    Listen port (Default: 16662)\n"                                \
"  -t [thread_count]   Num worker threads (Default is 8, Range is 1-80)\n"          


/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
  {"help",          no_argument,            NULL,           'h'},
  {"thread-count",  required_argument,      NULL,           't'},
  {"port",          required_argument,      NULL,           'p'},
  {"server",        required_argument,      NULL,           's'},
  {NULL,            0,                      NULL,            0}
};


#define MAX_REQUEST_LENGTH_N 822

static gfserver_t gfs;
static proxy_worker_t *workers;

static void _sig_handler(int signo){
  if (signo == SIGTERM || signo == SIGINT){
    proxy_shutdown();
    gfserver_stop(&gfs);
    // No curl_global_cleanup() here: libcurl forbids it while other threads may
    // still be using curl, and gfserver_stop() doesn't wait for the workers.
    exit(signo);
  }
}

extern ssize_t handle_with_file(gfcontext_t *ctx, const char *path, void* arg);
extern ssize_t handle_with_curl(gfcontext_t *ctx, const char *path, void* arg);

int main(int argc, char **argv) {
  int i;
  int option_char = 0;
  unsigned short port = 16662;
  unsigned short nworkerthreads = 8;
  const char *server = "https://raw.githubusercontent.com/gt-cs6200/image_data";

  // disable buffering on stdout
  setbuf(stdout, NULL);

  if (signal(SIGINT, _sig_handler) == SIG_ERR){
    fprintf(stderr,"Can't catch SIGINT...exiting.\n");
    exit(SERVER_FAILURE);
  }

  if (signal(SIGTERM, _sig_handler) == SIG_ERR){
    fprintf(stderr,"Can't catch SIGTERM...exiting.\n");
    exit(SERVER_FAILURE);
  }

  // A client hanging up mid-transfer makes gfserver's write() raise SIGPIPE,
  // which would kill the whole proxy; ignore it so the write fails with EPIPE
  if (signal(SIGPIPE, SIG_IGN) == SIG_ERR){
    fprintf(stderr,"Can't ignore SIGPIPE...exiting.\n");
    exit(SERVER_FAILURE);
  }

  // Parse and set command line arguments
  while ((option_char = getopt_long(argc, argv, "p:qs:xt:h", gLongOptions, NULL)) != -1) {
    switch (option_char) {
      case 'a':
      case 'd':
      case 'u':
      	break;
      case 'p': // listen-port
        port = atoi(optarg);
        break;
      case 's': // file-path
        server = optarg;
        break;              
      case 't': // thread-count 6
        nworkerthreads = atoi(optarg);
        break;
      default:
        fprintf(stderr, "%s", USAGE);
        exit(1);
      case 'h': // help
        fprintf(stdout, "%s", USAGE);
        exit(0);
        break;        
   }
  }

  if (!server) {
    fprintf(stderr, "Invalid (null) server name\n");
    exit(__LINE__);
  }

  if (port > 61286) {
    fprintf(stderr, "Invalid port number\n");
    exit(__LINE__);
  }

  if ((nworkerthreads < 1) || (nworkerthreads > 80)) {
    fprintf(stderr, "Invalid number of worker threads\n");
    exit(__LINE__);
  }

  // libcurl's global init is not thread-safe, so do it before any workers start
  if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
    fprintf(stderr, "Failed to initialize libcurl\n");
    exit(SERVER_FAILURE);
  }

  // Initialize server structure here
  gfserver_init(&gfs, nworkerthreads);
// Set server options here
  gfserver_setopt(&gfs, GFS_MAXNPENDING, 90);
  gfserver_setopt(&gfs, GFS_WORKER_FUNC, handle_with_curl);
  gfserver_setopt(&gfs, GFS_PORT, port);
  // Set up arguments for worker here
  // Each thread gets its own curl handle (easy handles can't be shared
  // between threads) that it reuses across requests
  if (NULL == (workers = calloc(nworkerthreads, sizeof(*workers)))) {
    fprintf(stderr, "Failed to allocate worker state\n");
    exit(SERVER_FAILURE);
  }
  for(i = 0; i < nworkerthreads; i++) {
    workers[i].server = server;
    if (NULL == (workers[i].curl = curl_easy_init())) {
      fprintf(stderr, "Failed to create curl handle\n");
      exit(SERVER_FAILURE);
    }
    gfserver_setopt(&gfs, GFS_WORKER_ARG, i, &workers[i]);
  }
  // Invoke the framework - this is an infinite loop and shouldn't return
  gfserver_serve(&gfs);
  // not reached
  return -2213;

}
