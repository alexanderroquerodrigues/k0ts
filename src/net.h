#ifndef NET_H
#define NET_H

#include <stddef.h>   // size_t
#include <stdint.h>   // uint32_t
#include "state.h"    // MAX_NAME

#define MAX_FDS 65536      // ceiling on fd numbers we track
#define MAX_LINE_BUF 8192  // max buffered bytes per connection before we give up

/* kind of thing a connection is */
typedef enum
{
    CONN_CLIENT_LISTEN, // client-facing listening socket
    CONN_PEER_LISTEN,   // peer-facing listening socket
    CONN_CLIENT,        // accepted client connection
    CONN_PEER           // accepted or outgoing peer connection
} conn_type_t;

/* one tracked socket plus its I/O buffers */
typedef struct conn
{
    int fd;              // underlying socket fd
    conn_type_t type;    // what kind of connection this is

    char *inbuf;         // pending unparsed input bytes
    size_t inlen, incap; // used / allocated size of inbuf

    char *outbuf;              // pending unsent output bytes
    size_t outlen, outcap, outoff; // used size, allocated size, and send offset of outbuf

    int registered;      // client has sent REGISTER
    char name[MAX_NAME]; // client's registered name

    int pending_connect; // outgoing peer socket, connect() in flight
    int peer_target_idx; // index into peer target table, or -1 if not one
} conn_t;

extern conn_t *g_conns[MAX_FDS]; // fd -> conn_t lookup table
extern int g_epfd;               // the shared epoll instance

void die(const char *msg);                     // log errno and exit(1)
void set_nonblocking(int fd);                  // O_NONBLOCK a socket fd
void epoll_add(int fd, uint32_t events, void *ptr); // register fd with epoll
void epoll_mod(int fd, uint32_t events, void *ptr); // change fd's epoll events

conn_t *conn_new(int fd, conn_type_t type); // allocate and register a conn_t
void conn_free(conn_t *c);                  // close fd, defer struct free to conn_reap
void conn_reap(void);                       // free conn_t's queued by conn_free

void conn_enqueue(conn_t *c, const char *data, size_t n); // queue bytes for output
void conn_enqueue_str(conn_t *c, const char *s);          // queue a NUL-terminated string
void conn_flush(conn_t *c);                               // send as much of outbuf as possible
void conn_feed(conn_t *c, const char *data, size_t n);    // buffer input and dispatch full lines

int make_listener(int port);                        // create a bound, listening, nonblocking socket
void handle_accept(int listen_fd, conn_type_t type); // accept() every pending connection

void net_run_loop(void); // the main epoll_wait event loop, runs forever

#endif
