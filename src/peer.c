#include <stdio.h>      // snprintf
#include <stdlib.h>     // atoi
#include <string.h>     // strchr, memcpy, strerror
#include <errno.h>      // errno, EINPROGRESS
#include <time.h>       // time_t, time()
#include <unistd.h>     // close
#include <sys/socket.h> // socket, connect, getsockopt
#include <sys/epoll.h>  // EPOLLIN, EPOLLOUT
#include <netinet/in.h> // sockaddr_in, htons
#include <arpa/inet.h>  // inet_pton

#include "peer.h"
#include "log.h"

/* a configured peer node, and the live connection to it if any */
typedef struct
{
    char host[128];       // peer's hostname/IP
    int client_port;      // peer's client-facing port (informational)
    int peer_port;        // peer's replication port, what we actually dial
    conn_t *conn;         // NULL if not currently connected
    time_t next_retry;    // earliest time we should try to (re)connect
} peer_target_t;

static peer_target_t g_peers[64]; // fixed table of configured peers
static int g_npeers = 0;          // how many entries in g_peers are in use

/* number of configured peer targets */
int peer_target_count(void)
{
    return g_npeers; // simple accessor for main's startup log line
}

/* parse a "host:port" or bare "port" CLI arg and add it to the peer table */
void add_peer_target(const char *arg)
{
    peer_target_t *pt = &g_peers[g_npeers]; // next free slot
    const char *colon = strchr(arg, ':');   // does arg specify a host?
    if (colon)
    {
        size_t hlen = (size_t)(colon - arg); // length of the host part
        if (hlen >= sizeof(pt->host))
            hlen = sizeof(pt->host) - 1; // truncate defensively
        memcpy(pt->host, arg, hlen);     // copy the host part
        pt->host[hlen] = '\0';           // NUL-terminate it
        pt->client_port = atoi(colon + 1); // port comes after the colon
    }
    else
    {
        snprintf(pt->host, sizeof(pt->host), "127.0.0.1"); // no host given, assume localhost
        pt->client_port = atoi(arg);                       // whole arg is the port
    }
    pt->peer_port = pt->client_port + PEER_PORT_OFFSET; // derive the replication port
    pt->conn = NULL;      // not connected yet
    pt->next_retry = 0;   // connect right away
    g_npeers++;           // commit the new entry
}

/* begin (or retry) an outgoing, nonblocking connect() to peer idx */
void start_peer_connect(int idx)
{
    peer_target_t *pt = &g_peers[idx]; // the peer we're dialing
    int fd = socket(AF_INET, SOCK_STREAM, 0); // new TCP socket
    if (fd < 0)
    {
        LOG("socket: %s\n", strerror(errno)); // couldn't even create the socket
        return;
    }
    set_nonblocking(fd); // connect() must not block the event loop

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(pt->peer_port); // target replication port
    if (inet_pton(AF_INET, pt->host, &addr.sin_addr) != 1)
    {
        LOG("[peer] bad host %s, skipping\n", pt->host); // unparsable address
        close(fd);
        pt->next_retry = time(NULL) + RETRY_SECONDS; // try again later
        return;
    }

    LOG("[peer] connecting to %s:%d ...\n", pt->host, pt->peer_port); // trace attempt
    int rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));    // kick off the connect
    conn_t *c = conn_new(fd, CONN_PEER); // track it as a peer connection
    c->peer_target_idx = idx;            // link it back to this peer slot
    pt->conn = c;                        // record it as the live connection

    if (rc == 0)
    {
        c->pending_connect = 0;                        // connected immediately (e.g. localhost)
        epoll_add(fd, EPOLLIN, c);                      // watch for incoming data
        LOG("[peer] connected to %s:%d immediately\n", pt->host, pt->peer_port);
        conn_enqueue_str(c, "SYNC\n"); // ask them for their current state
    }
    else if (errno == EINPROGRESS)
    {
        c->pending_connect = 1;      // connect is in flight
        epoll_add(fd, EPOLLOUT, c);  // we'll be notified when it's writable (done)
    }
    else
    {
        LOG("[peer] connect to %s:%d failed: %s\n", pt->host, pt->peer_port, strerror(errno));
        conn_free(c); // give up on this attempt
        pt->next_retry = time(NULL) + RETRY_SECONDS; // try again later
    }
}

/* finish a nonblocking connect() once the socket becomes writable */
void handle_connect_complete(conn_t *c)
{
    int err = 0;
    socklen_t elen = sizeof(err);
    getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &elen); // did the connect actually succeed?
    peer_target_t *pt = &g_peers[c->peer_target_idx];     // the peer this conn belongs to
    if (err != 0)
    {
        LOG("[peer] connect to %s:%d failed: %s\n", pt->host, pt->peer_port, strerror(err));
        conn_free(c); // connect failed, drop it (schedules a retry via peer_conn_closed)
        return;
    }
    LOG("[peer] connected to %s:%d\n", pt->host, pt->peer_port); // connect succeeded
    c->pending_connect = 0;       // no longer waiting on connect()
    epoll_mod(c->fd, EPOLLIN, c); // switch to watching for input
    conn_enqueue_str(c, "SYNC\n"); // ask them for their current state
}

/* called by net.c when a peer connection (managed or not) is closed */
void peer_conn_closed(int peer_target_idx)
{
    peer_target_t *pt = &g_peers[peer_target_idx]; // the peer slot that lost its connection
    pt->conn = NULL;                                // no live connection anymore
    pt->next_retry = time(NULL) + RETRY_SECONDS;    // schedule a reconnect attempt
    LOG("[peer] link to %s:%d dropped, will retry in %ds\n",
        pt->host, pt->peer_port, RETRY_SECONDS);
}

/* retry any peer whose connection is down and whose retry time has passed */
void peer_sweep_reconnect(void)
{
    time_t now = time(NULL); // current time, checked once for the whole sweep
    for (int i = 0; i < g_npeers; i++)
        if (g_peers[i].conn == NULL && now >= g_peers[i].next_retry)
            start_peer_connect(i); // due for a (re)connect attempt
}
