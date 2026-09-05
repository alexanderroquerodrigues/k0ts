#include <stdio.h>       // fprintf-family, snprintf
#include <stdlib.h>       // calloc, realloc, free
#include <string.h>       // memcpy, memmove, strerror
#include <unistd.h>       // close
#include <errno.h>        // errno, EAGAIN, EWOULDBLOCK, EINTR
#include <time.h>         // time()
#include <fcntl.h>        // fcntl, O_NONBLOCK
#include <sys/epoll.h>    // epoll_create1, epoll_ctl, epoll_wait
#include <sys/socket.h>   // socket, bind, listen, accept, send, recv, setsockopt
#include <netinet/in.h>   // sockaddr_in, htons
#include <arpa/inet.h>    // inet_ntop

#include "net.h"
#include "peer.h"
#include "protocol.h"
#include "log.h"

#define EPOLL_TIMEOUT_MS 1000 // how long epoll_wait blocks per loop iteration

conn_t *g_conns[MAX_FDS]; // fd -> conn_t lookup table
int g_epfd;               // the shared epoll instance

/* ---------- small helpers ---------- */

/* log the errno for msg and terminate the process */
void die(const char *msg)
{
    LOG("%s: %s\n", msg, strerror(errno)); // report what failed and why
    exit(1);                               // no recovery path for these failures
}

/* switch a socket fd to nonblocking mode */
void set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0); // read current fd flags
    if (flags < 0)
        die("fcntl(F_GETFL)"); // can't proceed without them
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) // add O_NONBLOCK
        die("fcntl(F_SETFL)");
}

/* register fd with epoll for the given events */
void epoll_add(int fd, uint32_t events, void *ptr)
{
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev)); // zero to avoid passing uninitialized padding
    ev.events = events;         // events to watch for
    ev.data.ptr = ptr;          // conn_t this event belongs to
    if (epoll_ctl(g_epfd, EPOLL_CTL_ADD, fd, &ev) < 0)
        die("epoll_ctl ADD");
}

/* change the events epoll watches for on fd */
void epoll_mod(int fd, uint32_t events, void *ptr)
{
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev)); // zero to avoid passing uninitialized padding
    ev.events = events;         // new event mask
    ev.data.ptr = ptr;          // conn_t this event belongs to
    if (epoll_ctl(g_epfd, EPOLL_CTL_MOD, fd, &ev) < 0)
        die("epoll_ctl MOD");
}

/* allocate a conn_t for fd and register it in the lookup table */
conn_t *conn_new(int fd, conn_type_t type)
{
    conn_t *c = calloc(1, sizeof(conn_t)); // zeroed struct, buffers start NULL/empty
    c->fd = fd;                            // remember the socket fd
    c->type = type;                        // remember what kind of conn this is
    c->peer_target_idx = -1;               // not a managed outgoing peer until set
    g_conns[fd] = c;                       // make it reachable by fd
    return c;
}

/*
 * A single epoll_wait() batch can hand back several events whose data.ptr
 * refers to the same connection (e.g. it also showed up as HUP earlier in
 * the same batch, or another event slot still points at it after it was
 * closed while handling an earlier slot). Freeing the conn_t immediately
 * would make those later slots dereference freed memory. So conn_free()
 * only closes the fd and clears g_conns[fd] right away (both must happen
 * immediately: the fd needs to be released, and a same-batch accept() may
 * legitimately reuse that fd number for a brand new connection); the
 * actual free() of the struct is deferred to conn_reap(), called once
 * after each batch has been fully processed. Every other place in this
 * file that touches a conn_t after a possible close checks identity via
 * `g_conns[c->fd] == c`, which stays correct even if the fd was reused,
 * since the table would then point at the new conn instead.
 */
static conn_t **g_to_free = NULL; // pending conn_t*'s awaiting free()
static int g_nto_free = 0;        // how many are queued
static int g_to_free_cap = 0;     // allocated capacity of g_to_free

/* close a connection's fd now, defer freeing its struct to conn_reap() */
void conn_free(conn_t *c)
{
    LOG("[net] fd=%d closing (type=%d)\n", c->fd, c->type); // trace every close
    if (c->peer_target_idx >= 0)
        peer_conn_closed(c->peer_target_idx); // let peer.c schedule a reconnect
    g_conns[c->fd] = NULL; // fd number may be reused by a later accept() in this batch
    close(c->fd);          // release the OS fd immediately
    if (g_nto_free == g_to_free_cap)
    {
        g_to_free_cap = g_to_free_cap ? g_to_free_cap * 2 : 16; // grow geometrically
        g_to_free = realloc(g_to_free, g_to_free_cap * sizeof(*g_to_free));
    }
    g_to_free[g_nto_free++] = c; // queue struct for freeing after this batch
}

/* free every conn_t queued by conn_free() since the last call */
void conn_reap(void)
{
    for (int i = 0; i < g_nto_free; i++)
    {
        free(g_to_free[i]->inbuf);  // release input buffer
        free(g_to_free[i]->outbuf); // release output buffer
        free(g_to_free[i]);         // release the struct itself
    }
    g_nto_free = 0; // queue is now empty
}

/* ---------- output buffering / backpressure ---------- */

/* append n bytes to c's output buffer, growing it as needed */
void conn_enqueue(conn_t *c, const char *data, size_t n)
{
    if (c->outlen + n > c->outcap)
    {
        size_t newcap = c->outcap ? c->outcap * 2 : 256; // start small, double from there
        while (newcap < c->outlen + n)
            newcap *= 2;                          // keep doubling until it fits
        c->outbuf = realloc(c->outbuf, newcap);   // grow the buffer
        c->outcap = newcap;                       // record new capacity
    }
    memcpy(c->outbuf + c->outlen, data, n); // append the new bytes
    c->outlen += n;                          // extend used length
    LOG("[net] fd=%d queued %zu bytes for output (backlog now %zu)\n",
        c->fd, n, c->outlen - c->outoff); // trace queue growth
    epoll_mod(c->fd, EPOLLIN | EPOLLOUT, c); // arm EPOLLOUT so we get notified when writable
}

/* convenience wrapper to enqueue a NUL-terminated string */
void conn_enqueue_str(conn_t *c, const char *s)
{
    conn_enqueue(c, s, strlen(s)); // delegate to the byte-buffer version
}

/* Drain as much of outbuf as the socket will take right now. Never blocks. */
void conn_flush(conn_t *c)
{
    while (c->outoff < c->outlen)
    {
        ssize_t n = send(c->fd, c->outbuf + c->outoff, c->outlen - c->outoff, MSG_NOSIGNAL); // try to send remaining bytes
        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                LOG("[net] fd=%d write would block, backing off (EPOLLOUT armed)\n", c->fd); // socket buffer is full
                return; /* stay armed for EPOLLOUT, try again later */
            }
            if (errno == EINTR) continue; // interrupted, just retry
            LOG("[net] fd=%d write error: %s\n", c->fd, strerror(errno)); // real error
            conn_free(c);                                                // give up on this conn
            return;
        }
        c->outoff += (size_t)n; // advance past the bytes we sent
    }
    /* fully drained */
    c->outlen = 0;                     // reset buffer to empty
    c->outoff = 0;                     // reset send offset
    epoll_mod(c->fd, EPOLLIN, c);      // no longer need EPOLLOUT notifications
}

/* ---------- TCP stream framing ---------- */

/* buffer newly-received bytes and dispatch every complete line to the protocol layer */
void conn_feed(conn_t *c, const char *data, size_t n)
{
    if (c->inlen + n > c->incap)
    {
        size_t newcap = c->incap ? c->incap * 2 : 256; // start small, double from there
        while (newcap < c->inlen + n)
            newcap *= 2;                 // keep doubling until it fits
        if (newcap > MAX_LINE_BUF)
            newcap = MAX_LINE_BUF;       // cap growth to guard against unbounded input
        if (c->inlen + n > newcap)
        {
            LOG("[net] fd=%d input line too long, dropping connection\n", c->fd); // line exceeds the cap
            conn_free(c);
            return;
        }
        c->inbuf = realloc(c->inbuf, newcap); // grow the buffer
        c->incap = newcap;                    // record new capacity
    }
    memcpy(c->inbuf + c->inlen, data, n); // append the newly received bytes
    c->inlen += n;                         // extend used length

    /* extract every complete '\n'-terminated line; a command may have
     * arrived split across recv()s, or several may share one recv(). */
    size_t start = 0; // start offset of the line currently being scanned
    for (size_t i = 0; i < c->inlen; i++)
    {
        if (c->inbuf[i] == '\n')
        {
            size_t linelen = i - start; // length of this line, excluding '\n'
            char linebuf[MAX_LINE_BUF]; // scratch copy so we can NUL-terminate
            if (linelen >= sizeof(linebuf))
                linelen = sizeof(linebuf) - 1; // truncate defensively
            memcpy(linebuf, c->inbuf + start, linelen); // copy the line out
            linebuf[linelen] = '\0';                    // NUL-terminate for string functions
            if (linelen > 0 && linebuf[linelen - 1] == '\r')
                linebuf[linelen - 1] = '\0'; // strip a trailing CR (CRLF input)

            if (c->type == CONN_CLIENT)
                handle_client_line(c, linebuf); // dispatch to client protocol handler
            else if (c->type == CONN_PEER)
                handle_peer_line(c, linebuf);   // dispatch to peer protocol handler

            /* handler may have freed c (e.g. line-too-long on a nested feed) */
            if (g_conns[c->fd] != c)
                return; // bail out, c is no longer valid

            start = i + 1; // next line starts right after this '\n'
        }
    }
    if (start > 0)
    {
        memmove(c->inbuf, c->inbuf + start, c->inlen - start); // shift leftover partial line to the front
        c->inlen -= start;                                     // shrink used length accordingly
    }
}

/* ---------- accept ---------- */

/* create a bound, listening, nonblocking TCP socket on port */
int make_listener(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0); // new TCP socket
    if (fd < 0)
        die("socket");
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)); // allow quick restart on the same port
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;      // IPv4
    addr.sin_addr.s_addr = INADDR_ANY; // listen on all interfaces
    addr.sin_port = htons(port);       // requested port, network byte order
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        die("bind");
    if (listen(fd, 128) < 0) // 128-deep backlog of pending connections
        die("listen");
    set_nonblocking(fd); // never block accept()
    return fd;
}

/* accept every pending connection on listen_fd, tagging each as type */
void handle_accept(int listen_fd, conn_type_t type)
{
    for (;;)
    {
        struct sockaddr_in peer_addr;
        socklen_t alen = sizeof(peer_addr);
        int fd = accept(listen_fd, (struct sockaddr *)&peer_addr, &alen); // pull the next pending connection
        if (fd < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return; // no more pending connections right now
            if (errno == EINTR)
                continue; // interrupted, retry
            LOG("[net] accept error: %s\n", strerror(errno)); // unexpected error
            return;
        }
        set_nonblocking(fd);                 // never let this conn block us
        conn_t *c = conn_new(fd, type);      // track the new connection
        epoll_add(fd, EPOLLIN, c);           // start out only watching for input
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer_addr.sin_addr, ip, sizeof(ip)); // format remote IP for logging
        LOG("[net] accepted fd=%d from %s:%d (%s)\n", fd, ip,
            ntohs(peer_addr.sin_port), type == CONN_PEER ? "peer" : "client");
        if (type == CONN_PEER)
        {
            /* let the other side know where we currently stand */
            conn_enqueue_str(c, "SYNC\n"); // ask the new peer to tell us its state too
        }
    }
}

/* ---------- main event loop ---------- */

/* run the epoll_wait loop forever, dispatching I/O and periodic peer retries */
void net_run_loop(void)
{
    struct epoll_event events[64]; // batch of events returned per epoll_wait call

    for (;;)
    {
        int n = epoll_wait(g_epfd, events, 64, EPOLL_TIMEOUT_MS); // wait for I/O or timeout
        if (n < 0)
        {
            if (errno == EINTR)
                continue; // interrupted, just wait again
            die("epoll_wait");
        }

        for (int i = 0; i < n; i++)
        {
            conn_t *c = (conn_t *)events[i].data.ptr; // conn this event belongs to
            uint32_t ev = events[i].events;            // which events fired

            if (c->type == CONN_CLIENT_LISTEN)
            {
                handle_accept(c->fd, CONN_CLIENT); // drain pending client accepts
                continue;
            }
            if (c->type == CONN_PEER_LISTEN)
            {
                handle_accept(c->fd, CONN_PEER); // drain pending peer accepts
                continue;
            }

            if (c->pending_connect && (ev & (EPOLLOUT | EPOLLERR)))
            {
                handle_connect_complete(c); // nonblocking connect() finished, check result
                if (g_conns[c->fd] != c)
                    continue;              /* freed on failure */
                ev &= ~(uint32_t)EPOLLOUT; /* already handled */
            }

            if (ev & (EPOLLHUP | EPOLLERR))
            {
                LOG("[net] fd=%d HUP/ERR\n", c->fd); // remote hung up or socket errored
                conn_free(c);
                continue;
            }

            if (ev & EPOLLIN)
            {
                char buf[4096]; // scratch recv buffer
                for (;;)
                {
                    ssize_t r = recv(c->fd, buf, sizeof(buf), 0); // pull available bytes
                    if (r > 0)
                    {
                        conn_feed(c, buf, (size_t)r); // hand bytes to the framer
                        if (g_conns[c->fd] != c)
                            break; /* conn_feed freed it */
                        if ((size_t)r < sizeof(buf))
                            break; /* drained for now */
                        continue;  // buffer was full, more may be waiting
                    }
                    else if (r == 0)
                    {
                        LOG("[net] fd=%d peer closed connection\n", c->fd); // orderly close
                        conn_free(c);
                        break;
                    }
                    else
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break; // nothing more to read right now
                        if (errno == EINTR)
                            continue; // interrupted, retry
                        LOG("[net] fd=%d recv error: %s\n", c->fd, strerror(errno)); // real error
                        conn_free(c);
                        break;
                    }
                }
                if (g_conns[c->fd] != c)
                    continue; // conn was freed above, skip further handling
            }

            if ((ev & EPOLLOUT) && g_conns[c->fd] == c)
                conn_flush(c); // socket is writable, push queued output
        }

        conn_reap(); /* safe now: no more stale events[] slots left to read */

        /* periodic reconnect sweep -- never blocks the loop */
        peer_sweep_reconnect(); // retry any peer links that are due
    }
}
