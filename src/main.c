/*
 * The Auction Nobody Can See -- single-node auction server, replicates
 * asynchronously to peer nodes. See DESIGN.md for the full write-up.
 *
 * Usage: ./auction <node_id> <client_port> [peer_client_port ...]
 *   - listens for clients on <client_port>
 *   - listens for peers  on <client_port> + PEER_PORT_OFFSET
 *   - actively connects out to each peer_client_port's peer port
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "state.h"
#include "dedup.h"

#define PEER_PORT_OFFSET 10000
#define MAX_FDS 65536
#define MAX_LINE_BUF 8192
#define RETRY_SECONDS 3
#define EPOLL_TIMEOUT_MS 1000

typedef enum {
    CONN_CLIENT_LISTEN,
    CONN_PEER_LISTEN,
    CONN_CLIENT,
    CONN_PEER
} conn_type_t;

typedef struct conn {
    int fd;
    conn_type_t type;

    char *inbuf;
    size_t inlen, incap;

    char *outbuf;
    size_t outlen, outcap, outoff;

    int registered;
    char name[MAX_NAME];

    int pending_connect; /* outgoing peer socket, connect() in flight */
    int peer_target_idx; /* -1 if not an outgoing-managed peer conn */
} conn_t;

typedef struct {
    char host[128];
    int client_port;
    int peer_port;
    conn_t *conn;      /* NULL if not currently connected */
    time_t next_retry;
} peer_target_t;

static conn_t *g_conns[MAX_FDS];
static int g_epfd;
static char g_node_id[32];
static long long g_seq = 0;
static auction_state_t g_state;
static dedup_set_t g_dedup;

static peer_target_t g_peers[64];
static int g_npeers = 0;

/* ---------- small helpers ---------- */

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) die("fcntl(F_GETFL)");
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) die("fcntl(F_SETFL)");
}

static void epoll_add(int fd, uint32_t events, void *ptr) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.ptr = ptr;
    if (epoll_ctl(g_epfd, EPOLL_CTL_ADD, fd, &ev) < 0) die("epoll_ctl ADD");
}

static void epoll_mod(int fd, uint32_t events, void *ptr) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.ptr = ptr;
    if (epoll_ctl(g_epfd, EPOLL_CTL_MOD, fd, &ev) < 0) die("epoll_ctl MOD");
}

static conn_t *conn_new(int fd, conn_type_t type) {
    conn_t *c = calloc(1, sizeof(conn_t));
    c->fd = fd;
    c->type = type;
    c->peer_target_idx = -1;
    g_conns[fd] = c;
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
static conn_t **g_to_free = NULL;
static int g_nto_free = 0;
static int g_to_free_cap = 0;

static void conn_free(conn_t *c) {
    fprintf(stderr, "[net] fd=%d closing (type=%d)\n", c->fd, c->type);
    if (c->peer_target_idx >= 0) {
        peer_target_t *pt = &g_peers[c->peer_target_idx];
        pt->conn = NULL;
        pt->next_retry = time(NULL) + RETRY_SECONDS;
        fprintf(stderr, "[peer] link to %s:%d dropped, will retry in %ds\n",
                pt->host, pt->peer_port, RETRY_SECONDS);
    }
    g_conns[c->fd] = NULL;
    close(c->fd);
    if (g_nto_free == g_to_free_cap) {
        g_to_free_cap = g_to_free_cap ? g_to_free_cap * 2 : 16;
        g_to_free = realloc(g_to_free, g_to_free_cap * sizeof(*g_to_free));
    }
    g_to_free[g_nto_free++] = c;
}

static void conn_reap(void) {
    for (int i = 0; i < g_nto_free; i++) {
        free(g_to_free[i]->inbuf);
        free(g_to_free[i]->outbuf);
        free(g_to_free[i]);
    }
    g_nto_free = 0;
}

/* ---------- output buffering / backpressure ---------- */

static void conn_enqueue(conn_t *c, const char *data, size_t n) {
    if (c->outlen + n > c->outcap) {
        size_t newcap = c->outcap ? c->outcap * 2 : 256;
        while (newcap < c->outlen + n) newcap *= 2;
        c->outbuf = realloc(c->outbuf, newcap);
        c->outcap = newcap;
    }
    memcpy(c->outbuf + c->outlen, data, n);
    c->outlen += n;
    fprintf(stderr, "[net] fd=%d queued %zu bytes for output (backlog now %zu)\n",
            c->fd, n, c->outlen - c->outoff);
    epoll_mod(c->fd, EPOLLIN | EPOLLOUT, c);
}

static void conn_enqueue_str(conn_t *c, const char *s) {
    conn_enqueue(c, s, strlen(s));
}

/* Drain as much of outbuf as the socket will take right now. Never blocks. */
static void conn_flush(conn_t *c) {
    while (c->outoff < c->outlen) {
        ssize_t n = send(c->fd, c->outbuf + c->outoff, c->outlen - c->outoff, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                fprintf(stderr, "[net] fd=%d write would block, backing off (EPOLLOUT armed)\n", c->fd);
                return; /* stay armed for EPOLLOUT, try again later */
            }
            if (errno == EINTR) continue;
            fprintf(stderr, "[net] fd=%d write error: %s\n", c->fd, strerror(errno));
            conn_free(c);
            return;
        }
        c->outoff += (size_t)n;
    }
    /* fully drained */
    c->outlen = 0;
    c->outoff = 0;
    epoll_mod(c->fd, EPOLLIN, c);
}

/* ---------- replication ---------- */

static void broadcast_to_peers(const char *msg) {
    for (int fd = 0; fd < MAX_FDS; fd++) {
        conn_t *c = g_conns[fd];
        if (c && c->type == CONN_PEER && !c->pending_connect) {
            conn_enqueue_str(c, msg);
        }
    }
}

static void broadcast_to_clients(const char *msg) {
    for (int fd = 0; fd < MAX_FDS; fd++) {
        conn_t *c = g_conns[fd];
        if (c && c->type == CONN_CLIENT) {
            conn_enqueue_str(c, msg);
        }
    }
}

static void send_current_to_peer(conn_t *c) {
    char line[256];
    if (g_state.amount < 0) {
        snprintf(line, sizeof(line), "CURRENT %s:0 none 0\n", g_node_id);
    } else {
        snprintf(line, sizeof(line), "CURRENT %s %s %lld\n",
                 g_state.bid_id, g_state.bidder, g_state.amount);
    }
    conn_enqueue_str(c, line);
}

/* apply a bid coming from replication (BID or CURRENT peer message) */
static void apply_replicated_bid(const char *bid_id, const char *bidder, long long amount) {
    if (!dedup_insert(&g_dedup, bid_id)) {
        return; /* already processed -- duplicate replication, ignore */
    }
    state_apply_bid(&g_state, bid_id, bidder, amount);
}

/* ---------- client protocol ---------- */

static void handle_client_line(conn_t *c, char *line) {
    fprintf(stderr, "[client fd=%d] <- %s\n", c->fd, line);
    char cmd[32] = {0};
    if (sscanf(line, "%31s", cmd) != 1) return;

    if (strcmp(cmd, "REGISTER") == 0) {
        char name[MAX_NAME] = {0};
        if (sscanf(line, "%*s %63s", name) == 1) {
            snprintf(c->name, sizeof(c->name), "%s", name);
            c->registered = 1;
            conn_enqueue_str(c, "OK REGISTERED\n");
        } else {
            conn_enqueue_str(c, "REJECTED\n");
        }
    } else if (strcmp(cmd, "BID") == 0) {
        long long amount;
        if (!c->registered) {
            fprintf(stderr, "[client fd=%d] BID rejected: not registered\n", c->fd);
            conn_enqueue_str(c, "REJECTED\n");
        } else if (g_state.closed) {
            fprintf(stderr, "[client fd=%d] BID rejected: auction closed\n", c->fd);
            conn_enqueue_str(c, "REJECTED\n");
        } else if (sscanf(line, "%*s %lld", &amount) != 1 || amount <= 0) {
            conn_enqueue_str(c, "REJECTED\n");
        } else if (amount <= g_state.amount) {
            fprintf(stderr, "[client fd=%d] BID %lld rejected: does not beat current %lld\n",
                    c->fd, amount, g_state.amount);
            conn_enqueue_str(c, "REJECTED\n");
        } else {
            char bid_id[MAX_BID_ID];
            snprintf(bid_id, sizeof(bid_id), "%s:%lld", g_node_id, ++g_seq);

            state_apply_bid(&g_state, bid_id, c->name, amount);
            dedup_insert(&g_dedup, bid_id); /* our own bid is "processed" too */

            conn_enqueue_str(c, "OK ACCEPTED\n");

            char rep[256];
            snprintf(rep, sizeof(rep), "BID %s %s %lld\n", bid_id, c->name, amount);
            fprintf(stderr, "[replication] queuing to all peers: %s", rep);
            broadcast_to_peers(rep);
        }
    } else if (strcmp(cmd, "STATUS") == 0) {
        char line_out[256];
        if (g_state.amount < 0) {
            snprintf(line_out, sizeof(line_out), "CURRENT 0 none\n");
        } else {
            snprintf(line_out, sizeof(line_out), "CURRENT %lld %s\n", g_state.amount, g_state.bidder);
        }
        conn_enqueue_str(c, line_out);
    } else if (strcmp(cmd, "CLOSE") == 0) {
        state_close(&g_state);
        conn_enqueue_str(c, "OK CLOSED\n");

        char notice[256];
        if (g_state.amount < 0) {
            snprintf(notice, sizeof(notice), "AUCTION CLOSED\nWINNER none 0\n");
        } else {
            snprintf(notice, sizeof(notice), "AUCTION CLOSED\nWINNER %s %lld\n",
                     g_state.bidder, g_state.amount);
        }
        broadcast_to_clients(notice);
    } else {
        conn_enqueue_str(c, "REJECTED\n");
    }
}

/* ---------- peer protocol ---------- */

static void handle_peer_line(conn_t *c, char *line) {
    fprintf(stderr, "[peer fd=%d] <- %s\n", c->fd, line);
    char cmd[32] = {0};
    if (sscanf(line, "%31s", cmd) != 1) return;

    if (strcmp(cmd, "BID") == 0) {
        char bid_id[MAX_BID_ID], bidder[MAX_NAME];
        long long amount;
        if (sscanf(line, "%*s %63s %63s %lld", bid_id, bidder, &amount) == 3) {
            apply_replicated_bid(bid_id, bidder, amount);
        }
    } else if (strcmp(cmd, "CURRENT") == 0) {
        char bid_id[MAX_BID_ID], bidder[MAX_NAME];
        long long amount;
        if (sscanf(line, "%*s %63s %63s %lld", bid_id, bidder, &amount) == 3) {
            apply_replicated_bid(bid_id, bidder, amount);
        }
    } else if (strcmp(cmd, "SYNC") == 0) {
        fprintf(stderr, "[peer fd=%d] SYNC requested, replying with CURRENT\n", c->fd);
        send_current_to_peer(c);
    } else {
        fprintf(stderr, "[peer fd=%d] unknown peer command, ignoring\n", c->fd);
    }
}

/* ---------- TCP stream framing ---------- */

static void conn_feed(conn_t *c, const char *data, size_t n) {
    if (c->inlen + n > c->incap) {
        size_t newcap = c->incap ? c->incap * 2 : 256;
        while (newcap < c->inlen + n) newcap *= 2;
        if (newcap > MAX_LINE_BUF) newcap = MAX_LINE_BUF;
        if (c->inlen + n > newcap) {
            fprintf(stderr, "[net] fd=%d input line too long, dropping connection\n", c->fd);
            conn_free(c);
            return;
        }
        c->inbuf = realloc(c->inbuf, newcap);
        c->incap = newcap;
    }
    memcpy(c->inbuf + c->inlen, data, n);
    c->inlen += n;

    /* extract every complete '\n'-terminated line; a command may have
     * arrived split across recv()s, or several may share one recv(). */
    size_t start = 0;
    for (size_t i = 0; i < c->inlen; i++) {
        if (c->inbuf[i] == '\n') {
            size_t linelen = i - start;
            char linebuf[MAX_LINE_BUF];
            if (linelen >= sizeof(linebuf)) linelen = sizeof(linebuf) - 1;
            memcpy(linebuf, c->inbuf + start, linelen);
            linebuf[linelen] = '\0';
            if (linelen > 0 && linebuf[linelen - 1] == '\r') linebuf[linelen - 1] = '\0';

            if (c->type == CONN_CLIENT) handle_client_line(c, linebuf);
            else if (c->type == CONN_PEER) handle_peer_line(c, linebuf);

            /* handler may have freed c (e.g. line-too-long on a nested feed) */
            if (g_conns[c->fd] != c) return;

            start = i + 1;
        }
    }
    if (start > 0) {
        memmove(c->inbuf, c->inbuf + start, c->inlen - start);
        c->inlen -= start;
    }
}

/* ---------- accept / connect ---------- */

static int make_listener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) die("socket");
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) die("bind");
    if (listen(fd, 128) < 0) die("listen");
    set_nonblocking(fd);
    return fd;
}

static void handle_accept(int listen_fd, conn_type_t type) {
    for (;;) {
        struct sockaddr_in peer_addr;
        socklen_t alen = sizeof(peer_addr);
        int fd = accept(listen_fd, (struct sockaddr *)&peer_addr, &alen);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            fprintf(stderr, "[net] accept error: %s\n", strerror(errno));
            return;
        }
        set_nonblocking(fd);
        conn_t *c = conn_new(fd, type);
        epoll_add(fd, EPOLLIN, c);
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer_addr.sin_addr, ip, sizeof(ip));
        fprintf(stderr, "[net] accepted fd=%d from %s:%d (%s)\n", fd, ip,
                ntohs(peer_addr.sin_port), type == CONN_PEER ? "peer" : "client");
        if (type == CONN_PEER) {
            /* let the other side know where we currently stand */
            conn_enqueue_str(c, "SYNC\n");
        }
    }
}

static void start_peer_connect(int idx) {
    peer_target_t *pt = &g_peers[idx];
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return; }
    set_nonblocking(fd);

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(pt->peer_port);
    if (inet_pton(AF_INET, pt->host, &addr.sin_addr) != 1) {
        fprintf(stderr, "[peer] bad host %s, skipping\n", pt->host);
        close(fd);
        pt->next_retry = time(NULL) + RETRY_SECONDS;
        return;
    }

    fprintf(stderr, "[peer] connecting to %s:%d ...\n", pt->host, pt->peer_port);
    int rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    conn_t *c = conn_new(fd, CONN_PEER);
    c->peer_target_idx = idx;
    pt->conn = c;

    if (rc == 0) {
        c->pending_connect = 0;
        epoll_add(fd, EPOLLIN, c);
        fprintf(stderr, "[peer] connected to %s:%d immediately\n", pt->host, pt->peer_port);
        conn_enqueue_str(c, "SYNC\n");
    } else if (errno == EINPROGRESS) {
        c->pending_connect = 1;
        epoll_add(fd, EPOLLOUT, c);
    } else {
        fprintf(stderr, "[peer] connect to %s:%d failed: %s\n", pt->host, pt->peer_port, strerror(errno));
        conn_free(c);
        pt->next_retry = time(NULL) + RETRY_SECONDS;
    }
}

static void handle_connect_complete(conn_t *c) {
    int err = 0;
    socklen_t elen = sizeof(err);
    getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &elen);
    peer_target_t *pt = &g_peers[c->peer_target_idx];
    if (err != 0) {
        fprintf(stderr, "[peer] connect to %s:%d failed: %s\n", pt->host, pt->peer_port, strerror(err));
        conn_free(c);
        return;
    }
    fprintf(stderr, "[peer] connected to %s:%d\n", pt->host, pt->peer_port);
    c->pending_connect = 0;
    epoll_mod(c->fd, EPOLLIN, c);
    conn_enqueue_str(c, "SYNC\n");
}

/* ---------- peer target config ---------- */

static void add_peer_target(const char *arg) {
    peer_target_t *pt = &g_peers[g_npeers];
    const char *colon = strchr(arg, ':');
    if (colon) {
        size_t hlen = (size_t)(colon - arg);
        if (hlen >= sizeof(pt->host)) hlen = sizeof(pt->host) - 1;
        memcpy(pt->host, arg, hlen);
        pt->host[hlen] = '\0';
        pt->client_port = atoi(colon + 1);
    } else {
        snprintf(pt->host, sizeof(pt->host), "127.0.0.1");
        pt->client_port = atoi(arg);
    }
    pt->peer_port = pt->client_port + PEER_PORT_OFFSET;
    pt->conn = NULL;
    pt->next_retry = 0; /* connect right away */
    g_npeers++;
}

/* ---------- main loop ---------- */

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s <node_id> <client_port> [peer_client_port ...]\n", argv[0]);
        return 1;
    }
    signal(SIGPIPE, SIG_IGN);

    snprintf(g_node_id, sizeof(g_node_id), "%s", argv[1]);
    int client_port = atoi(argv[2]);
    int peer_port = client_port + PEER_PORT_OFFSET;

    for (int i = 3; i < argc; i++) add_peer_target(argv[i]);

    state_init(&g_state);
    dedup_init(&g_dedup, 4096);

    g_epfd = epoll_create1(0);
    if (g_epfd < 0) die("epoll_create1");

    int client_listen_fd = make_listener(client_port);
    int peer_listen_fd = make_listener(peer_port);

    conn_t *client_listener = conn_new(client_listen_fd, CONN_CLIENT_LISTEN);
    conn_t *peer_listener = conn_new(peer_listen_fd, CONN_PEER_LISTEN);
    epoll_add(client_listen_fd, EPOLLIN, client_listener);
    epoll_add(peer_listen_fd, EPOLLIN, peer_listener);

    fprintf(stderr, "[main] node_id=%s client_port=%d peer_port=%d peers=%d\n",
            g_node_id, client_port, peer_port, g_npeers);

    struct epoll_event events[64];
    for (;;) {
        int n = epoll_wait(g_epfd, events, 64, EPOLL_TIMEOUT_MS);
        if (n < 0) {
            if (errno == EINTR) continue;
            die("epoll_wait");
        }

        for (int i = 0; i < n; i++) {
            conn_t *c = (conn_t *)events[i].data.ptr;
            uint32_t ev = events[i].events;

            if (c->type == CONN_CLIENT_LISTEN) {
                handle_accept(c->fd, CONN_CLIENT);
                continue;
            }
            if (c->type == CONN_PEER_LISTEN) {
                handle_accept(c->fd, CONN_PEER);
                continue;
            }

            if (c->pending_connect && (ev & (EPOLLOUT | EPOLLERR))) {
                handle_connect_complete(c);
                if (g_conns[c->fd] != c) continue; /* freed on failure */
                ev &= ~(uint32_t)EPOLLOUT; /* already handled */
            }

            if (ev & (EPOLLHUP | EPOLLERR)) {
                fprintf(stderr, "[net] fd=%d HUP/ERR\n", c->fd);
                conn_free(c);
                continue;
            }

            if (ev & EPOLLIN) {
                char buf[4096];
                for (;;) {
                    ssize_t r = recv(c->fd, buf, sizeof(buf), 0);
                    if (r > 0) {
                        conn_feed(c, buf, (size_t)r);
                        if (g_conns[c->fd] != c) break; /* conn_feed freed it */
                        if ((size_t)r < sizeof(buf)) break; /* drained for now */
                        continue;
                    } else if (r == 0) {
                        fprintf(stderr, "[net] fd=%d peer closed connection\n", c->fd);
                        conn_free(c);
                        break;
                    } else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        if (errno == EINTR) continue;
                        fprintf(stderr, "[net] fd=%d recv error: %s\n", c->fd, strerror(errno));
                        conn_free(c);
                        break;
                    }
                }
                if (g_conns[c->fd] != c) continue;
            }

            if ((ev & EPOLLOUT) && g_conns[c->fd] == c) {
                conn_flush(c);
            }
        }

        conn_reap(); /* safe now: no more stale events[] slots left to read */

        /* periodic reconnect sweep -- never blocks the loop */
        time_t now = time(NULL);
        for (int i = 0; i < g_npeers; i++) {
            if (g_peers[i].conn == NULL && now >= g_peers[i].next_retry) {
                start_peer_connect(i);
            }
        }
    }

    return 0;
}
