#include <stdio.h>     // fprintf
#include <stdlib.h>    // atoi
#include <signal.h>    // signal, SIGPIPE
#include <sys/epoll.h> // epoll_create1, EPOLLIN

#include "net.h"
#include "peer.h"
#include "protocol.h"
#include "log.h"

/* parse CLI args, wire up listeners and peers, and run the event loop */
int main(int argc, char **argv)
{
    if (argc < 3)
    {
        fprintf(stderr, "usage: %s <node_id> <client_port> [peer_client_port ...]\n", argv[0]);
        return 1; // missing required arguments
    }
    signal(SIGPIPE, SIG_IGN); // a dead peer's socket must not kill us on write

    const char *node_id = argv[1];               // this node's identifier
    int client_port = atoi(argv[2]);              // port clients connect to
    int peer_port = client_port + PEER_PORT_OFFSET; // derived replication port

    for (int i = 3; i < argc; i++)
        add_peer_target(argv[i]); // remaining args are peer addresses to dial

    protocol_init(node_id); // set up auction state and dedup tracking

    g_epfd = epoll_create1(0); // shared epoll instance for all sockets
    if (g_epfd < 0)
        die("epoll_create1");

    int client_listen_fd = make_listener(client_port); // accept client connections here
    int peer_listen_fd = make_listener(peer_port);     // accept peer connections here

    conn_t *client_listener = conn_new(client_listen_fd, CONN_CLIENT_LISTEN);
    conn_t *peer_listener = conn_new(peer_listen_fd, CONN_PEER_LISTEN);
    epoll_add(client_listen_fd, EPOLLIN, client_listener); // watch for incoming client connections
    epoll_add(peer_listen_fd, EPOLLIN, peer_listener);     // watch for incoming peer connections

    LOG("[main] node_id=%s client_port=%d peer_port=%d peers=%d\n",
        node_id, client_port, peer_port, peer_target_count());

    net_run_loop(); // never returns

    return 0;
}
