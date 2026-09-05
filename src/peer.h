#ifndef PEER_H
#define PEER_H

#include "net.h"

#define PEER_PORT_OFFSET 10000 // peer port = client port + this offset
#define RETRY_SECONDS 3        // how long to wait before retrying a dropped peer link

void add_peer_target(const char *arg);      // parse a "host:port" or "port" CLI arg into g_peers
int peer_target_count(void);                // number of configured peer targets

void start_peer_connect(int idx);           // begin an outgoing connect() to peer idx
void handle_connect_complete(conn_t *c);    // finish a nonblocking connect() once writable
void peer_conn_closed(int peer_target_idx); // called by net.c when a peer conn is freed
void peer_sweep_reconnect(void);            // retry any peers that are due for reconnect

#endif
