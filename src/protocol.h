#ifndef PROTOCOL_H
#define PROTOCOL_H

#include "net.h"

void protocol_init(const char *node_id); // set up auction state, dedup set, node id

void handle_client_line(conn_t *c, char *line); // parse and act on one client command
void handle_peer_line(conn_t *c, char *line);   // parse and act on one peer replication message

void send_current_to_peer(conn_t *c);        // reply to a peer's SYNC with our current bid
void broadcast_to_peers(const char *msg);    // send msg to every connected peer
void broadcast_to_clients(const char *msg);  // send msg to every connected client

#endif
