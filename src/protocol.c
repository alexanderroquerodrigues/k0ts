#include <stdio.h>  // snprintf, sscanf
#include <string.h> // strcmp

#include "protocol.h"
#include "state.h"
#include "dedup.h"
#include "log.h"

static char g_node_id[32];       // this node's identifier, used in bid ids
static long long g_seq = 0;      // monotonically increasing local bid sequence number
static auction_state_t g_state;  // the current auction state
static dedup_set_t g_dedup;      // set of bid ids already applied, for replication dedup

/* set up auction state, dedup set, and remember our node id */
void protocol_init(const char *node_id)
{
    snprintf(g_node_id, sizeof(g_node_id), "%s", node_id); // remember our id for bid ids
    state_init(&g_state);       // no bids yet
    dedup_init(&g_dedup, 4096); // start with an empty dedup table
}

/* ---------- replication ---------- */

/* send msg to every currently-connected peer (not ones still mid-connect) */
void broadcast_to_peers(const char *msg)
{
    for (int fd = 0; fd < MAX_FDS; fd++)
    {
        conn_t *c = g_conns[fd]; // check every slot for a live peer conn
        if (c && c->type == CONN_PEER && !c->pending_connect)
        {
            conn_enqueue_str(c, msg); // queue for delivery
        }
    }
}

/* send msg to every currently-connected client */
void broadcast_to_clients(const char *msg)
{
    for (int fd = 0; fd < MAX_FDS; fd++)
    {
        conn_t *c = g_conns[fd]; // check every slot for a live client conn
        if (c && c->type == CONN_CLIENT)
        {
            conn_enqueue_str(c, msg); // queue for delivery
        }
    }
}

/* reply to a peer's SYNC request with our current leading bid (or "none") */
void send_current_to_peer(conn_t *c)
{
    char line[256];
    if (g_state.amount < 0)
    {
        snprintf(line, sizeof(line), "CURRENT %s:0 none 0\n", g_node_id); // no bids yet
    }
    else
    {
        snprintf(line, sizeof(line), "CURRENT %s %s %lld\n",
                 g_state.bid_id, g_state.bidder, g_state.amount); // our current leader
    }
    conn_enqueue_str(c, line);
}

/* apply a bid coming from replication (BID or CURRENT peer message) */
static void apply_replicated_bid(const char *bid_id, const char *bidder, long long amount)
{
    if (!dedup_insert(&g_dedup, bid_id))
    {
        return; /* already processed -- duplicate replication, ignore */
    }
    state_apply_bid(&g_state, bid_id, bidder, amount); // fold it into our state
}

/* ---------- client protocol ---------- */

/* parse and act on one line of client input */
void handle_client_line(conn_t *c, char *line)
{
    LOG("[client fd=%d] <- %s\n", c->fd, line); // trace every client command
    char cmd[32] = {0};
    if (sscanf(line, "%31s", cmd) != 1)
        return; // blank or unparsable line, ignore

    if (strcmp(cmd, "REGISTER") == 0)
    {
        char name[MAX_NAME] = {0};
        if (sscanf(line, "%*s %63s", name) == 1)
        {
            snprintf(c->name, sizeof(c->name), "%s", name); // remember the client's name
            c->registered = 1;                              // now allowed to bid
            conn_enqueue_str(c, "OK REGISTERED\n");
        }
        else
        {
            conn_enqueue_str(c, "REJECTED\n"); // missing/invalid name
        }
    }
    else if (strcmp(cmd, "BID") == 0)
    {
        long long amount;
        if (!c->registered)
        {
            LOG("[client fd=%d] BID rejected: not registered\n", c->fd);
            conn_enqueue_str(c, "REJECTED\n"); // must REGISTER before bidding
        }
        else if (g_state.closed)
        {
            LOG("[client fd=%d] BID rejected: auction closed\n", c->fd);
            conn_enqueue_str(c, "REJECTED\n"); // auction already closed
        }
        else if (sscanf(line, "%*s %lld", &amount) != 1 || amount <= 0)
        {
            conn_enqueue_str(c, "REJECTED\n"); // missing/invalid/non-positive amount
        }
        else if (amount <= g_state.amount)
        {
            LOG("[client fd=%d] BID %lld rejected: does not beat current %lld\n",
                c->fd, amount, g_state.amount);
            conn_enqueue_str(c, "REJECTED\n"); // doesn't beat the current leader
        }
        else
        {
            char bid_id[MAX_BID_ID];
            snprintf(bid_id, sizeof(bid_id), "%s:%lld", g_node_id, ++g_seq); // unique local bid id

            state_apply_bid(&g_state, bid_id, c->name, amount); // accept it locally
            dedup_insert(&g_dedup, bid_id); /* our own bid is "processed" too */

            conn_enqueue_str(c, "OK ACCEPTED\n");

            char rep[256];
            snprintf(rep, sizeof(rep), "BID %s %s %lld\n", bid_id, c->name, amount);
            LOG("[replication] queuing to all peers: %s", rep);
            broadcast_to_peers(rep); // tell every peer about the new leader
        }
    }
    else if (strcmp(cmd, "STATUS") == 0)
    {
        char line_out[256];
        if (g_state.amount < 0)
        {
            snprintf(line_out, sizeof(line_out), "CURRENT 0 none\n"); // no bids yet
        }
        else
        {
            snprintf(line_out, sizeof(line_out), "CURRENT %lld %s\n", g_state.amount, g_state.bidder);
        }
        conn_enqueue_str(c, line_out);
    }
    else if (strcmp(cmd, "CLOSE") == 0)
    {
        state_close(&g_state); // mark the auction closed
        conn_enqueue_str(c, "OK CLOSED\n");

        char notice[256];
        if (g_state.amount < 0)
        {
            snprintf(notice, sizeof(notice), "AUCTION CLOSED\nWINNER none 0\n"); // no bids were placed
        }
        else
        {
            snprintf(notice, sizeof(notice), "AUCTION CLOSED\nWINNER %s %lld\n",
                     g_state.bidder, g_state.amount);
        }
        broadcast_to_clients(notice); // tell every client the result
    }
    else
    {
        conn_enqueue_str(c, "REJECTED\n"); // unknown command
    }
}

/* ---------- peer protocol ---------- */

/* parse and act on one line of peer replication traffic */
void handle_peer_line(conn_t *c, char *line)
{
    LOG("[peer fd=%d] <- %s\n", c->fd, line); // trace every peer message
    char cmd[32] = {0};
    if (sscanf(line, "%31s", cmd) != 1)
        return; // blank or unparsable line, ignore

    if (strcmp(cmd, "BID") == 0)
    {
        char bid_id[MAX_BID_ID], bidder[MAX_NAME];
        long long amount;
        if (sscanf(line, "%*s %63s %63s %lld", bid_id, bidder, &amount) == 3)
        {
            apply_replicated_bid(bid_id, bidder, amount); // fold in the replicated bid
        }
    }
    else if (strcmp(cmd, "CURRENT") == 0)
    {
        char bid_id[MAX_BID_ID], bidder[MAX_NAME];
        long long amount;
        if (sscanf(line, "%*s %63s %63s %lld", bid_id, bidder, &amount) == 3)
        {
            apply_replicated_bid(bid_id, bidder, amount); // sync to their reported leader
        }
    }
    else if (strcmp(cmd, "SYNC") == 0)
    {
        LOG("[peer fd=%d] SYNC requested, replying with CURRENT\n", c->fd);
        send_current_to_peer(c); // tell them where we stand
    }
    else
    {
        LOG("[peer fd=%d] unknown peer command, ignoring\n", c->fd);
    }
}
