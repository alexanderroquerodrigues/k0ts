#ifndef STATE_H
#define STATE_H

#define MAX_NAME 64
#define MAX_BID_ID 64

/* Pure auction state + tie-break rule. No sockets, no I/O. */
typedef struct {
    long long amount;          /* -1 means "no bid yet" */
    char bidder[MAX_NAME];
    char bid_id[MAX_BID_ID];
    int closed;
} auction_state_t;

void state_init(auction_state_t *s);

/*
 * Apply a candidate bid using the deterministic tie-break rule:
 *   higher amount wins; on a tie, lexicographically larger bid_id wins.
 * Returns 1 if the candidate became the new highest bid, 0 otherwise.
 * Idempotent/commutative: applying the same set of bids in any order or
 * more than once yields the same final state.
 */
int state_apply_bid(auction_state_t *s, const char *bid_id, const char *bidder, long long amount);

void state_close(auction_state_t *s);

#endif
