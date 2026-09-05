#include <string.h>
#include <stdio.h>
#include "state.h"
#include "log.h"

void state_init(auction_state_t *s) {
    s->amount = -1;
    s->bidder[0] = '\0';
    s->bid_id[0] = '\0';
    s->closed = 0;
}

int state_apply_bid(auction_state_t *s, const char *bid_id, const char *bidder, long long amount) {
    int wins = 0;

    if (amount > s->amount) wins = 1;
    else if (amount == s->amount && strcmp(bid_id, s->bid_id) > 0) wins = 1; /* deterministic tie-break: larger bid_id wins */

    LOG("[state] candidate bid_id=%s bidder=%s amount=%lld vs current amount=%lld bid_id=%s -> %s\n", bid_id, bidder, amount, s->amount, s->bid_id, wins ? "WINS" : "loses");

    if (wins) {
        s->amount = amount;
        snprintf(s->bidder, sizeof(s->bidder), "%s", bidder);
        snprintf(s->bid_id, sizeof(s->bid_id), "%s", bid_id);
    }
    return wins;
}

void state_close(auction_state_t *s) {
    s->closed = 1;
    LOG("[state] auction CLOSED winner=%s amount=%lld bid_id=%s\n", s->bidder, s->amount, s->bid_id);
}
