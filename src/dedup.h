#ifndef DEDUP_H
#define DEDUP_H

#include <stddef.h>

/*
 * processed_bid_ids: a plain chained hash set of bid_id strings.
 * Grows unbounded for the process lifetime. Needs to be reworked later.
 */

typedef struct dedup_node {
    char *bid_id;
    struct dedup_node *next;
} dedup_node_t;

typedef struct {
    dedup_node_t **buckets;
    size_t nbuckets;
    size_t count;
} dedup_set_t;

void dedup_init(dedup_set_t *set, size_t nbuckets);
void dedup_free(dedup_set_t *set);

/* Returns 1 if bid_id was newly inserted, 0 if it was already present. */
int dedup_insert(dedup_set_t *set, const char *bid_id);

#endif
