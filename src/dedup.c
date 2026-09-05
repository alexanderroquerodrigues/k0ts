#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "dedup.h"
#include "log.h"

static unsigned long hash_str(const char *s) {
    /* FNV-1a */
    unsigned long h = 2166136261UL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 16777619UL;
    }
    return h;
}

void dedup_init(dedup_set_t *set, size_t nbuckets) {
    set->nbuckets = nbuckets;
    set->count = 0;
    set->buckets = calloc(nbuckets, sizeof(dedup_node_t *));
}

void dedup_free(dedup_set_t *set) {
    for (size_t i = 0; i < set->nbuckets; i++) {
        dedup_node_t *n = set->buckets[i];
        while (n) {
            dedup_node_t *next = n->next;
            free(n->bid_id);
            free(n);
            n = next;
        }
    }
    free(set->buckets);
    set->buckets = NULL;
}

int dedup_insert(dedup_set_t *set, const char *bid_id) {
    unsigned long h = hash_str(bid_id) % set->nbuckets;
    for (dedup_node_t *n = set->buckets[h]; n; n = n->next) {
        if (strcmp(n->bid_id, bid_id) == 0) {
            LOG("[dedup] bid_id=%s already processed, dropping duplicate\n", bid_id);
            return 0;
        }
    }
    dedup_node_t *n = malloc(sizeof(dedup_node_t));
    n->bid_id = strdup(bid_id);
    n->next = set->buckets[h];
    set->buckets[h] = n;
    set->count++;
    LOG("[dedup] bid_id=%s recorded (total processed=%zu)\n", bid_id, set->count);
    return 1;
}
