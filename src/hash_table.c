#define _BSD_SOURCE

#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "hash_table.h"
#include "utils.h"

void hash_table_init(HashTable *h, unsigned int initial_capacity, float load_factor) {
    unsigned int minimum_capacity = ceilf(1 / load_factor);
    initial_capacity = round_up_power_of_two(
            initial_capacity > minimum_capacity ? initial_capacity : minimum_capacity);

    h->buckets = calloc(initial_capacity, sizeof(HashTableNode *));
    h->nodes = NULL;
    h->num_buckets = initial_capacity;
    h->num_nodes = 0;
    h->load_factor = load_factor;
}

static inline void hash_table_resize(HashTable *h, unsigned int minimum_capacity) {
    free(h->buckets);

    minimum_capacity = round_up_power_of_two(minimum_capacity);

    h->buckets = calloc(minimum_capacity, sizeof(HashTableNode *));
    h->num_buckets = minimum_capacity;

    // Re-insert all nodes.
    for (HashTableNode *node = h->nodes; node != NULL; node = node->nodes_next) {
        size_t idx = hash_string(node->key) & (h->num_buckets - 1);
        node->bucket_next = h->buckets[idx];
        h->buckets[idx] = node;
    }
}

void *hash_table_put(HashTable *h, char *key, void *value) {
    size_t hash = hash_string(key);
    size_t idx = hash & (h->num_buckets - 1);

    for (HashTableNode *node = h->buckets[idx]; node != NULL; node = node->bucket_next) {
        if (strcmp(node->key, key) == 0) {
            void *removed = node->value;
            node->value = value;
            return removed;
        }
    }

    unsigned int minimum_capacity = ceilf((h->num_nodes + 1) / h->load_factor);
    if (h->num_buckets < minimum_capacity) {
        hash_table_resize(h, minimum_capacity);

        // Recompute idx as the number of buckets have been increased.
        idx = hash & (h->num_buckets - 1);
    }

    HashTableNode *node = malloc(sizeof(HashTableNode));
    node->key = strdup(key);
    node->value = value;
    node->bucket_next = h->buckets[idx];
    node->nodes_next = h->nodes;

    h->buckets[idx] = node;
    h->nodes = node;
    h->num_nodes++;

    return NULL;
}

void *hash_table_get(HashTable *h, char *key) {
    size_t idx = hash_string(key) & (h->num_buckets - 1);

    for (HashTableNode *node = h->buckets[idx]; node != NULL; node = node->bucket_next) {
        if (strcmp(node->key, key) == 0) {
            return node->value;
        }
    }

    return NULL;
}

void hash_table_destroy(HashTable *h, void (*value_free)(void *)) {
    for (HashTableNode *node = h->nodes, *next; node != NULL; node = next) {
        free(node->key);
        if (value_free != NULL) {
            value_free(node->value);
        }
        next = node->nodes_next;
        free(node);
    }
    free(h->buckets);
}
