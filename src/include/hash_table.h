#ifndef HASH_TABLE_H
#define HASH_TABLE_H

typedef struct HashTableNode {
    char *key;
    void *value;
    struct HashTableNode *bucket_next;
    struct HashTableNode *nodes_next;
} HashTableNode;

typedef struct HashTable {
    HashTableNode **buckets;
    HashTableNode *nodes;
    unsigned int num_buckets;
    unsigned int num_nodes;
    float load_factor;
} HashTable;

void hash_table_init(HashTable *h, unsigned int initial_capacity, float load_factor);
void *hash_table_put(HashTable *h, char *key, void *value);
void *hash_table_get(HashTable *h, char *key);
void hash_table_clear(HashTable *h, void (*value_free)(void *));
void hash_table_destroy(HashTable *h, void (*value_free)(void *));

#endif /* HASH_TABLE_H */
