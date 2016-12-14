#ifndef BTREE_H
#define BTREE_H

#include <stdbool.h>
#include <stdio.h>

#define BTREE_INTERNAL_NODE_CAPACITY 512
#define BTREE_LEAF_NODE_CAPACITY 512

typedef struct BTreeNode BTreeNode;
typedef union BTreeNodeFields BTreeNodeFields;
typedef struct BTreeInternalNode BTreeInternalNode;
typedef struct BTreeLeafNode BTreeLeafNode;

struct BTreeInternalNode {
    int values[BTREE_INTERNAL_NODE_CAPACITY];
    BTreeNode *children[BTREE_INTERNAL_NODE_CAPACITY];
    unsigned int size;
};

struct BTreeLeafNode {
    int values[BTREE_LEAF_NODE_CAPACITY];
    unsigned int positions[BTREE_LEAF_NODE_CAPACITY];
    unsigned int size;
    BTreeLeafNode *prev;
    BTreeLeafNode *next;
};

union BTreeNodeFields {
    BTreeInternalNode internal;
    BTreeLeafNode leaf;
};

struct BTreeNode {
    bool leaf;
    BTreeNodeFields fields;
};

typedef struct BTreeIndex {
    BTreeNode *root;
    BTreeLeafNode *head;
    BTreeLeafNode *tail;
    unsigned int size;
} BTreeIndex;

void btree_init(BTreeIndex *index, int *values, unsigned int *positions, unsigned int size);
void btree_destroy(BTreeIndex *index);

bool btree_save(BTreeIndex *index, FILE *file);
bool btree_load(BTreeIndex *index, FILE *file);

void btree_insert(BTreeIndex *index, int value, unsigned int position);
bool btree_remove(BTreeIndex *index, int value, unsigned int position, unsigned int *positions_map,
        unsigned int *position_ptr);

bool btree_search(BTreeIndex *index, int value, unsigned int position, unsigned int *positions_map,
        unsigned int *position_ptr);

unsigned int btree_select_lower(BTreeIndex *index, int high, unsigned int *result);
unsigned int btree_select_higher(BTreeIndex *index, int low, unsigned int *result);
unsigned int btree_select_range(BTreeIndex *index, int low, int high, unsigned int *result);

int btree_min(BTreeIndex *index, unsigned int *position_ptr);
int btree_max(BTreeIndex *index, unsigned int *position_ptr);

#endif /* BTREE_H */
