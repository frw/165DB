#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "btree.h"
#include "utils.h"

static inline BTreeNode *btree_leaf_node_create() {
    BTreeNode *node = malloc(sizeof(BTreeNode));
    node->leaf = true;
    node->fields.leaf.size = 0;
    node->fields.leaf.prev = NULL;
    node->fields.leaf.next = NULL;
    return node;
}

static inline BTreeNode *btree_internal_node_create() {
    BTreeNode *node = malloc(sizeof(BTreeNode));
    node->leaf = false;
    node->fields.internal.size = 0;
    return node;
}

static void btree_node_free(BTreeNode *node) {
    if (!node->leaf) {
        for (unsigned int i = 0; i < node->fields.internal.size; i++) {
            btree_node_free(node->fields.internal.children[i]);
        }
    }
    free(node);
}

static bool btree_node_save(BTreeNode *node, FILE *file) {
    if (fwrite(&node->leaf, sizeof(node->leaf), 1, file) != 1) {
        log_err("Unable to write B-Tree is_leaf\n");
        return false;
    }

    if (node->leaf) {
        BTreeLeafNode *leaf = &node->fields.leaf;

        if (fwrite(&leaf->size, sizeof(leaf->size), 1, file) != 1) {
            log_err("Unable to write B-Tree leaf node size\n");
            return false;
        }

        if (fwrite(leaf->values, sizeof(int), leaf->size, file) != leaf->size) {
            log_err("Unable to write B-Tree leaf node values\n");
            return false;
        }

        if (fwrite(leaf->positions, sizeof(unsigned int), leaf->size, file) != leaf->size) {
            log_err("Unable to write B-Tree leaf node positions\n");
            return false;
        }
    } else {
        BTreeInternalNode *internal = &node->fields.internal;

        if (fwrite(&internal->size, sizeof(internal->size), 1, file) != 1) {
            log_err("Unable to write B-Tree internal node size\n");
            return false;
        }

        if (fwrite(internal->values, sizeof(int), internal->size, file) != internal->size) {
            log_err("Unable to write B-Tree internal node values\n");
            return false;
        }

        for (unsigned int i = 0; i < internal->size; i++) {
            btree_node_save(internal->children[i], file);
        }
    }

    return true;
}

static BTreeNode *btree_node_load(BTreeLeafNode **head, BTreeLeafNode **tail, FILE *file) {
    BTreeNode *node = malloc(sizeof(BTreeNode));

    if (fread(&node->leaf, sizeof(node->leaf), 1, file) != 1) {
        log_err("Unable to write B-Tree is_leaf\n");
        free(node);
        return NULL;
    }

    if (node->leaf) {
        BTreeLeafNode *leaf = &node->fields.leaf;

        if (fread(&leaf->size, sizeof(leaf->size), 1, file) != 1) {
            log_err("Unable to read B-Tree leaf node size\n");
            free(node);
            return NULL;
        }

        if (fread(leaf->values, sizeof(int), leaf->size, file) != leaf->size) {
            log_err("Unable to read B-Tree leaf node values\n");
            free(node);
            return NULL;
        }

        if (fread(leaf->positions, sizeof(unsigned int), leaf->size, file) != leaf->size) {
            log_err("Unable to read B-Tree leaf node positions\n");
            free(node);
            return NULL;
        }

        leaf->prev = *tail;
        leaf->next = NULL;

        if (*head == NULL) {
            *head = leaf;
        } else {
            (*tail)->next = leaf;
        }
        *tail = leaf;
    } else {
        BTreeInternalNode *internal = &node->fields.internal;

        if (fread(&internal->size, sizeof(internal->size), 1, file) != 1) {
            log_err("Unable to read B-Tree internal node size\n");
            return false;
        }

        if (fread(internal->values, sizeof(int), internal->size, file) != internal->size) {
            log_err("Unable to read B-Tree internal node values\n");
            return false;
        }

        for (unsigned int i = 0; i < internal->size; i++) {
            BTreeNode *child = btree_node_load(head, tail, file);
            if (child == NULL) {
                for (unsigned int j = 0; j < i; j++) {
                    btree_node_free(internal->children[j]);
                }
                free(node);
                return NULL;
            }

            internal->children[i] = child;
        }
    }

    return node;
}

void btree_init(BTreeIndex *index, int *values, unsigned int *positions, unsigned int size) {
    if (size == 0) {
        index->root = NULL;
        index->head = NULL;
        index->tail = NULL;
        index->size = 0;
        return;
    }

    unsigned int num_nodes;
    unsigned int min_num_values;
    unsigned int remainder;
    unsigned int offset;
    unsigned int num_values;

    num_nodes = size / BTREE_LEAF_NODE_CAPACITY;
    if (size % BTREE_LEAF_NODE_CAPACITY > 0) {
        num_nodes++;
    }

    int *values_buf = malloc(num_nodes * sizeof(int));
    BTreeNode **children_buf = malloc(num_nodes * sizeof(BTreeNode *));

    min_num_values = size / num_nodes;
    remainder = size % num_nodes;

    offset = 0;

    unsigned int counter = 0;
    BTreeLeafNode *head = NULL;
    BTreeLeafNode *tail = NULL;
    for (unsigned int i = 0; i < num_nodes; i++) {
        num_values = min_num_values;
        if (i < remainder) {
            num_values++;
        }

        BTreeNode *node = btree_leaf_node_create();
        BTreeLeafNode *leaf = &node->fields.leaf;
        memcpy(leaf->values, values + offset, num_values * sizeof(int));
        if (positions == NULL) {
            for (unsigned int j = 0; j < num_values; j++) {
                leaf->positions[j] = counter++;
            }
        } else {
            memcpy(leaf->positions, positions + offset, num_values * sizeof(unsigned int));
        }
        leaf->prev = tail;
        leaf->size = num_values;

        if (i == 0) {
            head = leaf;
        } else {
            tail->next = leaf;
        }
        tail = leaf;

        values_buf[i] = values[offset];
        children_buf[i] = node;

        offset += num_values;
    }

    unsigned int num_children = num_nodes;
    while (num_children > 1) {
        num_nodes = num_children / BTREE_INTERNAL_NODE_CAPACITY;
        if (num_children % BTREE_INTERNAL_NODE_CAPACITY > 0) {
            num_nodes++;
        }

        min_num_values = num_children / num_nodes;
        remainder = num_children % num_nodes;

        offset = 0;

        for (unsigned int i = 0; i < num_nodes; i++) {
            num_values = min_num_values;
            if (i < remainder) {
                num_values++;
            }

            BTreeNode *node = btree_internal_node_create();
            BTreeInternalNode *internal = &node->fields.internal;
            memcpy(internal->values, values_buf + offset, num_values * sizeof(int));
            memcpy(internal->children, children_buf + offset, num_values * sizeof(BTreeNode *));
            internal->size = num_values;

            values_buf[i] = values_buf[offset];
            children_buf[i] = node;

            offset += num_values;
        }

        num_children = num_nodes;
    }

    index->root = children_buf[0];
    index->head = head;
    index->tail = tail;
    index->size = size;

    free(values_buf);
    free(children_buf);
}

void btree_destroy(BTreeIndex *index) {
    if (index->root != NULL) {
        btree_node_free(index->root);
    }
}

bool btree_save(BTreeIndex *index, FILE *file) {
    if (fwrite(&index->size, sizeof(index->size), 1, file) != 1) {
        log_err("Unable to write B-Tree size\n");
        return false;
    }

    if (index->size > 0 && !btree_node_save(index->root, file)) {
        return false;
    }

    return true;
}

bool btree_load(BTreeIndex *index, FILE *file) {
    if (fread(&index->size, sizeof(index->size), 1, file) != 1) {
        log_err("Unable to read B-Tree size\n");
        return false;
    }

    if (index->size == 0) {
        index->root = NULL;
        index->head = NULL;
        index->tail = NULL;
    } else {
        index->head = NULL;
        index->tail = NULL;

        BTreeNode *root = btree_node_load(&index->head, &index->tail, file);
        if (root == NULL) {
            return false;
        }

        index->root = root;
    }

    return true;
}

static inline void btree_values_insert(int *values, unsigned int size, unsigned int idx, int value) {
    if (idx < size) {
        memmove(values + idx + 1, values + idx, (size - idx) * sizeof(int));
    }
    values[idx] = value;
}

static inline void btree_positions_insert(unsigned int *positions, unsigned int size,
        unsigned int idx, unsigned int position) {
    if (idx < size) {
        memmove(positions + idx + 1, positions + idx, (size - idx) * sizeof(unsigned int));
    }
    positions[idx] = position;
}

static inline void btree_children_insert(BTreeNode **children, unsigned int size, unsigned int idx,
        BTreeNode *child) {
    if (idx < size) {
        memmove(children + idx + 1, children + idx, (size - idx) * sizeof(BTreeNode*));
    }
    children[idx] = child;
}

static inline int btree_node_first_value(BTreeNode *node) {
    if (node->leaf) {
        return node->fields.leaf.values[0];
    } else {
        return node->fields.internal.values[0];
    }
}

static inline BTreeNode *btree_leaf_node_split(BTreeIndex *index, BTreeLeafNode *leaf,
        unsigned int split) {
    BTreeNode *new = btree_leaf_node_create();
    BTreeLeafNode *new_leaf = &new->fields.leaf;

    unsigned int new_size = leaf->size - split;

    memcpy(new_leaf->values, leaf->values + split, new_size * sizeof(int));
    memcpy(new_leaf->positions, leaf->positions + split, new_size * sizeof(unsigned int));
    new_leaf->size = new_size;
    new_leaf->prev = leaf;
    new_leaf->next = leaf->next;

    if (leaf->next != NULL) {
        leaf->next->prev = new_leaf;
    } else {
        index->tail = new_leaf;
    }

    leaf->size = split;
    leaf->next = new_leaf;

    return new;
}

static inline BTreeNode *btree_internal_node_split(BTreeInternalNode *internal, unsigned int split) {
    BTreeNode *new = btree_internal_node_create();
    BTreeInternalNode *new_internal = &new->fields.internal;

    unsigned int new_size = internal->size - split;

    memcpy(new_internal->values, internal->values + split, new_size * sizeof(int));
    memcpy(new_internal->children, internal->children + split, new_size * sizeof(BTreeNode *));
    new_internal->size = new_size;

    internal->size = split;

    return new;
}

static BTreeNode *btree_node_insert(BTreeIndex *index, BTreeNode *root, int value,
        unsigned int position) {
    if (root->leaf) {
        BTreeLeafNode *leaf = &root->fields.leaf;
        BTreeNode *new_leaf = NULL;

        unsigned int idx = binary_search_right(leaf->values, leaf->size, value);

        if (leaf->size == BTREE_LEAF_NODE_CAPACITY) {
            unsigned int median = leaf->size / 2;

            new_leaf = btree_leaf_node_split(index, leaf, median);

            if (idx > median) {
                idx -= median;
                leaf = &new_leaf->fields.leaf;
            }
        }

        btree_values_insert(leaf->values, leaf->size, idx, value);
        btree_positions_insert(leaf->positions, leaf->size, idx, position);

        leaf->size++;

        return new_leaf;
    } else {
        BTreeInternalNode *internal = &root->fields.internal;
        BTreeNode *new_internal = NULL;

        unsigned int idx = binary_search_right(internal->values, internal->size, value);
        if (idx > 0) {
            idx--;
        }

        BTreeNode *child = internal->children[idx];
        BTreeNode *new_child = btree_node_insert(index, child, value, position);
        // Update slot value to the first value in child since it might have been updated.
        internal->values[idx] = btree_node_first_value(child);

        if (new_child != NULL) {
            unsigned int insert_idx = idx + 1;

            if (internal->size == BTREE_INTERNAL_NODE_CAPACITY) {
                unsigned int median = internal->size / 2;

                new_internal = btree_internal_node_split(internal, median);

                if (insert_idx >= median) {
                    insert_idx -= median;
                    internal = &new_internal->fields.internal;
                }
            }

            btree_values_insert(internal->values, internal->size, insert_idx,
                    btree_node_first_value(new_child));
            btree_children_insert(internal->children, internal->size, insert_idx, new_child);

            internal->size++;
        }

        return new_internal;
    }
}

void btree_insert(BTreeIndex *index, int value, unsigned int position) {
    BTreeNode *root = index->root;

    if (root == NULL) {
        root = index->root = btree_leaf_node_create();
        index->head = index->tail = &root->fields.leaf;
    }

    BTreeNode *new_node = btree_node_insert(index, root, value, position);
    if (new_node != NULL) {
        BTreeNode *new_root = btree_internal_node_create();

        new_root->fields.internal.values[0] = btree_node_first_value(root);
        new_root->fields.internal.children[0] = root;

        new_root->fields.internal.values[1] = btree_node_first_value(new_node);
        new_root->fields.internal.children[1] = new_node;

        new_root->fields.internal.size = 2;

        index->root = new_root;
    }

    index->size++;
}

static inline void btree_values_remove(int *values, unsigned int size, unsigned int idx) {
    if (idx < size - 1) {
        memmove(values + idx, values + idx + 1, (size - idx - 1) * sizeof(int));
    }
}

static inline void btree_positions_remove(unsigned int *positions, unsigned int size,
        unsigned int idx) {
    if (idx < size - 1) {
        memmove(positions + idx, positions + idx + 1, (size - idx - 1) * sizeof(unsigned int));
    }
}

static inline void btree_children_remove(BTreeNode **children, unsigned int size, unsigned int idx) {
    if (idx < size - 1) {
        memmove(children + idx, children + idx + 1, (size - idx - 1) * sizeof(BTreeNode*));
    }
}

static inline unsigned int btree_node_size(BTreeNode *node) {
    if (node->leaf) {
        return node->fields.leaf.size;
    } else {
        return node->fields.internal.size;
    }
}

static bool btree_node_remove(BTreeIndex *index, BTreeNode *root, int value, unsigned int position,
        unsigned int *positions_map, unsigned int *position_ptr) {
    if (root->leaf) {
        BTreeLeafNode *leaf = &root->fields.leaf;

        for (unsigned int idx = binary_search_left(leaf->values, leaf->size, value);
                idx < leaf->size && leaf->values[idx] == value; idx++) {
            unsigned int raw_pos = leaf->positions[idx];
            unsigned int pos = positions_map != NULL ? positions_map[raw_pos] : raw_pos;

            if (pos == position) {
                if (position_ptr != NULL) {
                    *position_ptr = raw_pos;
                }

                btree_values_remove(leaf->values, leaf->size, idx);
                btree_positions_remove(leaf->positions, leaf->size, idx);

                leaf->size--;

                if (leaf->size == 0) {
                    if (leaf->prev != NULL) {
                        leaf->prev->next = leaf->next;
                    } else {
                        index->head = leaf->next;
                    }

                    if (leaf->next != NULL) {
                        leaf->next->prev = leaf->prev;
                    } else {
                        index->tail = leaf->prev;
                    }
                }

                return true;
            }
        }

        return false;
    } else {
        BTreeInternalNode *internal = &root->fields.internal;

        unsigned int idx = binary_search_left(internal->values, internal->size, value);
        if (idx == internal->size) {
            idx--;
        } else if (internal->values[idx] > value) {
            if (idx > 0) {
                idx--;
            } else {
                return false;
            }
        } else if (idx > 0) {
            idx--;
        }

        for (; idx < internal->size && internal->values[idx] <= value; idx++) {
            BTreeNode *child = internal->children[idx];

            bool removed = btree_node_remove(index, child, value, position, positions_map,
                    position_ptr);
            if (removed) {
                if (btree_node_size(child) == 0) {
                    free(child);

                    btree_values_remove(internal->values, internal->size, idx);
                    btree_children_remove(internal->children, internal->size, idx);

                    internal->size--;
                } else {
                    internal->values[idx] = btree_node_first_value(child);
                }

                return true;
            }
        }

        return false;
    }
}

bool btree_remove(BTreeIndex *index, int value, unsigned int position, unsigned int *positions_map,
        unsigned int *position_ptr) {
    BTreeNode *root = index->root;

    if (root == NULL) {
        return false;
    }

    bool removed = btree_node_remove(index, root, value, position, positions_map, position_ptr);
    if (removed) {
        if (btree_node_size(root) == 0) {
            free(root);

            index->root = NULL;
        }

        index->size--;
    }

    return removed;
}

static BTreeLeafNode *btree_node_descend_left(BTreeNode *root, int value) {
    if (root->leaf) {
        return &root->fields.leaf;
    } else {
        BTreeInternalNode *internal = &root->fields.internal;

        unsigned int idx = binary_search_left(internal->values, internal->size, value);
        if (idx > 0) {
            if (idx == internal->size) {
                idx--;
            } else {
                BTreeLeafNode *leaf = btree_node_descend_left(internal->children[idx - 1], value);
                if (leaf->values[leaf->size - 1] >= value) {
                    return leaf;
                }
            }
        }

        return btree_node_descend_left(internal->children[idx], value);
    }
}

static inline int btree_leaf_node_search_left(BTreeLeafNode *leaf, int value) {
    unsigned int idx = binary_search_left(leaf->values, leaf->size, value);
    if (idx == leaf->size) {
        return -1;
    }

    return idx;
}

static BTreeLeafNode *btree_node_descend_right(BTreeNode *root, int value) {
    if (root->leaf) {
        return &root->fields.leaf;
    } else {
        BTreeInternalNode *internal = &root->fields.internal;

        unsigned int idx = binary_search_left(internal->values, internal->size, value);
        if (idx == 0) {
            return NULL;
        }

        return btree_node_descend_right(internal->children[idx - 1], value);
    }
}

static inline int btree_leaf_node_search_right(BTreeLeafNode *leaf, int value) {
    unsigned int idx = binary_search_left(leaf->values, leaf->size, value);
    if (idx == 0) {
        return -1;
    }

    return idx - 1;
}

bool btree_search(BTreeIndex *index, int value, unsigned int position, unsigned int *positions_map,
        unsigned int *position_ptr) {
    BTreeLeafNode *leaf = btree_node_descend_left(index->root, value);
    if (leaf == NULL) {
        return false;
    }

    int idx = btree_leaf_node_search_left(leaf, value);
    if (idx == -1) {
        return false;
    }

    unsigned int i = idx;
    for (BTreeLeafNode *node = leaf; node != NULL; node = node->next) {
        for (; i < node->size; i++) {
            if (node->values[i] != value) {
                return false;
            }

            unsigned int raw_pos = node->positions[i];
            unsigned int pos = positions_map != NULL ? positions_map[raw_pos] : raw_pos;

            if (pos == position) {
                if (position_ptr != NULL) {
                    *position_ptr = raw_pos;
                }

                return true;
            }
        }

        i = 0;
    }

    return false;
}

unsigned int btree_select_lower(BTreeIndex *index, int high, unsigned int *result) {
    if (index->size == 0) {
        return 0;
    }

    BTreeLeafNode *leaf = btree_node_descend_right(index->root, high);
    if (leaf == NULL) {
        return 0;
    }

    int idx = btree_leaf_node_search_right(leaf, high);
    if (idx == -1) {
        return 0;
    }

    unsigned int result_count = 0;

    for (BTreeLeafNode *node = index->head; node != leaf; node = node->next) {
        memcpy(result + result_count, node->positions, node->size * sizeof(unsigned int));
        result_count += node->size;
    }

    memcpy(result + result_count, leaf->positions, (idx + 1) * sizeof(unsigned int));
    result_count += idx + 1;

    return result_count;
}

unsigned int btree_select_higher(BTreeIndex *index, int low, unsigned int *result) {
    if (index->size == 0) {
        return 0;
    }

    BTreeLeafNode *leaf = btree_node_descend_left(index->root, low);
    if (leaf == NULL) {
        return 0;
    }

    int idx = btree_leaf_node_search_left(leaf, low);
    if (idx == -1) {
        return 0;
    }

    unsigned int result_count = 0;

    memcpy(result, leaf->positions + idx, (leaf->size - idx) * sizeof(unsigned int));
    result_count += leaf->size - idx;

    for (BTreeLeafNode *node = leaf->next; node != NULL; node = node->next) {
        memcpy(result + result_count, node->positions, node->size * sizeof(unsigned int));
        result_count += node->size;
    }

    return result_count;
}

unsigned int btree_select_range(BTreeIndex *index, int low, int high, unsigned int *result) {
    if (index->size == 0) {
        return 0;
    }

    BTreeLeafNode *left_leaf = btree_node_descend_left(index->root, low);
    if (left_leaf == NULL) {
        return 0;
    }

    int left_idx = btree_leaf_node_search_left(left_leaf, low);
    if (left_idx == -1) {
        return 0;
    }

    BTreeLeafNode *right_leaf = btree_node_descend_right(index->root, high);
    if (right_leaf == NULL) {
        return 0;
    }

    int right_idx = btree_leaf_node_search_right(right_leaf, high);
    if (right_idx == -1) {
        return 0;
    }

    if (left_leaf == right_leaf) {
        memcpy(result, left_leaf->positions + left_idx,
                (right_idx - left_idx + 1) * sizeof(unsigned int));
        return right_idx - left_idx + 1;
    } else {
        unsigned int result_count = 0;

        memcpy(result, left_leaf->positions + left_idx,
                (left_leaf->size - left_idx) * sizeof(unsigned int));
        result_count += left_leaf->size - left_idx;

        for (BTreeLeafNode *node = left_leaf->next; node != right_leaf; node = node->next) {
            memcpy(result + result_count, node->positions, node->size * sizeof(unsigned int));
            result_count += node->size;
        }

        memcpy(result + result_count, right_leaf->positions,
                (right_idx + 1) * sizeof(unsigned int));
        result_count += right_idx + 1;

        return result_count;
    }
}

int btree_min(BTreeIndex *index, unsigned int *position_ptr) {
    BTreeLeafNode *leaf = index->head;

    if (position_ptr != NULL) {
        *position_ptr = leaf->positions[0];
    }

    return leaf->values[0];
}

int btree_max(BTreeIndex *index, unsigned int *position_ptr) {
    BTreeLeafNode *leaf = index->tail;
    unsigned int idx = leaf->size - 1;

    if (position_ptr != NULL) {
        *position_ptr = leaf->positions[idx];
    }

    return leaf->values[idx];
}

