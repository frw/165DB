#include <stdio.h>
#include <stdlib.h>

#include "../btree.c"

#define NUM_VALUES 10000

int main(int argc, char *argv[]) {
    int *values = malloc(NUM_VALUES * sizeof(int));
    for (int i = 0; i < NUM_VALUES; i++) {
        values[i] = i - NUM_VALUES / 2;
    }

    BTreeIndex index;
    btree_init(&index, true, NULL, NULL, 0);

    unsigned int pos;

    /*
    for (int i = 0; i < 50; i++) {
        int val = (i - 25) * 2 - 1;
        btree_insert(&index, val, &pos);
        printf("Insert: %d, %u\n", val, pos);
    }

    for (int i = 0; i < 50; i++) {
        int val = (i - 25) * 2;
        btree_insert(&index, val, &pos);
        printf("Insert: %d, %u\n", val, pos);
    }
    */

    for (int i = 1; i <= 11; i += 2) {
        for (int j = 0; j < 7; j++) {
            btree_insert(&index, i, &pos);
            printf("Insert: %d, %u\n", i, pos);
        }
    }

    unsigned int count = 0;
    for (BTreeLeafNode *n = index.head; n != NULL; n = n->next) {
        printf("New Node\n");
        count += n->size;
        for (size_t i = 0; i < n->size; i++) {
            printf("%d, %u\n", n->values[i], n->positions[i]);
        }
    }
    printf("Count: %u\n", count);


    /*
    for (int i = -53; i <= 50; i++) {
        printf("%d, %d\n", i, btree_search_left(&index, i));
    }
    */

    int *results = malloc(NUM_VALUES * sizeof(int));
    unsigned int results_count;

    results_count = btree_select_range(&index, 3, 6, results);
    printf("%u\n", results_count);
    for (unsigned int i = 0; i < results_count; i++) {
        if (i > 0) {
            printf(", ");
        }
        printf("%d", results[i]);
    }
    printf("\n");

    printf("%d\n", btree_min(&index, NULL));

    printf("%d\n", btree_max(&index, NULL));

    btree_destroy(&index);

    free(values);
}
