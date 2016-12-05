#include <assert.h>
#include <time.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "vector.h"
#include "utils.h"

#define VALUES_COUNT 100000

void generate_random(unsigned int seed, int *values, size_t count) {
    srand(seed);

    for (size_t i = 0; i < count; i++) {
        values[i] = rand() % 50;
    }
}

void generate_ascending(int offset, int *values, size_t count) {
    for (size_t i = 0; i < count; i++) {
        values[i] = offset + i;
    }
}

static inline void probe(unsigned int *table, unsigned int *counts, unsigned int *offsets,
        int *values2, unsigned int *positions2, unsigned int count2, PosVector *pos_out1,
        PosVector *pos_out2) {
    for (unsigned int i = 0; i < count2; i++) {
        unsigned int bucket = values2[i] & 0xFF;
        unsigned int count = counts[bucket];

        if (count > 0) {
            unsigned int pos2 = positions2[i];

            unsigned int start = offsets[bucket];
            unsigned int end = start + count;
            for (unsigned int j = start; j < end; j++) {
                pos_vector_append(pos_out1, table[j]);
                pos_vector_append(pos_out2, pos2);
            }
        }
    }
}

static inline void build(int *values1, unsigned int *positions1, unsigned int *table,
        unsigned int count1, int *values2, unsigned int *positions2, unsigned int count2,
        PosVector *pos_out1, PosVector *pos_out2) {
    unsigned int counts[0x100] = { 0 };
    for (unsigned int i = 0; i < count1; i++) {
        counts[values1[i] & 0xFF]++;
    }

    unsigned int offsets[0x100];
    unsigned int ends[0x100];
    unsigned int accum = 0;
    for (unsigned int i = 0; i < 0x100; i++) {
        offsets[i] = ends[i] = accum;
        accum += counts[i];
    }

    for (unsigned int i = 0; i < count1; i++) {
        table[ends[values1[i] & 0xFF]++] = positions1[i];
    }

    probe(table, counts, offsets, values2, positions2, count2, pos_out1, pos_out2);
}

static void partition(int *values1, int *values1_buf, unsigned int *positions1,
        unsigned int *positions1_buf, unsigned int count1, int *values2, int *values2_buf,
        unsigned int *positions2, unsigned int *positions2_buf, unsigned int count2,
        PosVector *pos_out1, PosVector *pos_out2, unsigned int shift, bool alloc_buf) {
    if (shift == 0) {
        if (count1 <= count2) {
            build(values1, positions1, positions1_buf, count1, values2, positions2, count2,
                    pos_out1, pos_out2);
        } else {
            build(values2, positions2, positions2_buf, count2, values1, positions1, count1,
                    pos_out2, pos_out1);
        }
        return;
    }

    // Partition first side.

    unsigned int c1[0x100] = { 0 };
    for (unsigned int i = 0; i < count1; i++) {
        c1[(values1[i] >> shift) & 0xFF]++;
    }

    unsigned int o1[0x100];
    unsigned int b1[0x100];
    unsigned int accum1 = 0;
    for (unsigned int i = 0; i < 0x100; i++) {
        o1[i] = b1[i] = accum1;
        accum1 += c1[i];
    }

    for (unsigned int i = 0; i < count1; i++) {
        int value = values1[i];
        unsigned int index = b1[(value >> shift) & 0xFF]++;
        values1_buf[index] = value;
        positions1_buf[index] = positions1[i];
    }

    // Partition second side.

    unsigned int c2[0x100] = { 0 };
    for (unsigned int i = 0; i < count2; i++) {
        c2[(values2[i] >> shift) & 0xFF]++;
    }

    unsigned int o2[0x100];
    unsigned int b2[0x100];
    unsigned int accum2 = 0;
    for (unsigned int i = 0; i < 0x100; i++) {
        o2[i] = b2[i] = accum2;
        accum2 += c2[i];
    }

    for (unsigned int i = 0; i < count2; i++) {
        int value = values2[i];
        unsigned int index = b2[(value >> shift) & 0xFF]++;
        values2_buf[index] = value;
        positions2_buf[index] = positions2[i];
    }

    // Allocate buffers if needed.
    if (alloc_buf) {
        values1 = malloc(count1 * sizeof(int));
        positions1 = malloc(count1 * sizeof(unsigned int));

        values2 = malloc(count2 * sizeof(int));
        positions2 = malloc(count2 * sizeof(unsigned int));
    }

    // Go through partitions.

    for (unsigned int i = 0; i < 0x100; i++) {
        unsigned int count1 = c1[i];
        unsigned int count2 = c2[i];

        if (count1 > 0 && count2 > 0) {
            unsigned int offset1 = o1[i];
            unsigned int offset2 = o2[i];

            partition(values1_buf + offset1, values1 + offset1, positions1_buf + offset1,
                    positions1 + offset1, count1, values2_buf + offset2, values2 + offset2,
                    positions2_buf + offset2, positions2 + offset2, count2, pos_out1, pos_out2,
                    shift - 8, false);
        }
    }

    if (alloc_buf) {
        free(values1);
        free(positions1);

        free(values2);
        free(positions2);
    }
}

void join_hash(int *values1, unsigned int *positions1, unsigned int count1, int *values2,
        unsigned int *positions2, unsigned int count2, PosVector *pos_out1, PosVector *pos_out2) {
    unsigned int *values1_buf = malloc(count1 * sizeof(int));
    unsigned int *positions1_buf = malloc(count1 * sizeof(unsigned int));

    unsigned int *values2_buf = malloc(count2 * sizeof(int));
    unsigned int *positions2_buf = malloc(count2 * sizeof(unsigned int));

    partition(values1, values1_buf, positions1, positions1_buf, count1, values2, values2_buf,
            positions2, positions2_buf, count2, pos_out1, pos_out2, (sizeof(int) - 1) * 8, true);

    free(values1_buf);
    free(positions1_buf);

    free(values2_buf);
    free(positions2_buf);
}

#define NODE_CAPACITY 8

typedef struct HashJoinNode {
    int values[NODE_CAPACITY];
    unsigned int positions[NODE_CAPACITY];
    unsigned int size;
    struct HashJoinNode *next;
    struct HashJoinNode *node_next;
} HashJoinNode;

static inline int hash(int key) {
    return key;
}

void join_hash2(int *values1, unsigned int *positions1, unsigned int count1, int *values2,
        unsigned int *positions2, unsigned int count2, PosVector *pos_out1, PosVector *pos_out2) {
    unsigned int table_size = round_up_power_of_two(count1);
    unsigned int mask = table_size - 1;

    HashJoinNode **table = calloc(table_size, sizeof(HashJoinNode *));
    HashJoinNode *head = NULL;

    for (unsigned int i = 0; i < count1; i++) {
        int value = values1[i];

        unsigned idx = hash(value) & mask;

        HashJoinNode *n = table[idx];

        if (n == NULL || n->size == NODE_CAPACITY) {
            HashJoinNode *new = malloc(sizeof(HashJoinNode));
            new->next = n;
            new->node_next = head;

            table[idx] = new;
            head = new;

            n = new;
        }

        n->values[n->size] = value;
        n->positions[n->size] = positions1[i];
        n->size++;
    }

    for (unsigned int i = 0; i < count2; i++) {
        int value = values2[i];

        unsigned idx = hash(value) & mask;

        for (HashJoinNode *n = table[idx]; n != NULL; n = n->next) {
            for (unsigned int j = 0; j < n->size; j++) {
                if (n->values[j] == value) {
                    pos_vector_append(pos_out1, n->positions[j]);
                    pos_vector_append(pos_out2, positions2[i]);
                }
            }
        }
    }

    for (HashJoinNode *n = head; n != NULL;) {
        HashJoinNode *node = n;
        n = n->node_next;
        free(node);
    }
    free(table);
}

void join_hash3(int *values1, unsigned int *positions1, unsigned int count1, int *values2,
        unsigned int *positions2, unsigned int count2, PosVector *pos_out1, PosVector *pos_out2) {
    unsigned int table_size = round_up_power_of_two(count1);
    unsigned int mask = table_size - 1;

    unsigned int *c1 = calloc(table_size, sizeof(unsigned int));
    for (unsigned int i = 0; i < count1; i++) {
        c1[hash(values1[i]) & mask]++;
    }

    unsigned int *o1 = malloc(table_size * sizeof(unsigned int));
    unsigned int *b1 = malloc(table_size * sizeof(unsigned int));
    unsigned int accum1 = 0;
    for (unsigned int i = 0; i < table_size; i++) {
        o1[i] = b1[i] = accum1;
        accum1 += c1[i];
    }

    int *table_values = malloc(count1 * sizeof(unsigned int));
    int *table_positions = malloc(count1 * sizeof(unsigned int));

    for (unsigned int i = 0; i < count1; i++) {
        int value = values1[i];
        unsigned int index = b1[hash(value) & mask]++;
        table_values[index] = value;
        table_positions[index] = positions1[i];
    }

    for (unsigned int i = 0; i < count2; i++) {
        int val2 = values2[i];
        unsigned int bucket = hash(val2) & mask;
        unsigned int count = c1[bucket];

        if (count > 0) {
            unsigned int pos2 = positions2[i];

            unsigned int start = o1[bucket];
            unsigned int end = start + count;
            for (unsigned int j = start; j < end; j++) {
                unsigned int val1 = table_values[j];

                if (val1 == val2) {
                    pos_vector_append(pos_out1, table_positions[j]);
                    pos_vector_append(pos_out2, pos2);
                }
            }
        }
    }

    free(c1);
    free(o1);
    free(b1);

    free(table_values);
    free(table_positions);
}

#define HASH_BLOCK_SIZE 32768

static inline void build_probe(int *values1, int *table_values, unsigned int *positions1,
        unsigned int *table_positions, unsigned int count1, int *values2, unsigned int *positions2,
        unsigned int count2, PosVector *pos_out1, PosVector *pos_out2, unsigned int shift) {
    if (shift == 0) {
        unsigned int counts[0x100] = { 0 };
        for (unsigned int i = 0; i < count1; i++) {
            counts[values1[i] & 0xFF]++;
        }

        unsigned int offsets[0x100];
        unsigned int ends[0x100];
        unsigned int accum = 0;
        for (unsigned int i = 0; i < 0x100; i++) {
            offsets[i] = ends[i] = accum;
            accum += counts[i];
        }

        for (unsigned int i = 0; i < count1; i++) {
            table_positions[ends[values1[i] & 0xFF]++] = positions1[i];
        }

        for (unsigned int i = 0; i < count2; i++) {
            unsigned int bucket = values2[i] & 0xFF;
            unsigned int count = counts[bucket];

            if (count > 0) {
                unsigned int pos2 = positions2[i];

                unsigned int start = offsets[bucket];
                unsigned int end = start + count;
                for (unsigned int j = start; j < end; j++) {
                    unsigned int pos1 = table_positions[j];

                    pos_vector_append(pos_out1, pos1);
                    pos_vector_append(pos_out2, pos2);
                }
            }
        }
    } else {
        unsigned int table_size = round_up_power_of_two(count1);
        unsigned int mask = table_size - 1;

        unsigned int *counts = calloc(table_size, sizeof(unsigned int));
        for (unsigned int i = 0; i < count1; i++) {
            counts[values1[i] & mask]++;
        }

        unsigned int *offsets = malloc(table_size * sizeof(unsigned int));
        unsigned int *ends = malloc(table_size * sizeof(unsigned int));
        unsigned int accum = 0;
        for (unsigned int i = 0; i < table_size; i++) {
            offsets[i] = ends[i] = accum;
            accum += counts[i];
        }

        for (unsigned int i = 0; i < count1; i++) {
            int value = values1[i];
            unsigned int index = ends[value & mask]++;
            table_values[index] = value;
            table_positions[index] = positions1[i];
        }

        for (unsigned int i = 0; i < count2; i++) {
            int val2 = values2[i];
            unsigned int bucket = val2 & mask;
            unsigned int count = counts[bucket];

            if (count > 0) {
                unsigned int pos2 = positions2[i];

                unsigned int start = offsets[bucket];
                unsigned int end = start + count;
                for (unsigned int j = start; j < end; j++) {
                    unsigned int val1 = table_values[j];

                    if (val1 == val2) {
                        unsigned int pos1 = table_positions[j];

                        pos_vector_append(pos_out1, pos1);
                        pos_vector_append(pos_out2, pos2);
                    }
                }
            }
        }

        free(counts);
        free(offsets);
        free(ends);
    }
}

static void partition4(int *values1, int *values1_buf, unsigned int *positions1,
        unsigned int *positions1_buf, unsigned int count1, int *values2, int *values2_buf,
        unsigned int *positions2, unsigned int *positions2_buf, unsigned int count2,
        PosVector *pos_out1, PosVector *pos_out2, unsigned int shift, bool alloc_buf) {
    if (count1 < HASH_BLOCK_SIZE) {
        build_probe(values1, values1_buf, positions1, positions1_buf, count1, values2, positions2,
                count2, pos_out1, pos_out2, shift);
        return;
    } else if (count2 < HASH_BLOCK_SIZE) {
        build_probe(values2, values2_buf, positions2, positions2_buf, count2, values1, positions1,
                count1, pos_out2, pos_out1, shift);
        return;
    } else if (shift == 0) {
        if (count1 <= count2) {
            build_probe(values1, values1_buf, positions1, positions1_buf, count1, values2,
                    positions2, count2, pos_out1, pos_out2, shift);
        } else {
            build_probe(values2, values2_buf, positions2, positions2_buf, count2, values1,
                    positions1, count1, pos_out2, pos_out1, shift);
        }
        return;
    }

    // Partition first side.

    unsigned int c1[0x100] = { 0 };
    for (unsigned int i = 0; i < count1; i++) {
        c1[(values1[i] >> shift) & 0xFF]++;
    }

    unsigned int o1[0x100];
    unsigned int b1[0x100];
    unsigned int accum1 = 0;
    for (unsigned int i = 0; i < 0x100; i++) {
        o1[i] = b1[i] = accum1;
        accum1 += c1[i];
    }

    for (unsigned int i = 0; i < count1; i++) {
        int value = values1[i];
        unsigned int index = b1[(value >> shift) & 0xFF]++;
        values1_buf[index] = value;
        positions1_buf[index] = positions1[i];
    }

    // Partition second side.

    unsigned int c2[0x100] = { 0 };
    for (unsigned int i = 0; i < count2; i++) {
        c2[(values2[i] >> shift) & 0xFF]++;
    }

    unsigned int o2[0x100];
    unsigned int b2[0x100];
    unsigned int accum2 = 0;
    for (unsigned int i = 0; i < 0x100; i++) {
        o2[i] = b2[i] = accum2;
        accum2 += c2[i];
    }

    for (unsigned int i = 0; i < count2; i++) {
        int value = values2[i];
        unsigned int index = b2[(value >> shift) & 0xFF]++;
        values2_buf[index] = value;
        positions2_buf[index] = positions2[i];
    }

    // Allocate buffers if needed.
    if (alloc_buf) {
        values1 = malloc(count1 * sizeof(int));
        positions1 = malloc(count1 * sizeof(unsigned int));

        values2 = malloc(count2 * sizeof(int));
        positions2 = malloc(count2 * sizeof(unsigned int));
    }

    // Go through partitions.

    for (unsigned int i = 0; i < 0x100; i++) {
        unsigned int count1 = c1[i];
        unsigned int count2 = c2[i];

        if (count1 > 0 && count2 > 0) {
            unsigned int offset1 = o1[i];
            unsigned int offset2 = o2[i];

            partition4(values1_buf + offset1, values1 + offset1, positions1_buf + offset1,
                    positions1 + offset1, count1, values2_buf + offset2, values2 + offset2,
                    positions2_buf + offset2, positions2 + offset2, count2, pos_out1, pos_out2,
                    shift - 8, false);
        }
    }

    if (alloc_buf) {
        free(values1);
        free(positions1);

        free(values2);
        free(positions2);
    }
}

void join_hash4(int *values1, unsigned int *positions1, unsigned int count1, int *values2,
        unsigned int *positions2, unsigned int count2, PosVector *pos_out1, PosVector *pos_out2) {
    unsigned int *values1_buf = malloc(count1 * sizeof(int));
    unsigned int *positions1_buf = malloc(count1 * sizeof(unsigned int));

    unsigned int *values2_buf = malloc(count2 * sizeof(int));
    unsigned int *positions2_buf = malloc(count2 * sizeof(unsigned int));

    partition(values1, values1_buf, positions1, positions1_buf, count1, values2, values2_buf,
            positions2, positions2_buf, count2, pos_out1, pos_out2, (sizeof(int) - 1) * 8, true);

    free(values1_buf);
    free(positions1_buf);

    free(values2_buf);
    free(positions2_buf);
}

#define NESTED_BLOCK_SIZE 32768

void join_nested_loop(int *values1, unsigned int *positions1, unsigned int count1, int *values2,
        unsigned int *positions2, unsigned int count2, PosVector *pos_out1, PosVector *pos_out2) {
    for (unsigned int i = 0; i < count1; i += NESTED_BLOCK_SIZE) {

        for (unsigned int j = 0; j < count2; j += NESTED_BLOCK_SIZE) {

            unsigned int r_end = i + NESTED_BLOCK_SIZE;
            r_end = count1 < r_end ? count1 : r_end;
            for (unsigned int r = i; r < r_end; r++) {
                unsigned int val1 = values1[r];

                unsigned int s_end = j + NESTED_BLOCK_SIZE;
                s_end = count2 < s_end ? count2 : s_end;
                for (unsigned int s = j; s < s_end; s++) {
                    unsigned int val2 = values2[s];

                    if (val1 == val2) {
                        pos_vector_append(pos_out1, positions1[r]);
                        pos_vector_append(pos_out2, positions2[s]);
                    }

                }

            }

        }

    }
}

void join_sort_merge(int *values1, unsigned int *positions1, unsigned int count1, int *values2,
        unsigned int *positions2, unsigned int count2, PosVector *pos_out1, PosVector *pos_out2) {
    int *sorted_values1 = malloc(count1 * sizeof(int));
    unsigned int *sorted_positions1 = malloc(count1 * sizeof(unsigned int));
    radix_sort_indices(values1, positions1, sorted_values1, sorted_positions1, count1);

    int *sorted_values2 = malloc(count2 * sizeof(int));
    unsigned int *sorted_positions2 = malloc(count2 * sizeof(unsigned int));
    radix_sort_indices(values2, positions2, sorted_values2, sorted_positions2, count2);

    unsigned int i = 0;
    unsigned int j = 0;
    while (i < count1 && j < count2) {
        int val1 = sorted_values1[i];
        int val2 = sorted_values2[j];

        if (val1 < val2) {
            i++;
        } else if (val1 > val2) {
            j++;
        } else {
            unsigned int i_start = i;
            unsigned int j_start = j;

            while (++i < count1 && sorted_values1[i] == val1)
                ;
            while (++j < count2 && sorted_values2[j] == val2)
                ;

            for (unsigned int r = i_start; r < i; r++) {
                unsigned int pos1 = sorted_positions1[r];

                for (unsigned int s = j_start; s < j; s++) {
                    unsigned int pos2 = sorted_positions2[s];

                    pos_vector_append(pos_out1, pos1);
                    pos_vector_append(pos_out2, pos2);
                }
            }
        }
    }

    free(sorted_values1);
    free(sorted_positions1);

    free(sorted_values2);
    free(sorted_positions2);
}


int main(int argc, char *argv[]) {
    int *values1 = malloc(VALUES_COUNT * sizeof(int));
    unsigned int *positions1 = malloc(VALUES_COUNT * sizeof(int));

    int *values2 = malloc(VALUES_COUNT * sizeof(int));
    unsigned int *positions2 = malloc(VALUES_COUNT * sizeof(int));

    generate_random(42, values1, VALUES_COUNT);
    generate_random(24, values2, VALUES_COUNT);
    // generate_ascending(0, values1, VALUES_COUNT);
    // generate_ascending(10, values2, VALUES_COUNT);
    generate_ascending(0, positions1, VALUES_COUNT);
    generate_ascending(0, positions2, VALUES_COUNT);

    PosVector pos_out1;
    PosVector pos_out2;

    clock_t start, end;


     pos_vector_init(&pos_out1, VALUES_COUNT);
     pos_vector_init(&pos_out2, VALUES_COUNT);
     start = clock();
     join_hash(values1, positions1, VALUES_COUNT, values2, positions2, VALUES_COUNT, &pos_out1,
     &pos_out2);
     end = clock();
     for (unsigned int i = 0; i < pos_out1.size; i++) {
     assert(values1[pos_out1.data[i]] == values2[pos_out2.data[i]]);
     }
     printf("Hash Join 1: %f\n", (double) (end - start) / CLOCKS_PER_SEC);
     printf("Result Size: %u\n", pos_out1.size);
     pos_vector_destroy(&pos_out1);
     pos_vector_destroy(&pos_out2);



     /*
     pos_vector_init(&pos_out1, VALUES_COUNT);
     pos_vector_init(&pos_out2, VALUES_COUNT);
     start = clock();
     join_hash2(values1, positions1, VALUES_COUNT, values2, positions2, VALUES_COUNT, &pos_out1,
     &pos_out2);
     end = clock();
     for (unsigned int i = 0; i < pos_out1.size; i++) {
     assert(values1[pos_out1.data[i]] == values2[pos_out2.data[i]]);
     }
     printf("Hash Join 2: %f\n", (double) (end - start) / CLOCKS_PER_SEC);
     printf("Result Size: %u\n", pos_out1.size);
     pos_vector_destroy(&pos_out1);
     pos_vector_destroy(&pos_out2);
    */


    pos_vector_init(&pos_out1, VALUES_COUNT);
    pos_vector_init(&pos_out2, VALUES_COUNT);
    start = clock();
    join_hash3(values1, positions1, VALUES_COUNT, values2, positions2, VALUES_COUNT, &pos_out1,
            &pos_out2);
    end = clock();
    for (unsigned int i = 0; i < pos_out1.size; i++) {
        assert(values1[pos_out1.data[i]] == values2[pos_out2.data[i]]);
    }
    printf("Hash Join 3: %f\n", (double) (end - start) / CLOCKS_PER_SEC);
    printf("Result Size: %u\n", pos_out1.size);
    pos_vector_destroy(&pos_out1);
    pos_vector_destroy(&pos_out2);


    /*
    pos_vector_init(&pos_out1, VALUES_COUNT);
    pos_vector_init(&pos_out2, VALUES_COUNT);
    start = clock();
    join_hash4(values1, positions1, VALUES_COUNT, values2, positions2, VALUES_COUNT, &pos_out1,
            &pos_out2);
    end = clock();
    for (unsigned int i = 0; i < pos_out1.size; i++) {
        assert(values1[pos_out1.data[i]] == values2[pos_out2.data[i]]);
    }
    printf("Hash Join 4: %f\n", (double) (end - start) / CLOCKS_PER_SEC);
    printf("Result Size: %u\n", pos_out1.size);
    pos_vector_destroy(&pos_out1);
    pos_vector_destroy(&pos_out2);
    */


    /*
    pos_vector_init(&pos_out1, VALUES_COUNT);
    pos_vector_init(&pos_out2, VALUES_COUNT);
    start = clock();
    join_nested_loop(values1, positions1, VALUES_COUNT, values2, positions2, VALUES_COUNT, &pos_out1,
            &pos_out2);
    end = clock();
    for (unsigned int i = 0; i < pos_out1.size; i++) {
        assert(values1[pos_out1.data[i]] == values2[pos_out2.data[i]]);
    }
    printf("Nested Loop Join: %f\n", (double) (end - start) / CLOCKS_PER_SEC);
    printf("Result Size: %u\n", pos_out1.size);
    pos_vector_destroy(&pos_out1);
    pos_vector_destroy(&pos_out2);
    */

    pos_vector_init(&pos_out1, VALUES_COUNT);
    pos_vector_init(&pos_out2, VALUES_COUNT);
    start = clock();
    join_sort_merge(values1, positions1, VALUES_COUNT, values2, positions2, VALUES_COUNT, &pos_out1,
            &pos_out2);
    end = clock();
    for (unsigned int i = 0; i < pos_out1.size; i++) {
        assert(values1[pos_out1.data[i]] == values2[pos_out2.data[i]]);
    }
    printf("Sort Merge Join: %f\n", (double) (end - start) / CLOCKS_PER_SEC);
    printf("Result Size: %u\n", pos_out1.size);
    pos_vector_destroy(&pos_out1);
    pos_vector_destroy(&pos_out2);


    free(values1);
    free(positions1);
    free(values2);
    free(positions2);
}

