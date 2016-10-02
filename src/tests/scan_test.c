#include <time.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>

#define VALUES_COUNT 200000000

void generate(int *values, int *positions, size_t count) {
    srand(42);

    for (size_t i = 0; i < count; i++) {
        values[i] = rand();
        positions[i] = rand();
    }
}

inline unsigned int select_low1(int *values, unsigned int values_count, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = i;
        result_count += values[i] < high;
    }
    return result_count;
}

inline unsigned int select_high1(int *values, unsigned int values_count, int low,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = i;
        result_count += values[i] >= low;
    }
    return result_count;
}

inline unsigned int select_equal1(int *values, unsigned int values_count, int value,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = i;
        result_count += values[i] == value;
    }
    return result_count;
}

inline unsigned int select_range1(int *values, unsigned int values_count, int low, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = i;
        int value = values[i];
        result_count += (value >= low) & (value < high);
    }
    return result_count;
}

inline unsigned int select_low2(int *values, unsigned int values_count, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        if (values[i] < high) {
            result[result_count++] = i;
        }
    }
    return result_count;
}

inline unsigned int select_high2(int *values, unsigned int values_count, int low,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        if (values[i] >= low) {
            result[result_count++] = i;
        }
    }
    return result_count;
}

inline unsigned int select_equal2(int *values, unsigned int values_count, int value,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        if (values[i] == value) {
            result[result_count++] = i;
        }
    }
    return result_count;
}

inline unsigned int select_range2(int *values, unsigned int values_count, int low, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        int value = values[i];
        if (value >= low && value < high) {
            result[result_count++] = i;
        }
    }
    return result_count;
}

inline unsigned int select_range3(int *values, unsigned int values_count, int low, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = i;
        int value = values[i];
        result_count += (value >= low) && (value < high);
    }
    return result_count;
}

inline unsigned int select_pos_low1(unsigned int *positions, int *values, unsigned int values_count, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = positions[i];
        result_count += values[i] < high;
    }
    return result_count;
}

inline unsigned int select_pos_high1(unsigned int *positions, int *values, unsigned int values_count, int low,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = positions[i];
        result_count += values[i] >= low;
    }
    return result_count;
}

inline unsigned int select_pos_equal1(unsigned int *positions, int *values, unsigned int values_count, int value,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = positions[i];
        result_count += values[i] == value;
    }
    return result_count;
}

inline unsigned int select_pos_range1(unsigned int *positions, int *values, unsigned int values_count, int low, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = positions[i];
        int value = values[i];
        result_count += (value >= low) & (value < high);
    }
    return result_count;
}

inline unsigned int select_pos_low2(unsigned int *positions, int *values, unsigned int values_count, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        if (values[i] < high) {
            result[result_count++] = positions[i];
        }
    }
    return result_count;
}

inline unsigned int select_pos_high2(unsigned int *positions, int *values, unsigned int values_count, int low,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        if (values[i] >= low) {
            result[result_count++] = positions[i];
        }
    }
    return result_count;
}

inline unsigned int select_pos_equal2(unsigned int *positions, int *values, unsigned int values_count, int value,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        if (values[i] == value) {
            result[result_count++] = positions[i];
        }
    }
    return result_count;
}

inline unsigned int select_pos_range2(unsigned int *positions, int *values, unsigned int values_count, int low, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        int value = values[i];
        if (value >= low && value < high) {
            result[result_count++] = positions[i];
        }
    }
    return result_count;
}

inline unsigned int select_pos_range3(unsigned int *positions, int *values, unsigned int values_count, int low, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = positions[i];
        int value = values[i];
        result_count += (value >= low) && (value < high);
    }
    return result_count;
}

inline int max1(int *values, unsigned int values_count) {
    int max_value = values[0];
    for (unsigned int i = 1; i < values_count; i++) {
        int value = values[i];
        max_value = value > max_value ? value : max_value;
    }
    return max_value;
}

inline int max2(int *values, unsigned int values_count) {
    int max_value = values[0];
    for (unsigned int i = 1; i < values_count; i++) {
        int value = values[i];
        if (value > max_value) {
            max_value = value;
        }
    }
    return max_value;
}

inline int max3(int *values, unsigned int values_count) {
    int max_value = values[0];
    for (unsigned int i = 1; i < values_count; i++) {
        int value = values[i];
        max_value = value ^ ((value ^ max_value) & -(value < max_value));
    }
    return max_value;
}

inline int max4(int *values, unsigned int values_count) {
    int max_value = values[0];
    for (unsigned int i = 1; i < values_count; i++) {
        int value = values[i];
        int diff = value - max_value;
        max_value = value - (diff & (diff >> (sizeof(int) * 8 - 1)));
    }
    return max_value;
}

inline unsigned int max_pos1(int *values, unsigned int values_count) {
    unsigned int max_position = 0;
    int max_value = values[0];
    for (unsigned int i = 1; i < values_count; i++) {
        int value = values[i];

        bool larger = value > max_value;

        max_position = larger ? i : max_position;
        max_value = larger ? value : max_value;
    }
    return max_position;
}

inline unsigned int max_pos2(int *values, unsigned int values_count) {
    unsigned int max_position = 0;
    int max_value = values[0];
    for (unsigned int i = 1; i < values_count; i++) {
        int value = values[i];
        if (value > max_value) {
            max_position = i;
            max_value = value;
        }
    }
    return max_position;
}

int main() {
    int *values = malloc(VALUES_COUNT * sizeof(int));
    unsigned int *positions = malloc(VALUES_COUNT * sizeof(int));
    unsigned int *result = malloc(VALUES_COUNT * sizeof(int));

    generate(values, positions, VALUES_COUNT);

    clock_t start, end;

    unsigned int result_count;

    start = clock();
    result_count = select_low1(values, VALUES_COUNT, RAND_MAX / 2, result);
    end = clock() ;
    printf("Select Low 1: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_low1(values, VALUES_COUNT, RAND_MAX / 2, result);
    end = clock() ;
    printf("Select Low 1: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_high1(values, VALUES_COUNT, RAND_MAX / 2, result);
    end = clock() ;
    printf("Select High 1: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_equal1(values, VALUES_COUNT, 2147483611, result);
    end = clock() ;
    printf("Select Equal 1: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_range1(values, VALUES_COUNT, RAND_MAX / 4, RAND_MAX / 4 * 3, result);
    end = clock() ;
    printf("Select Range 1: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_low2(values, VALUES_COUNT, RAND_MAX / 2, result);
    end = clock() ;
    printf("Select Low 2: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_high2(values, VALUES_COUNT, RAND_MAX / 2, result);
    end = clock() ;
    printf("Select High 2: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_equal2(values, VALUES_COUNT, 2147483611, result);
    end = clock() ;
    printf("Select Equal 2: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_range2(values, VALUES_COUNT, RAND_MAX / 4, RAND_MAX / 4 * 3, result);
    end = clock() ;
    printf("Select Range 2: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_range3(values, VALUES_COUNT, RAND_MAX / 4, RAND_MAX / 4 * 3, result);
    end = clock() ;
    printf("Select Range 3: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_pos_low1(positions, values, VALUES_COUNT, RAND_MAX / 2, result);
    end = clock() ;
    printf("Select Pos Low 1: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_pos_high1(positions, values, VALUES_COUNT, RAND_MAX / 2, result);
    end = clock() ;
    printf("Select Pos High 1: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_pos_equal1(positions, values, VALUES_COUNT, 2147483611, result);
    end = clock() ;
    printf("Select Pos Equal 1: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_pos_range1(positions, values, VALUES_COUNT, RAND_MAX / 4, RAND_MAX / 4 * 3, result);
    end = clock() ;
    printf("Select Pos Range 1: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_pos_low2(positions, values, VALUES_COUNT, RAND_MAX / 2, result);
    end = clock() ;
    printf("Select Pos Low 2: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_pos_high2(positions, values, VALUES_COUNT, RAND_MAX / 2, result);
    end = clock() ;
    printf("Select Pos High 2: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_pos_equal2(positions, values, VALUES_COUNT, 2147483611, result);
    end = clock() ;
    printf("Select Pos Equal 2: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_pos_range2(positions, values, VALUES_COUNT, RAND_MAX / 4, RAND_MAX / 4 * 3, result);
    end = clock() ;
    printf("Select Pos Range 2: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    start = clock();
    result_count = select_pos_range3(positions, values, VALUES_COUNT, RAND_MAX / 4, RAND_MAX / 4 * 3, result);
    end = clock() ;
    printf("Select Pos Range 3: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, result_count);
    printf("%d\n", result[rand() % VALUES_COUNT]);

    int max_result;

    start = clock();
    max_result = max1(values, VALUES_COUNT);
    end = clock() ;
    printf("Max 1: %f, %d\n", (double) (end - start) / CLOCKS_PER_SEC, max_result);

    start = clock();
    max_result = max2(values, VALUES_COUNT);
    end = clock() ;
    printf("Max 2: %f, %d\n", (double) (end - start) / CLOCKS_PER_SEC, max_result);

    start = clock();
    max_result = max3(values, VALUES_COUNT);
    end = clock() ;
    printf("Max 3: %f, %d\n", (double) (end - start) / CLOCKS_PER_SEC, max_result);

    start = clock();
    max_result = max4(values, VALUES_COUNT);
    end = clock() ;
    printf("Max 4: %f, %d\n", (double) (end - start) / CLOCKS_PER_SEC, max_result);

    unsigned int max_pos_result;

    start = clock();
    max_pos_result = max_pos1(values, VALUES_COUNT);
    end = clock() ;
    printf("Max Pos 1: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, max_pos_result);

    start = clock();
    max_pos_result = max_pos2(values, VALUES_COUNT);
    end = clock() ;
    printf("Max Pos 2: %f, %u\n", (double) (end - start) / CLOCKS_PER_SEC, max_pos_result);
}
