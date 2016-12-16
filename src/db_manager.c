#include <dirent.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/stat.h>

#include "db_manager.h"
#include "hash_table.h"
#include "message.h"
#include "queue.h"
#include "utils.h"
#include "vector.h"

#define DATA_DIRECTORY "data"

#define MAX_PATH_LENGTH 4096

#define FILE_MAGIC 0xC001D00D

#define DB_MANAGER_TABLE_INITIAL_CAPACITY 1
#define DB_MANAGER_TABLE_LOAD_FACTOR 1.0f

#define COLUMN_INITIAL_CAPACITY 8

Db *db_manager_dbs = NULL;

HashTable db_manager_table;
pthread_mutex_t db_manager_table_mutex;

static inline void db_free(Db *db);
static inline void table_free(Table *table);
static inline void column_free(Column *column);
static inline void index_free(ColumnIndex *index);

static inline bool db_save(Db *db);
static inline bool table_save(Table *table, FILE *file);
static inline bool column_save(Column *column, FILE *file);
static inline bool index_save(ColumnIndex *index, FILE *file);

static inline Db *db_load(char *db_name);
static inline Table *table_load(FILE *file);
static inline bool column_load(Column *column, unsigned int order, FILE *file);
static inline ColumnIndex *index_load(FILE *file);

static inline void db_register(Db *db);
static inline void table_register(Table *table, char *db_name);
static inline void column_register(Column *column, char *table_fqn);

void db_manager_startup() {
    hash_table_init(&db_manager_table, DB_MANAGER_TABLE_INITIAL_CAPACITY,
            DB_MANAGER_TABLE_LOAD_FACTOR);
    pthread_mutex_init(&db_manager_table_mutex, NULL);

    DIR *dir;
    struct dirent *ent;
    if ((dir = opendir(DATA_DIRECTORY)) != NULL) {
        while ((ent = readdir(dir)) != NULL) {
            if (!is_valid_name(ent->d_name)) {
                continue;
            }

            Db *db = db_load(ent->d_name);
            if (db != NULL) {
                db->next = db_manager_dbs;
                db_manager_dbs = db;
                db_register(db);
            }
        }
        closedir(dir);
    } else {
        log_err("Unable to open directory \"%s\"\n", DATA_DIRECTORY);
    }
}

void db_manager_shutdown() {
    for (Db *db = db_manager_dbs, *next; db != NULL; db = next) {
        db_save(db);
        next = db->next;
        db_free(db);
    }

    hash_table_destroy(&db_manager_table, NULL);
    pthread_mutex_destroy(&db_manager_table_mutex);
}

void db_create(char *name, Message *send_message) {
    pthread_mutex_lock(&db_manager_table_mutex);

    if (hash_table_get(&db_manager_table, name) != NULL) {
        send_message->status = DATABASE_ALREADY_EXISTS;
        pthread_mutex_unlock(&db_manager_table_mutex);
        return;
    }

    Db *db = malloc(sizeof(Db));
    db->name = strdup(name);
    db->tables = NULL;
    db->tables_count = 0;
    db->next = db_manager_dbs;

    db_manager_dbs = db;

    hash_table_put(&db_manager_table, name, db);

    pthread_mutex_unlock(&db_manager_table_mutex);
}

void table_create(char *name, char *db_name, unsigned int num_columns, Message *send_message) {
    char *table_fqn = strjoin(db_name, name, '.');

    pthread_mutex_lock(&db_manager_table_mutex);

    if (hash_table_get(&db_manager_table, table_fqn) != NULL) {
        send_message->status = TABLE_ALREADY_EXISTS;
        pthread_mutex_unlock(&db_manager_table_mutex);
        free(table_fqn);
        return;
    }

    Db *db = hash_table_get(&db_manager_table, db_name);
    if (db == NULL) {
        send_message->status = DATABASE_NOT_FOUND;
        pthread_mutex_unlock(&db_manager_table_mutex);
        free(table_fqn);
        return;
    }

    Table *table = malloc(sizeof(Table));
    table->name = strdup(name);
    table->columns = malloc(num_columns * sizeof(Column));
    table->columns_count = 0;
    table->columns_capacity = num_columns;
    table->rows_count = 0;
    queue_init(&table->delete_queue);
    table->deleted_rows = NULL;
    pthread_rwlock_init(&table->rwlock, NULL);
    table->db = db;
    table->next = db->tables;

    db->tables = table;
    db->tables_count++;
    hash_table_put(&db_manager_table, table_fqn, table);

    pthread_mutex_unlock(&db_manager_table_mutex);

    free(table_fqn);
}

void column_create(char *name, char *table_fqn, Message *send_message) {
    char *column_fqn = strjoin(table_fqn, name, '.');

    pthread_mutex_lock(&db_manager_table_mutex);

    if (hash_table_get(&db_manager_table, column_fqn) != NULL) {
        send_message->status = COLUMN_ALREADY_EXISTS;
        pthread_mutex_unlock(&db_manager_table_mutex);
        free(column_fqn);
        return;
    }

    Table *table = hash_table_get(&db_manager_table, table_fqn);
    if (table == NULL) {
        send_message->status = TABLE_NOT_FOUND;
        pthread_mutex_unlock(&db_manager_table_mutex);
        free(column_fqn);
        return;
    }

    if (table->columns_count == table->columns_capacity) {
        // Cannot add any more columns.
        send_message->status = TABLE_FULL;
        pthread_mutex_unlock(&db_manager_table_mutex);
        free(column_fqn);
        return;
    }

    Column *column = &table->columns[table->columns_count];
    column->name = strdup(name);
    column->order = table->columns_count;
    int_vector_init(&column->values, COLUMN_INITIAL_CAPACITY);
    column->index = NULL;
    column->table = table;

    table->columns_count++;
    hash_table_put(&db_manager_table, column_fqn, column);

    pthread_mutex_unlock(&db_manager_table_mutex);

    free(column_fqn);
}

static inline void filter_removed(int *values, bool *deleted_rows, unsigned int values_count,
        int *dst_values, unsigned int *dst_positions, unsigned int rows_count) {
    unsigned int j = 0;
    for (unsigned int i = 0; i < values_count && j < rows_count; i++) {
        dst_values[j] = values[i];
        dst_positions[j] = i;
        j += !deleted_rows[i];
    }
}

static void index_init(ColumnIndex *index) {
    Column *column = index->column;
    Table *table = column->table;

    unsigned int rows_count = table->rows_count;

    if (index->clustered) {
        unsigned int num_columns = table->columns_capacity;

        index->clustered_positions = malloc(sizeof(PosVector));
        index->clustered_columns = malloc(num_columns * sizeof(IntVector));
        index->num_columns = num_columns;

        if (rows_count == 0) {
            pos_vector_init(index->clustered_positions, 0);

            for (unsigned int i = 0; i < num_columns; i++) {
                int_vector_init(index->clustered_columns + i, 0);
            }

            switch (index->type) {
            case BTREE:
                btree_init(&index->fields.btree, NULL, NULL, 0);
                break;
            case SORTED:
                sorted_init(&index->fields.sorted, NULL, NULL, 0);
                break;
            }
        } else {
            IntVector *leading_values_vector = index->clustered_columns + column->order;
            int_vector_init(leading_values_vector, rows_count);
            int *leading_values = leading_values_vector->data;

            PosVector *positions_vector = index->clustered_positions;
            pos_vector_init(positions_vector, rows_count);
            unsigned int *sorted_positions = positions_vector->data;

            bool *deleted_rows = table->deleted_rows != NULL ? table->deleted_rows->data : NULL;
            if (deleted_rows == NULL) {
                radix_sort_indices(column->values.data, NULL, leading_values, sorted_positions, rows_count);
            } else {
                filter_removed(column->values.data, deleted_rows, column->values.size, leading_values, sorted_positions, rows_count);

                radix_sort_indices(leading_values, sorted_positions, leading_values, sorted_positions, rows_count);
            }


            leading_values_vector->size = rows_count;
            positions_vector->size = rows_count;

            for (unsigned int i = 0; i < num_columns; i++) {
                if (i != column->order) {
                    IntVector *values_vector = index->clustered_columns + i;
                    int_vector_init(values_vector, rows_count);

                    int *sorted_values = values_vector->data;
                    int *unsorted_values = table->columns[i].values.data;

                    for (unsigned int j = 0; j < rows_count; j++) {
                        sorted_values[j] = unsorted_values[sorted_positions[j]];
                    }

                    values_vector->size = rows_count;
                }
            }

            switch (index->type) {
            case BTREE:
                btree_init(&index->fields.btree, leading_values, NULL, rows_count);
                break;
            case SORTED:
                sorted_init(&index->fields.sorted, leading_values, NULL, rows_count);
                break;
            }
        }
    } else {
        index->clustered_positions = NULL;
        index->clustered_columns = NULL;
        index->num_columns = 0;

        if (rows_count == 0) {
            switch (index->type) {
            case BTREE:
                btree_init(&index->fields.btree, NULL, NULL, 0);
                break;
            case SORTED:
                sorted_init(&index->fields.sorted, NULL, NULL, 0);
                break;
            }
        } else {
            int *values = malloc(rows_count * sizeof(int));
            unsigned int *positions = malloc(rows_count * sizeof(unsigned int));

            bool *deleted_rows = table->deleted_rows != NULL ? table->deleted_rows->data : NULL;
            if (deleted_rows == NULL) {
                radix_sort_indices(column->values.data, NULL, values, positions, rows_count);
            } else {
                filter_removed(column->values.data, deleted_rows, column->values.size, values, positions, rows_count);

                radix_sort_indices(values, positions, values, positions, rows_count);
            }

            radix_sort_indices(column->values.data, NULL, values, positions, rows_count);

            switch (index->type) {
            case BTREE:
                btree_init(&index->fields.btree, values, positions, rows_count);
                break;
            case SORTED:
                sorted_init(&index->fields.sorted, values, positions, rows_count);
                break;
            }

            free(values);
            free(positions);
        }
    }
}

static void index_destroy(ColumnIndex *index) {
    switch (index->type) {
    case BTREE:
        btree_destroy(&index->fields.btree);
        break;
    case SORTED:
        sorted_destroy(&index->fields.sorted);
        break;
    }

    if (index->clustered) {
        if (index->clustered_positions != NULL) {
            pos_vector_destroy(index->clustered_positions);
            free(index->clustered_positions);
        }

        if (index->clustered_columns != NULL) {
            for (unsigned int i = 0; i < index->num_columns; i++) {
                int_vector_destroy(index->clustered_columns + i);
            }
            free(index->clustered_columns);
        }
    }
}

void index_create(char *column_fqn, ColumnIndexType type, bool clustered, Message *send_message) {
    Column *column = column_lookup(column_fqn);
    if (column == NULL) {
        send_message->status = COLUMN_NOT_FOUND;
        return;
    }

    Table *table = column->table;

    pthread_rwlock_wrlock(&table->rwlock);

    if (column->index != NULL) {
        send_message->status = INDEX_ALREADY_EXISTS;
        pthread_rwlock_unlock(&table->rwlock);
        return;
    }

    ColumnIndex *index = malloc(sizeof(ColumnIndex));
    index->type = type;
    index->clustered = clustered;
    index->column = column;

    index_init(index);

    column->index = index;

    pthread_rwlock_unlock(&table->rwlock);
}

void index_rebuild(ColumnIndex *index) {
    index_destroy(index);
    index_init(index);
}

static void *index_rebuild_routine(void *data) {
    ColumnIndex *index = data;
    index_rebuild(index);
    return NULL;
}

void index_rebuild_all(Table *table) {
    ColumnIndex *indices[table->columns_count];
    unsigned int indices_count = 0;
    for (unsigned int i = 0; i < table->columns_count; i++) {
        ColumnIndex *index = table->columns[i].index;
        if (index != NULL) {
            indices[indices_count++] = index;
        }
    }

    if (indices_count == 1) {
        index_rebuild(indices[0]);
    } else if (indices_count > 1){
        pthread_t threads[indices_count];

        for (unsigned int i = 0; i < indices_count; i++) {
            if (pthread_create(threads + i, NULL, &index_rebuild_routine, indices[i]) != 0) {
                log_err("Unable to spawn index rebuild worker thread.");
                exit(1);
            }
        }

        for (unsigned int i = 0; i < indices_count; i++) {
            pthread_join(threads[i], NULL);
        }
    }
}

Db *db_lookup(char *db_name) {
    pthread_mutex_lock(&db_manager_table_mutex);
    Db *db = hash_table_get(&db_manager_table, db_name);
    pthread_mutex_unlock(&db_manager_table_mutex);
    return db;
}

Table *table_lookup(char *table_fqn) {
    pthread_mutex_lock(&db_manager_table_mutex);
    Table *table = hash_table_get(&db_manager_table, table_fqn);
    pthread_mutex_unlock(&db_manager_table_mutex);
    return table;
}

Column *column_lookup(char *column_fqn) {
    pthread_mutex_lock(&db_manager_table_mutex);
    Column *column = hash_table_get(&db_manager_table, column_fqn);
    pthread_mutex_unlock(&db_manager_table_mutex);
    return column;
}

static inline void db_free(Db *db) {
    free(db->name);
    for (Table *table = db->tables, *next; table != NULL; table = next) {
        next = table->next;
        table_free(table);
    }
    free(db);
}

static inline void table_free(Table *table) {
    free(table->name);
    for (size_t i = 0; i < table->columns_count; i++) {
        column_free(&table->columns[i]);
    }
    free(table->columns);
    queue_destroy(&table->delete_queue);
    if (table->deleted_rows != NULL) {
        bool_vector_destroy(table->deleted_rows);
        free(table->deleted_rows);
    }
    pthread_rwlock_destroy(&table->rwlock);
    free(table);
}

static inline void column_free(Column *column) {
    free(column->name);
    int_vector_destroy(&column->values);
    if (column->index != NULL) {
        index_free(column->index);
    }
}

static inline void index_free(ColumnIndex *index) {
    index_destroy(index);
    free(index);
}

static inline bool db_save(Db *db) {
    struct stat st;

    if (stat(DATA_DIRECTORY, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            log_err("\"%s\" is not a directory\n", DATA_DIRECTORY);
            return false;
        }
    } else if (mkdir(DATA_DIRECTORY, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == -1) {
        log_err("Unable to make directory \"%s\"\n", DATA_DIRECTORY);
        return false;
    }

    char path[MAX_PATH_LENGTH];
    sprintf(path, "%s/%s", DATA_DIRECTORY, db->name);

    FILE *file = fopen(path, "wb");
    if (file == NULL) {
        return false;
    }

    unsigned int file_magic = FILE_MAGIC;
    if (fwrite(&file_magic, sizeof(file_magic), 1, file) != 1) {
        log_err("Unable to write file magic\n");
        goto ERROR;
    }

    if (fwrite(&db->tables_count, sizeof(db->tables_count), 1, file) != 1) {
        log_err("Unable to write database tables count\n");
        goto ERROR;
    }

    for (Table *table = db->tables; table != NULL; table = table->next) {
        if (!table_save(table, file)) {
            goto ERROR;
        }
    }

    fflush(file);
    fclose(file);

    return true;

ERROR:
    fclose(file);
    unlink(path);

    return false;
}

static inline bool table_save(Table *table, FILE *file) {
    unsigned int name_length = strlen(table->name);
    if (fwrite(&name_length, sizeof(name_length), 1, file) != 1) {
        log_err("Unable to write table name length\n");
        return false;
    }

    if (fwrite(table->name, sizeof(char), name_length, file) != name_length) {
        log_err("Unable to write table name\n");
        return false;
    }

    if (fwrite(&table->columns_capacity, sizeof(table->columns_capacity), 1, file) != 1) {
        log_err("Unable to write table columns capacity\n");
        return false;
    }

    if (fwrite(&table->columns_count, sizeof(table->columns_count), 1, file) != 1) {
        log_err("Unable to write table columns count\n");
        return false;
    }

    for (unsigned int i = 0; i < table->columns_count; i++) {
        if (!column_save(&table->columns[i], file)) {
            return false;
        }
    }

    if (!queue_save(&table->delete_queue, file)) {
        log_err("Unable to write table delete queue\n");
        return false;
    }

    if (fwrite(&table->rows_count, sizeof(table->rows_count), 1, file) != 1) {
        log_err("Unable to write table rows count\n");
        return false;
    }

    bool has_deleted_rows = table->deleted_rows != NULL;

    if (fwrite(&has_deleted_rows, sizeof(has_deleted_rows), 1, file) != 1) {
        log_err("Unable to write table has_deleted_rows\n");
        return false;
    }

    if (has_deleted_rows && !bool_vector_save(table->deleted_rows, file)) {
        log_err("Unable to write table deleted rows\n");
        return false;
    }

    return true;
}

static inline bool column_save(Column *column, FILE *file) {
    unsigned int name_length = strlen(column->name);
    if (fwrite(&name_length, sizeof(name_length), 1, file) != 1) {
        log_err("Unable to write column name length\n");
        return false;
    }

    if (fwrite(column->name, sizeof(char), name_length, file) != name_length) {
        log_err("Unable to write column name\n");
        return false;
    }

    if (!int_vector_save(&column->values, file)) {
        log_err("Unable to write column values\n");
        return false;
    }

    bool has_index = column->index != NULL;

    if (fwrite(&has_index, sizeof(has_index), 1, file) != 1) {
        log_err("Unable to write column has_index\n");
        return false;
    }

    if (has_index && !index_save(column->index, file)) {
        return false;
    }

    return true;
}

static inline bool index_save(ColumnIndex *index, FILE *file) {
    if (fwrite(&index->type, sizeof(index->type), 1, file) != 1) {
        log_err("Unable to write index type\n");
        return false;
    }

    if (fwrite(&index->clustered, sizeof(index->clustered), 1, file) != 1) {
        log_err("Unable to write index clustered\n");
        return false;
    }

    switch (index->type) {
    case BTREE:
        if (!btree_save(&index->fields.btree, file)) {
            return false;
        }
        break;
    case SORTED:
        if (!sorted_save(&index->fields.sorted, file)) {
            return false;
        }
        break;
    }

    if (index->clustered) {
        if (!pos_vector_save(index->clustered_positions, file)) {
            return false;
        }

        if (fwrite(&index->num_columns, sizeof(index->num_columns), 1, file) != 1) {
            log_err("Unable to write index num_columns\n");
            return false;
        }

        for (unsigned int i = 0; i < index->num_columns; i++) {
            if (!int_vector_save(index->clustered_columns + i, file)) {
                return false;
            }
        }
    }

    return true;
}

static inline Db *db_load(char *db_name) {
    char path[MAX_PATH_LENGTH];
    sprintf(path, "%s/%s", DATA_DIRECTORY, db_name);

    FILE *file = fopen(path, "rb");
    if (file == NULL) {
        return NULL;
    }

    unsigned int file_magic;
    if (fread(&file_magic, sizeof(file_magic), 1, file) != 1) {
        log_err("Unable to read file magic\n");
        fclose(file);
        return NULL;
    }
    if (file_magic != FILE_MAGIC) {
        log_err("Incorrect file magic: Expected 0x%08X but received 0x%08X", FILE_MAGIC,
                file_magic);
        fclose(file);
        return NULL;
    }

    unsigned int tables_count = 0;
    if (fread(&tables_count, sizeof(tables_count), 1, file) != 1) {
        log_err("Unable to read database tables count\n");
        fclose(file);
        return NULL;
    }

    Db *db = malloc(sizeof(Db));
    db->name = strdup(db_name);
    db->tables = NULL;
    db->tables_count = 0;

    for (unsigned int i = 0; i < tables_count; i++) {
        Table *table = table_load(file);
        if (table == NULL) {
            db_free(db);
            db = NULL;
            break;
        }

        table->db = db;
        table->next = db->tables;

        db->tables = table;
        db->tables_count++;
    }

    fclose(file);

    return db;
}

static inline Table *table_load(FILE *file) {
    unsigned int name_length;
    if (fread(&name_length, sizeof(name_length), 1, file) != 1) {
        log_err("Unable to read table name length\n");
        return NULL;
    }

    char *name = malloc((name_length + 1) * sizeof(char));
    if (fread(name, sizeof(char), name_length, file) != name_length) {
        log_err("Unable to read table name\n");
        free(name);
        return NULL;
    }
    name[name_length] = '\0';

    unsigned int columns_capacity;
    if (fread(&columns_capacity, sizeof(columns_capacity), 1, file) != 1) {
        log_err("Unable to read table columns capacity\n");
        free(name);
        return NULL;
    }

    unsigned int columns_count;
    if (fread(&columns_count, sizeof(columns_count), 1, file) != 1) {
        log_err("Unable to read table columns count\n");
        free(name);
        return NULL;
    }

    Table *table = malloc(sizeof(Table));
    table->name = name;
    table->columns = malloc(columns_capacity * sizeof(Column));
    table->columns_count = 0;
    table->columns_capacity = columns_capacity;
    queue_init(&table->delete_queue);
    table->deleted_rows = NULL;
    pthread_rwlock_init(&table->rwlock, NULL);

    for (unsigned int i = 0; i < columns_count; i++) {
        Column *column = &table->columns[i];
        if (!column_load(column, i, file)) {
            table_free(table);
            table = NULL;
            break;
        }

        column->table = table;

        table->columns_count++;
    }

    if (!queue_load(&table->delete_queue, file)) {
        log_err("Unable to read table delete queue\n");
        table_free(table);
        return false;
    }

    if (fread(&table->rows_count, sizeof(table->rows_count), 1, file) != 1) {
        log_err("Unable to read table rows count\n");
        table_free(table);
        return false;
    }

    bool has_deleted_rows;

    if (fread(&has_deleted_rows, sizeof(has_deleted_rows), 1, file) != 1) {
        log_err("Unable to read table has_deleted_rows\n");
        table_free(table);
        return false;
    }

    if (has_deleted_rows) {
        table->deleted_rows = malloc(sizeof(BoolVector));
        bool_vector_init(table->deleted_rows, 0);
        if (!bool_vector_load(table->deleted_rows, file)) {
            log_err("Unable to read table deleted rows\n");
            table_free(table);
            return false;
        }
    }

    return table;
}

static inline bool column_load(Column *column, unsigned int order, FILE *file) {
    unsigned int name_length;
    if (fread(&name_length, sizeof(name_length), 1, file) != 1) {
        log_err("Unable to read column name length\n");
        return false;
    }

    char *name = malloc((name_length + 1) * sizeof(char));
    if (fread(name, sizeof(char), name_length, file) != name_length) {
        log_err("Unable to read column name\n");
        free(name);
        return false;
    }
    name[name_length] = '\0';

    column->name = name;
    column->order = order;
    int_vector_init(&column->values, 0);
    column->index = NULL;

    if (!int_vector_load(&column->values, file)) {
        log_err("Unable to read column values\n");
        column_free(column);
        return false;
    }

    bool has_index;

    if (fread(&has_index, sizeof(has_index), 1, file) != 1) {
        log_err("Unable to read column has_index\n");
        column_free(column);
        return false;
    }

    if (has_index) {
        ColumnIndex *index = index_load(file);
        if (index == NULL) {
            column_free(column);
            return false;
        }

        index->column = column;

        column->index = index;
    }

    return true;
}

static inline ColumnIndex *index_load(FILE *file) {
    ColumnIndexType type;
    if (fread(&type, sizeof(type), 1, file) != 1) {
        log_err("Unable to read index type\n");
        return NULL;
    }

    bool clustered;
    if (fread(&clustered, sizeof(clustered), 1, file) != 1) {
        log_err("Unable to read index clustered\n");
        return NULL;
    }

    IndexFields fields;
    switch (type) {
    case BTREE:
        if (!btree_load(&fields.btree, file)) {
            return false;
        }
        break;
    case SORTED:
        if (!sorted_load(&fields.sorted, file)) {
            return false;
        }
        break;
    }

    ColumnIndex *index = malloc(sizeof(ColumnIndex));
    index->type = type;
    index->clustered = clustered;
    index->fields = fields;
    index->clustered_positions = NULL;
    index->clustered_columns = NULL;
    index->num_columns = 0;

    if (clustered) {
        index->clustered_positions = malloc(sizeof(PosVector));
        pos_vector_init(index->clustered_positions, 0);
        if (!pos_vector_load(index->clustered_positions, file)) {
            index_free(index);
            return NULL;
        }

        if (fread(&index->num_columns, sizeof(index->num_columns), 1, file) != 1) {
            log_err("Unable to read index num_columns\n");
            index_free(index);
            return false;
        }

        index->clustered_columns = malloc(index->num_columns * sizeof(IntVector));
        for (unsigned int i = 0; i < index->num_columns; i++) {
            int_vector_init(index->clustered_columns + i, 0);
        }
        for (unsigned int i = 0; i < index->num_columns; i++) {
            if (!int_vector_load(index->clustered_columns + i, file)) {
                index_free(index);
                return NULL;
            }
        }
    }

    return index;
}

static inline void db_register(Db *db) {
    hash_table_put(&db_manager_table, db->name, db);

    for (Table *table = db->tables; table != NULL; table = table->next) {
        table_register(table, db->name);
    }
}

static inline void table_register(Table *table, char *db_name) {
    char *table_fqn = strjoin(db_name, table->name, '.');

    hash_table_put(&db_manager_table, table_fqn, table);

    for (unsigned int i = 0; i < table->columns_count; i++) {
        column_register(&table->columns[i], table_fqn);
    }

    free(table_fqn);
}

static inline void column_register(Column *column, char *table_fqn) {
    char *column_fqn = strjoin(table_fqn, column->name, '.');

    hash_table_put(&db_manager_table, column_fqn, column);

    free(column_fqn);
}
