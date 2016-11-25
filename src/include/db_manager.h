#ifndef DB_MANAGER_H
#define DB_MANAGER_H

#include <stdbool.h>

#include "btree.h"
#include "common.h"
#include "hash_table.h"
#include "message.h"
#include "sorted.h"
#include "vector.h"

typedef struct Db Db;
typedef struct Table Table;
typedef struct Column Column;
typedef struct ColumnIndex ColumnIndex;

/**
 * Db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/
struct Db {
    char *name;
    Table *tables;
    unsigned int tables_count;
    Db *next;
};

/**
 * Table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/
struct Table {
    char *name;
    Column *columns;
    unsigned int columns_count;
    unsigned int columns_capacity;
    Db *db;
    Table *next;
};

struct Column {
    char *name;
    unsigned int order;
    IntVector values;
    ColumnIndex *index;
    Table *table;
};

typedef enum ColumnIndexType {
    BTREE, SORTED
} ColumnIndexType;

typedef union IndexFields {
    BTreeIndex btree;
    SortedIndex sorted;
} IndexFields;

struct ColumnIndex {
    ColumnIndexType type;
    bool clustered;
    IndexFields fields;
    PosVector *clustered_positions;
    IntVector *clustered_columns;
    unsigned int num_columns;
    Column *column;
};

extern HashTable db_manager_table;

void db_manager_startup();
void db_manager_shutdown();

void db_create(char *name, Message *send_message);
void table_create(char *name, char *db_name, unsigned int num_columns, Message *send_message);
void column_create(char *name, char *table_fqn, Message *send_message);
void index_create(char *column_fqn, ColumnIndexType type, bool clustered, Message *send_message);
void index_rebuild_all(Table *table);

static inline Db *db_lookup(char *db_name) {
    return hash_table_get(&db_manager_table, db_name);
}
static inline Table *table_lookup(char *table_fqn) {
    return hash_table_get(&db_manager_table, table_fqn);
}
static inline Column *column_lookup(char *column_fqn) {
    return hash_table_get(&db_manager_table, column_fqn);
}

#endif /* DB_MANAGER_H */
