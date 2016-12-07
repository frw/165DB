#ifndef DB_OPERATOR_H
#define DB_OPERATOR_H

#include "client_context.h"
#include "dsl.h"
#include "vector.h"

/**
 * Necessary fields for database creation.
 */
typedef struct CreateDbOperator {
	char *name;
} CreateDbOperator;

/**
 * Necessary fields for table creation.
 */
typedef struct CreateTableOperator {
	char *name;
    char *db_name;
	unsigned int num_columns;
} CreateTableOperator;

/**
 * Necessary fields for column creation.
 */
typedef struct CreateColumnOperator {
	char *name;
    char *table_fqn;
} CreateColumnOperator;

/**
 * Necessary fields for index creation.
 */
typedef struct CreateIndexOperator {
	char *column_fqn;
	ColumnIndexType type;
	bool clustered;
} CreateIndexOperator;

/**
 * Necessary fields for loading.
 */
typedef struct LoadOperator {
    Vector *col_fqns;
    IntVector *col_vals;
} LoadOperator;

/**
 * Necessary fields for selection.
 */
typedef struct SelectOperator {
    GeneralizedColumnHandle col_hdl;
    Comparator comparator;
    char *pos_out_var;
} SelectOperator;

/**
 * Necessary fields for fetch-selection.
 */
typedef struct SelectPosOperator {
    char *pos_var;
    char *val_var;
    Comparator comparator;
    char *pos_out_var;
} SelectPosOperator;

/**
 * Necessary fields for fetching.
 */
typedef struct FetchOperator {
    char *column_fqn;
    char *pos_var;
    char *val_out_var;
} FetchOperator;

/**
 * Necessary fields for insertion.
 */
typedef struct RelationalInsertOperator {
    char *table_fqn;
    IntVector values;
} RelationalInsertOperator;

typedef struct RelationalDeleteOperator {
    char *table_fqn;
    char *pos_var;
} RelationalDeleteOperator;

typedef struct RelationalUpdateOperator {
    char *column_fqn;
    char *pos_var;
    int value;
} RelationalUpdateOperator;

typedef struct JoinOperator {
    JoinType type;
    char *val_var1;
    char *pos_var1;
    char *val_var2;
    char *pos_var2;
    char *pos_out_var1;
    char *pos_out_var2;
} JoinOperator;

typedef struct MinOperator {
    GeneralizedColumnHandle col_hdl;
    char *val_out_var;
} MinOperator;

typedef struct MinPosOperator {
    char *pos_var;
    GeneralizedColumnHandle col_hdl;
    char *pos_out_var;
    char *val_out_var;
} MinPosOperator;

typedef struct MaxOperator {
    GeneralizedColumnHandle col_hdl;
    char *val_out_var;
} MaxOperator;

typedef struct MaxPosOperator {
    char *pos_var;
    GeneralizedColumnHandle col_hdl;
    char *pos_out_var;
    char *val_out_var;
} MaxPosOperator;

typedef struct SumOperator {
    GeneralizedColumnHandle col_hdl;
    char *val_out_var;
} SumOperator;

typedef struct AvgOperator {
    GeneralizedColumnHandle col_hdl;
    char *val_out_var;
} AvgOperator;

typedef struct AddOperator {
    char *val_var1;
    char *val_var2;
    char *val_out_var;
} AddOperator;

typedef struct SubOperator {
    char *val_var1;
    char *val_var2;
    char *val_out_var;
} SubOperator;

typedef struct PrintOperator {
    Vector val_vars;
} PrintOperator;

/**
 * Tells the database what type of operator this is.
 */
typedef enum OperatorType {
    CREATE_DB,
    CREATE_TBL,
    CREATE_COL,
    CREATE_IDX,
    LOAD,
	SELECT,
	SELECT_POS,
	FETCH,
    RELATIONAL_INSERT,
	RELATIONAL_DELETE,
	RELATIONAL_UPDATE,
	JOIN,
	MIN,
    MIN_POS,
	MAX,
	MAX_POS,
	SUM,
	AVG,
	ADD,
	SUB,
	PRINT,
	BATCH_QUERIES,
	BATCH_EXECUTE,
	SHUTDOWN
} OperatorType;

/*
 * Union type holding the fields of any operator
 */
typedef union OperatorFields {
	CreateDbOperator create_db;
	CreateTableOperator create_table;
	CreateColumnOperator create_column;
	CreateIndexOperator create_index;
	LoadOperator load;
	SelectOperator select;
	SelectPosOperator select_pos;
	FetchOperator fetch;
    RelationalInsertOperator relational_insert;
    RelationalDeleteOperator relational_delete;
    RelationalUpdateOperator relational_update;
    JoinOperator join;
    MinOperator min;
    MinPosOperator min_pos;
    MaxOperator max;
    MaxPosOperator max_pos;
    SumOperator sum;
    AvgOperator avg;
    AddOperator add;
    SubOperator sub;
    PrintOperator print;
} OperatorFields;

/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields fields;
    ClientContext *context;
} DbOperator;

void db_operator_log(DbOperator *query);
void db_operator_execute(DbOperator *query, Message *message);
void db_operator_free(DbOperator *query);

#endif /* DB_OPERATOR_H */
