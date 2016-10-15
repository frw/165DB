#ifndef DSL_H
#define DSL_H

#include "client_context.h"
#include "message.h"
#include "vector.h"

typedef enum CreateIndexType {
    BTREE, SORTED
} CreateIndexType;

typedef enum JoinType {
    HASH, NESTED_LOOP
} JoinType;

typedef struct GeneralizedColumnHandle {
    char *name;
    bool is_column_fqn;
} GeneralizedColumnHandle;

void dsl_create_db(char *name, Message *send_message);
void dsl_create_table(char *name, char *db_name, unsigned int num_columns, Message *send_message);
void dsl_create_column(char *name, char *table_fqn, Message *send_message);
void dsl_create_index(char *column_fqn, CreateIndexType type, bool clustered, Message *send_message);

void dsl_load(Vector *file_contents, Message *send_message);

void dsl_select(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, int low,
        bool has_low, int high, bool has_high, char *pos_out_var, Message *send_message);
void dsl_select_pos(ClientContext *client_context, char *pos_var, char *val_var, int low,
        bool has_low, int high, bool has_high, char *pos_out_var, Message *send_message);

void dsl_fetch(ClientContext *client_context, char *column_fqn, char *pos_var, char *val_out_var,
        Message *send_message);

void dsl_relational_insert(char *table_fqn, IntVector *values, Message *send_message);
void dsl_relational_delete(ClientContext *client_context, char *table_fqn, char *pos_var,
        Message *send_message);
void dsl_relational_update(ClientContext *client_context, char *column_fqn, char *pos_var,
        int value, Message *send_message);

void dsl_join(ClientContext *client_context, char *val_var1, char *pos_var1, char *val_var2,
        char *pos_var2, char *pos_out_var1, char *pos_out_var2, JoinType type,
        Message *send_message);

void dsl_min(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message);
void dsl_min_pos(ClientContext *client_context, char *pos_var, GeneralizedColumnHandle *col_hdl,
        char *pos_out_var, char *val_out_var, Message *send_message);
void dsl_max(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message);
void dsl_max_pos(ClientContext *client_context, char *pos_var, GeneralizedColumnHandle *col_hdl,
        char *pos_out_var, char *val_out_var, Message *send_message);

void dsl_sum(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message);
void dsl_avg(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message);

void dsl_add(ClientContext *client_context, char *val_var1, char *val_var2, char *val_out_var,
        Message *send_message);
void dsl_sub(ClientContext *client_context, char *val_var1, char *val_var2, char *val_out_var,
        Message *send_message);

void dsl_print(ClientContext *client_context, Vector *val_vars, Message *send_message);

void dsl_batch_queries(Message *send_message);
void dsl_batch_execute(Message *send_message);

void dsl_shutdown(Message *send_message);

bool is_shutdown_initiated();

#endif /* DSL_H */
