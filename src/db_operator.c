#include <stdlib.h>

#include "db_operator.h"
#include "dsl.h"
#include "message.h"
#include "utils.h"
#include "vector.h"

void db_operator_log(DbOperator *query) {
#ifdef LOG_INFO
    switch (query->type) {
    case CREATE_DB:
        log_info("CREATE_DB: %s\n", query->fields.create_db.name);
        break;
    case CREATE_TBL:
        log_info("CREATE_TBL: %s, %s, %d\n", query->fields.create_table.name,
                query->fields.create_table.db_name, query->fields.create_table.num_columns);
        break;
    case CREATE_COL:
        log_info("CREATE_COL: %s, %s\n", query->fields.create_column.name,
                query->fields.create_column.table_fqn);
        break;
    case CREATE_IDX:
        log_info("CREATE_IDX: %s, %d, %d\n", query->fields.create_index.column_fqn,
                query->fields.create_index.type, query->fields.create_index.clustered);
        break;
    case LOAD:
        log_info("LOAD\n");
        break;
    case SELECT:
        log_info("SELECT: %s(%d), %d(%d), %d(%d) -> %s\n", query->fields.select.col_hdl.name,
                query->fields.select.col_hdl.is_column_fqn, query->fields.select.comparator.low,
                query->fields.select.comparator.has_low, query->fields.select.comparator.high,
                query->fields.select.comparator.has_high, query->fields.select.pos_out_var);
        break;
    case SELECT_POS:
        log_info("SELECT_POS: %s, %s %d(%d), %d(%d) -> %s\n", query->fields.select_pos.pos_var,
                query->fields.select_pos.val_var, query->fields.select_pos.comparator.low,
                query->fields.select_pos.comparator.has_low,
                query->fields.select_pos.comparator.high,
                query->fields.select_pos.comparator.has_high, query->fields.select_pos.pos_out_var);
        break;
    case FETCH:
        log_info("FETCH: %s, %s -> %s\n", query->fields.fetch.column_fqn,
                query->fields.fetch.pos_var, query->fields.fetch.val_out_var);
        break;
    case RELATIONAL_INSERT:
        log_info("RELATIONAL_INSERT: %s", query->fields.relational_insert.table_fqn);
        for (size_t i = 0; i < query->fields.relational_insert.values.size; i++) {
            log_info(", %d", query->fields.relational_insert.values.data[i]);
        }
        log_info("\n");
        break;
    case RELATIONAL_DELETE:
        log_info("RELATIONAL_DELETE: %s, %s\n", query->fields.relational_delete.table_fqn,
                query->fields.relational_delete.pos_var);
        break;
    case RELATIONAL_UPDATE:
        log_info("RELATIONAL_UPDATE: %s, %s, %d\n", query->fields.relational_update.column_fqn,
                query->fields.relational_update.pos_var, query->fields.relational_update.value);
        break;
    case JOIN:
        log_info("JOIN: %d, %s, %s, %s, %s -> %s, %s\n", query->fields.join.type,
                query->fields.join.val_var1, query->fields.join.pos_var1,
                query->fields.join.val_var2, query->fields.join.pos_var2,
                query->fields.join.pos_out_var1, query->fields.join.pos_out_var2);
        break;
    case MIN:
        log_info("MIN: %s(%d) -> %s\n", query->fields.min.col_hdl.name,
                query->fields.min.col_hdl.is_column_fqn, query->fields.min.val_out_var);
        break;
    case MIN_POS:
        log_info("MIN_POS: %s, %s(%d) -> %s, %s\n", query->fields.min_pos.pos_var,
                query->fields.min_pos.col_hdl.name, query->fields.min_pos.col_hdl.is_column_fqn,
                query->fields.min_pos.pos_out_var, query->fields.min_pos.val_out_var);
        break;
    case MAX:
        log_info("MAX: %s(%d) -> %s\n", query->fields.max.col_hdl.name,
                query->fields.max.col_hdl.is_column_fqn, query->fields.max.col_hdl.is_column_fqn,
                query->fields.max.val_out_var);
        break;
    case MAX_POS:
        log_info("MAX_POS: %s, %s(%d) -> %s, %s\n", query->fields.max_pos.pos_var,
                query->fields.max_pos.col_hdl.name, query->fields.max_pos.col_hdl.is_column_fqn,
                query->fields.max_pos.pos_out_var, query->fields.max_pos.val_out_var);
        break;
    case SUM:
        log_info("SUM: %s(%d) -> %s\n", query->fields.sum.col_hdl.name,
                query->fields.sum.col_hdl.is_column_fqn, query->fields.sum.val_out_var);
        break;
    case AVG:
        log_info("AVG: %s(%d) -> %s\n", query->fields.avg.col_hdl.name,
                query->fields.avg.col_hdl.is_column_fqn, query->fields.avg.val_out_var);
        break;
    case ADD:
        log_info("ADD: %s, %s -> %s\n", query->fields.add.val_var1, query->fields.add.val_var2,
                query->fields.add.val_out_var);
        break;
    case SUB:
        log_info("SUB: %s, %s -> %s\n", query->fields.sub.val_var1, query->fields.sub.val_var2,
                query->fields.sub.val_out_var);
        break;
    case PRINT:
        log_info("PRINT: %s", query->fields.print.val_vars.data[0]);
        for (size_t i = 1; i < query->fields.print.val_vars.size; i++) {
            log_info(", %s", query->fields.print.val_vars.data[i]);
        }
        log_info("\n");
        break;
    case BATCH_QUERIES:
        log_info("BATCH_QUERIES\n");
        break;
    case BATCH_EXECUTE:
        log_info("BATCH_EXECUTE\n");
        break;
    case SHUTDOWN:
        log_info("SHUTDOWN\n");
        break;
    }
#else
    (void) query;
#endif
}

void db_operator_execute(DbOperator *query, Message *message) {
    switch (query->type) {
    case CREATE_DB:
        dsl_create_db(query->fields.create_db.name, message);
        break;
    case CREATE_TBL:
        dsl_create_table(query->fields.create_table.name, query->fields.create_table.db_name,
                query->fields.create_table.num_columns, message);
        break;
    case CREATE_COL:
        dsl_create_column(query->fields.create_column.name, query->fields.create_column.table_fqn,
                message);
        break;
    case CREATE_IDX:
        dsl_create_index(query->fields.create_index.column_fqn, query->fields.create_index.type,
                query->fields.create_index.clustered, message);
        break;
    case LOAD:
        dsl_load(query->fields.load.col_fqns, query->fields.load.col_vals, message);
        break;
    case SELECT:
        dsl_select(query->context, &query->fields.select.col_hdl, &query->fields.select.comparator,
                query->fields.select.pos_out_var, message);
        break;
    case SELECT_POS:
        dsl_select_pos(query->context, query->fields.select_pos.pos_var,
                query->fields.select_pos.val_var, &query->fields.select.comparator,
                query->fields.select_pos.pos_out_var, message);
        break;
    case FETCH:
        dsl_fetch(query->context, query->fields.fetch.column_fqn, query->fields.fetch.pos_var,
                query->fields.fetch.val_out_var, message);
        break;
    case RELATIONAL_INSERT:
        dsl_relational_insert(query->fields.relational_insert.table_fqn,
                &query->fields.relational_insert.values, message);
        break;
    case RELATIONAL_DELETE:
        dsl_relational_delete(query->context, query->fields.relational_delete.table_fqn,
                query->fields.relational_delete.pos_var, message);
        break;
    case RELATIONAL_UPDATE:
        dsl_relational_update(query->context, query->fields.relational_update.column_fqn,
                query->fields.relational_update.pos_var, query->fields.relational_update.value,
                message);
        break;
    case JOIN:
        dsl_join(query->context, query->fields.join.type, query->fields.join.val_var1,
                query->fields.join.pos_var1, query->fields.join.val_var2,
                query->fields.join.pos_var2, query->fields.join.pos_out_var1,
                query->fields.join.pos_out_var2, message);
        break;
    case MIN:
        dsl_min(query->context, &query->fields.min.col_hdl, query->fields.min.val_out_var, message);
        break;
    case MIN_POS:
        dsl_min_pos(query->context, query->fields.min_pos.pos_var, &query->fields.min_pos.col_hdl,
                query->fields.min_pos.pos_out_var, query->fields.min_pos.val_out_var, message);
        break;
    case MAX:
        dsl_max(query->context, &query->fields.max.col_hdl, query->fields.max.val_out_var, message);
        break;
    case MAX_POS:
        dsl_max_pos(query->context, query->fields.max_pos.pos_var, &query->fields.max_pos.col_hdl,
                query->fields.max_pos.pos_out_var, query->fields.max_pos.val_out_var, message);
        break;
    case SUM:
        dsl_sum(query->context, &query->fields.sum.col_hdl, query->fields.sum.val_out_var, message);
        break;
    case AVG:
        dsl_avg(query->context, &query->fields.avg.col_hdl, query->fields.avg.val_out_var, message);
        break;
    case ADD:
        dsl_add(query->context, query->fields.add.val_var1, query->fields.add.val_var2,
                query->fields.add.val_out_var, message);
        break;
    case SUB:
        dsl_sub(query->context, query->fields.sub.val_var1, query->fields.sub.val_var2,
                query->fields.sub.val_out_var, message);
        break;
    case PRINT:
        dsl_print(query->context, &query->fields.print.val_vars, message);
        break;
    case BATCH_QUERIES:
        dsl_batch_queries(query->context, message);
        break;
    case BATCH_EXECUTE:
        dsl_batch_execute(query->context, message);
        break;
    case SHUTDOWN:
        dsl_shutdown();
        break;
    }
}

void db_operator_free(DbOperator *query) {
    switch (query->type) {
    case CREATE_DB:
        free(query->fields.create_db.name);
        break;
    case CREATE_TBL:
        free(query->fields.create_table.name);
        free(query->fields.create_table.db_name);
        break;
    case CREATE_COL:
        free(query->fields.create_column.name);
        free(query->fields.create_column.table_fqn);
        break;
    case CREATE_IDX:
        free(query->fields.create_index.column_fqn);
        break;
    case LOAD:
        if (query->fields.load.col_fqns != NULL && query->fields.load.col_vals != NULL) {
            for (unsigned int i = 0; i < query->fields.load.col_fqns->size; i++) {
                int_vector_destroy(&query->fields.load.col_vals[i]);
            }
            free(query->fields.load.col_vals);

            vector_destroy(query->fields.load.col_fqns, &free);
            free(query->fields.load.col_fqns);
        }
        break;
    case SELECT:
        free(query->fields.select.col_hdl.name);
        free(query->fields.select.pos_out_var);
        break;
    case SELECT_POS:
        free(query->fields.select_pos.pos_var);
        free(query->fields.select_pos.val_var);
        free(query->fields.select_pos.pos_out_var);
        break;
    case FETCH:
        free(query->fields.fetch.column_fqn);
        free(query->fields.fetch.pos_var);
        free(query->fields.fetch.val_out_var);
        break;
    case RELATIONAL_INSERT:
        free(query->fields.relational_insert.table_fqn);
        int_vector_destroy(&query->fields.relational_insert.values);
        break;
    case RELATIONAL_DELETE:
        free(query->fields.relational_delete.table_fqn);
        free(query->fields.relational_delete.pos_var);
        break;
    case RELATIONAL_UPDATE:
        free(query->fields.relational_update.column_fqn);
        free(query->fields.relational_update.pos_var);
        break;
    case JOIN:
        free(query->fields.join.val_var1);
        free(query->fields.join.pos_var1);
        free(query->fields.join.val_var2);
        free(query->fields.join.pos_var2);
        free(query->fields.join.pos_out_var1);
        free(query->fields.join.pos_out_var2);
        break;
    case MIN:
        free(query->fields.min.col_hdl.name);
        free(query->fields.min.val_out_var);
        break;
    case MIN_POS:
        free(query->fields.min_pos.pos_var);
        free(query->fields.min_pos.col_hdl.name);
        free(query->fields.min_pos.pos_out_var);
        free(query->fields.min_pos.val_out_var);
        break;
    case MAX:
        free(query->fields.max.col_hdl.name);
        free(query->fields.max.val_out_var);
        break;
    case MAX_POS:
        free(query->fields.max_pos.pos_var);
        free(query->fields.max_pos.col_hdl.name);
        free(query->fields.max_pos.pos_out_var);
        free(query->fields.max_pos.val_out_var);
        break;
    case SUM:
        free(query->fields.sum.col_hdl.name);
        free(query->fields.sum.val_out_var);
        break;
    case AVG:
        free(query->fields.avg.col_hdl.name);
        free(query->fields.avg.val_out_var);
        break;
    case ADD:
        free(query->fields.add.val_var1);
        free(query->fields.add.val_var2);
        free(query->fields.add.val_out_var);
        break;
    case SUB:
        free(query->fields.sub.val_var1);
        free(query->fields.sub.val_var2);
        free(query->fields.sub.val_out_var);
        break;
    case PRINT:
        vector_destroy(&query->fields.print.val_vars, &free);
        break;
    case BATCH_QUERIES:
        break;
    case BATCH_EXECUTE:
        break;
    case SHUTDOWN:
        break;
    }
    free(query);
}
