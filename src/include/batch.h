#ifndef SCHEDULER_H
#define SCHEDULER_H

#include "client_context.h"
#include "db_operator.h"
#include "message.h"

void batch_query(DbOperator *dbo, Message *message);
void batch_execute_concurrently(ClientContext *client_context, Message *message);
void batch_execute_sequentially(ClientContext *client_context, Message *message);

#endif /* SCHEDULER_H */
