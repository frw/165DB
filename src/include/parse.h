#ifndef PARSE_H
#define PARSE_H

#include "client_context.h"
#include "db_operator.h"
#include "message.h"

DbOperator *parse_command(char* query_command, Message *send_message, ClientContext* context);

#endif /* PARSE_H */
