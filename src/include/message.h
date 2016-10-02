#ifndef MESSAGE_H
#define MESSAGE_H

#include <stdlib.h>

#define SHUTDOWN_FLAG (1 << (sizeof(MessageStatus) * 8 - 1))

// MesageStatus defines the status of the previous request.
#define MESSAGE_STATUSES \
    ENUM(OK) \
    ENUM(OK_WAIT_FOR_RESPONSE) \
    ENUM(UNKNOWN_COMMAND) \
    ENUM(INCORRECT_FORMAT) \
    ENUM(WRONG_NUMBER_OF_ARGUMENTS) \
    ENUM(WRONG_NUMBER_OF_HANDLES) \
    ENUM(QUERY_UNSUPPORTED) \
    ENUM(DATABASE_ALREADY_EXISTS) \
    ENUM(DATABASE_NOT_FOUND) \
    ENUM(TABLE_ALREADY_EXISTS) \
    ENUM(TABLE_NOT_FOUND) \
    ENUM(INVALID_NUMBER_OF_COLUMNS) \
    ENUM(TABLE_FULL) \
    ENUM(TABLE_NOT_FULLY_INITIALIZED) \
    ENUM(COLUMN_ALREADY_EXISTS) \
    ENUM(COLUMN_NOT_FOUND) \
    ENUM(INDEX_ALREADY_EXISTS) \
    ENUM(VARIABLE_NOT_FOUND) \
    ENUM(WRONG_VARIABLE_TYPE) \
    ENUM(TUPLE_COUNT_MISMATCH) \
    ENUM(EMPTY_VECTOR) \
    ENUM(NO_SELECT_CONDITION) \
    ENUM(INSERT_COLUMNS_MISMATCH) \
    ENUM(FILE_READ_ERROR) \
    ENUM(INCORRECT_FILE_FORMAT) \
    ENUM(COMMUNICATION_ERROR) \

typedef enum MessageStatus {
#define ENUM(X) X,
    MESSAGE_STATUSES
#undef ENUM
} MessageStatus;

// Message is a single packet of information sent between client/server.
// status: defines the status of the message.
// length: defines the length of the string message to be sent.
// payload: defines the payload of the message.
typedef struct Message {
    MessageStatus status;
    size_t length;
    void *payload;
} Message;

inline char *message_status_to_string(MessageStatus status) {
    switch (status) {
#define ENUM(X) case X: return #X;
    MESSAGE_STATUSES
#undef ENUM
    }
    return NULL;
}

#endif /* MESSAGE_H */
