/**
 * @file parse.c
 * @implements parse.h
 */

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db_schema.h"
#include "logging.h"
#include "parse.h"

/**
 * The context available to a parsing function.
 *
 * This includes the argument string (excluding the command name and the opening
 * parenthesis), the message to send back to the client, the client context, the
 * batch context, the name of the handle (NULL if not applicable), and an extra
 * flag that is needed by certain parsing functions.
 */
typedef struct ParserContext {
  char *args;
  Message *send_message;
  ClientContext *context;
  BatchContext *batch_context;
  char *handle_name;
  int flag;
} ParserContext;

/**
 * Command handler structure for command parsing.
 *
 * Prefix is the command name to match as prefix. Length is the length of the
 * prefix plus 1 for the opening parenthesis (excluding null terminator). The
 * parse function is the function to call to parse the command. There is an
 * optional extra flag that is needed by certain parsing functions.
 */
typedef struct CommandHandler {
  DbOperator *(*parser)(ParserContext *);
  const char *prefix;
  size_t length;
  int flag;
} CommandHandler;

/**
 * Helper macro to early return NULL if batching is active.
 */
#define _ERROR_IF_BATCHING                                                     \
  if (ctx->batch_context->is_active) {                                         \
    ctx->send_message->status = MESSAGE_STATUS_PARSE_ERROR;                    \
    ctx->send_message->payload = "Unbatchable command type.";                  \
    ctx->send_message->length = strlen(ctx->send_message->payload);            \
    return NULL;                                                               \
  }

/**
 * Helper macro to allocate a tokenizer for the command arguments.
 *
 * This macro will create `tokenizer` and `to_free` variables. The tokenizer is
 * a copy of the arguments string because `strsep` will modify the string in
 * place. The `to_free` variable is used to free the tokenizer later.
 */
#define _TOKENIZE_ARGS                                                         \
  char *tokenizer, *to_free;                                                   \
  tokenizer = to_free = _internal_malloc(                                      \
      (strlen(ctx->args) + 1) * sizeof(char), ctx->send_message);              \
  if (tokenizer == NULL) {                                                     \
    return NULL;                                                               \
  }                                                                            \
  strcpy(tokenizer, ctx->args);

/**
 * Helper macro to get a next token from the tokenizer.
 *
 * This will create the specified token variable and set it to the next token in
 * the tokenizer. If there are no more tokens, this will early return NULL and
 * set proper message status.
 */
#define _NEXT_TOKEN(token_name)                                                \
  char *token_name = strsep(&tokenizer, ",");                                  \
  if (token_name == NULL) {                                                    \
    ctx->send_message->status = MESSAGE_STATUS_INVALID_COMMAND;                \
    free(to_free);                                                             \
    return NULL;                                                               \
  }

/**
 * Helper macro to expect no more tokens in the tokenizer.
 *
 * This will early return NULL and set proper message status if there are more
 * tokens in the tokenizer.
 */
#define _EXPECT_NO_MORE_TOKENS                                                 \
  if (tokenizer != NULL) {                                                     \
    ctx->send_message->status = MESSAGE_STATUS_INVALID_COMMAND;                \
    free(to_free);                                                             \
    return NULL;                                                               \
  }

/**
 * Helper macro to set the object name in some operator.
 *
 * Object names should be surrounded by double quotes and with length not
 * exceeding a limit. This will early return NULL and set proper message if
 * these are not satisfied. Otherwise it will set the object name in the
 * operator removing the quotes.
 */
#define _SET_OBJECT_NAME(object, token)                                        \
  do {                                                                         \
    size_t token_length = strlen(token);                                       \
    if (token[0] != '"' || token[token_length - 1] != '"') {                   \
      ctx->send_message->status = MESSAGE_STATUS_INVALID_COMMAND;              \
      free(dbo);                                                               \
      free(to_free);                                                           \
      return NULL;                                                             \
    }                                                                          \
    token[token_length - 1] = '\0';                                            \
    if (token_length - 1 >= MAX_SIZE_NAME) {                                   \
      ctx->send_message->status = MESSAGE_STATUS_INVALID_COMMAND;              \
      free(dbo);                                                               \
      free(to_free);                                                           \
      return NULL;                                                             \
    }                                                                          \
    memcpy(dbo->fields.object, token + 1, token_length - 1);                   \
    dbo->fields.object[MAX_SIZE_NAME - 1] = '\0';                              \
  } while (0)

/**
 * Helper macro to set the output handle name in some operator.
 *
 * If the handle name is not provided by the parser context, this will set the
 * first byte of the handle to null terminator so that we can deterministically
 * check this later.
 */
#define _SET_HANDLE_NAME(handle, source)                                       \
  if (source != NULL) {                                                        \
    strncpy(dbo->fields.handle, source, HANDLE_MAX_SIZE);                      \
    dbo->fields.handle[HANDLE_MAX_SIZE - 1] = '\0';                            \
  } else {                                                                     \
    dbo->fields.handle[0] = '\0';                                              \
  }

/**
 * Helper macro to throw parse error if a condition is met.
 */
#define _THROW_PARSE_ERROR_IF(condition, message)                              \
  if (condition) {                                                             \
    ctx->send_message->status = MESSAGE_STATUS_PARSE_ERROR;                    \
    ctx->send_message->payload = message;                                      \
    ctx->send_message->length = strlen(message);                               \
    free(dbo);                                                                 \
    free(to_free);                                                             \
    return NULL;                                                               \
  }

/**
 * Helper macro to throw parse error with custom cleanup if a condition is met.
 */
#define _THROW_PARSE_ERROR_CUSTOM_CLEANUP_IF(condition, message, cleanup)      \
  if (condition) {                                                             \
    ctx->send_message->status = MESSAGE_STATUS_PARSE_ERROR;                    \
    ctx->send_message->payload = message;                                      \
    ctx->send_message->length = strlen(message);                               \
    cleanup;                                                                   \
    return NULL;                                                               \
  }

static char *DB_ERROR =
    "The database variable must be the current active database.";
static char *TABLE_ERROR =
    "The table variable must be an existing table in the database.";
static char *COLUMN_ERROR =
    "The column variable must be an existing column in the table.";
static char *INDEX_ERROR =
    "The index variable must be an existing index on the column.";

static char *VALVEC_ERROR = "The value vector variable does not exist in the "
                            "context and is not an existing column.";
static char *POSVEC_ERROR =
    "The position vector variable does not exist in the context.";
static char *NUMVAL_ERROR =
    "The numeric value variable does not exist in the context.";

/**
 * Internal malloc function that also sets the message.
 */
static inline void *_internal_malloc(size_t size, Message *message) {
  void *ptr = malloc(size);
  if (ptr == NULL) {
    message->status = MESSAGE_STATUS_PARSE_ERROR;
    message->payload = "Internal memory allocation failure during parsing.";
    message->length = strlen(message->payload);
    return NULL;
  }
  return ptr;
}

/**
 * Internal realloc function that also sets the message.
 */
static inline void *_internal_realloc(void *ptr, size_t size,
                                      Message *message) {
  void *new_ptr = realloc(ptr, size);
  if (new_ptr == NULL) {
    message->status = MESSAGE_STATUS_PARSE_ERROR;
    message->payload = "Internal memory reallocation failure during parsing.";
    message->length = strlen(message->payload);
    return NULL;
  }
  return new_ptr;
}

/**
 * Parse a range bound into a long integer.
 *
 * This function takes in a string representation of a range bound. If the bound
 * is a number, it will be returned and status will be untouched. If the bound
 * is exactly "null", the status will also be untouched and LONG_MIN or
 * LONG_MAX will be returned depending on whether the bound is a lower or upper
 * bound. If there are parsing errors, status will be set to bad argument and 0
 * will be returned as the dummy value.
 */
static inline long _parse_range_bound(ParserContext *ctx, char *expr,
                                      bool is_lower) {
  if (strcmp(expr, "null") == 0) {
    return is_lower ? LONG_MIN : LONG_MAX;
  }

  // Parse the bound as a long integer; note that only when *endptr is the null
  // terminator is the entire string validly parsed
  char *endptr;
  long value = strtol(expr, &endptr, 10);
  if (*endptr != '\0') {
    ctx->send_message->status = MESSAGE_STATUS_PARSE_ERROR;
    ctx->send_message->payload =
        "Invalid range; bounds must be integer or \"null\".";
    ctx->send_message->length = strlen(ctx->send_message->payload);
    return 0;
  }
  return value;
}

/**
 * Check whether two value vector handles are equal.
 *
 * Most handles are stored in the client context so they are equal only when
 * they are pointing to exactly the same memory address. However there is an
 * exception for value vector handles that wrap actual database columns, which
 * are not owned by the client context but rather created and freed on demand.
 * In the special case if they has the same underlying database column we still
 * consider them to be equal.
 */
static inline bool _valvec_handles_are_equal(GeneralizedValvecHandle *handle1,
                                             GeneralizedValvecHandle *handle2) {
  if (handle1 == handle2) {
    return true;
  }

  return handle1 != NULL && handle2 != NULL &&
         handle1->generalized_valvec.valvec_type ==
             GENERALIZED_VALVEC_TYPE_COLUMN &&
         handle2->generalized_valvec.valvec_type ==
             GENERALIZED_VALVEC_TYPE_COLUMN &&
         handle1->generalized_valvec.valvec_pointer.column ==
             handle2->generalized_valvec.valvec_pointer.column;
}

/**
 * Subroutine to parse the create database command.
 */
static inline DbOperator *_parse_create_db(ParserContext *ctx, char *tokenizer,
                                           char *to_free) {
  _NEXT_TOKEN(db_name);
  _EXPECT_NO_MORE_TOKENS;

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_CREATE;
  dbo->fields.create.create_type = CREATE_TYPE_DB;

  _SET_OBJECT_NAME(create.spec.db.name, db_name);
  return dbo;
}

/**
 * Subroutine to parse the create table command.
 */
static inline DbOperator *_parse_create_tbl(ParserContext *ctx, char *tokenizer,
                                            char *to_free) {
  _NEXT_TOKEN(table_name);
  _NEXT_TOKEN(db_var);
  _NEXT_TOKEN(col_cnt);
  _EXPECT_NO_MORE_TOKENS;

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_CREATE;
  dbo->fields.create.create_type = CREATE_TYPE_TABLE;

  // Turn the string column count into an integer; if the conversion failed then
  // 0 will be returned, and we indeed do not support a table with 0 columns
  int n_cols = atoi(col_cnt);
  _THROW_PARSE_ERROR_IF(n_cols <= 0,
                        "The number of columns must be a positive integer.");
  dbo->fields.create.spec.tbl.n_cols = n_cols;

  // Check that the database argument is the current active database
  DbSchemaStatus lookup_status = lookup_db(db_var);
  _THROW_PARSE_ERROR_IF(lookup_status != DB_SCHEMA_STATUS_OK, DB_ERROR);
  dbo->fields.create.spec.tbl.db = __DB__;

  _SET_OBJECT_NAME(create.spec.tbl.name, table_name);
  return dbo;
}

/**
 * Subroutine to parse the create column command.
 */
static inline DbOperator *_parse_create_col(ParserContext *ctx, char *tokenizer,
                                            char *to_free) {
  _NEXT_TOKEN(col_name);
  _NEXT_TOKEN(table_var);
  _EXPECT_NO_MORE_TOKENS;

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_CREATE;
  dbo->fields.create.create_type = CREATE_TYPE_COLUMN;

  // Check that the table argument is an existing table
  Table *table;
  DbSchemaStatus lookup_status = lookup_table(table_var, &table);
  _THROW_PARSE_ERROR_IF(lookup_status != DB_SCHEMA_STATUS_OK, TABLE_ERROR);
  dbo->fields.create.spec.col.table = table;
  dbo->fields.create.spec.col.db = __DB__;

  _SET_OBJECT_NAME(create.spec.col.name, col_name);
  return dbo;
}

/**
 * Subroutine to parse the create index command.
 */
static inline DbOperator *_parse_create_idx(ParserContext *ctx, char *tokenizer,
                                            char *to_free) {
  _NEXT_TOKEN(col_name);
  _NEXT_TOKEN(index_type);
  _NEXT_TOKEN(index_metatype);
  _EXPECT_NO_MORE_TOKENS;

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_CREATE;
  dbo->fields.create.create_type = CREATE_TYPE_INDEX;

  // Set the index type in the operator
  if (strcmp(index_type, "btree") == 0) {
    if (strcmp(index_metatype, "clustered") == 0) {
      dbo->fields.create.spec.idx.index_type =
          COLUMN_INDEX_TYPE_CLUSTERED_BTREE;
    } else if (strcmp(index_metatype, "unclustered") == 0) {
      dbo->fields.create.spec.idx.index_type =
          COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE;
    } else {
      _THROW_PARSE_ERROR_IF(true, INDEX_ERROR);
    }
  } else if (strcmp(index_type, "sorted") == 0) {
    if (strcmp(index_metatype, "clustered") == 0) {
      dbo->fields.create.spec.idx.index_type =
          COLUMN_INDEX_TYPE_CLUSTERED_SORTED;
    } else if (strcmp(index_metatype, "unclustered") == 0) {
      dbo->fields.create.spec.idx.index_type =
          COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED;
    } else {
      _THROW_PARSE_ERROR_IF(true, INDEX_ERROR);
    }
  } else {
    _THROW_PARSE_ERROR_IF(true, INDEX_ERROR);
  }

  // Check that the column argument is an existing column
  Table *table;
  size_t ith_column;
  DbSchemaStatus lookup_status = lookup_column(col_name, &table, &ith_column);
  _THROW_PARSE_ERROR_IF(lookup_status != DB_SCHEMA_STATUS_OK, COLUMN_ERROR);
  dbo->fields.create.spec.idx.table = table;
  dbo->fields.create.spec.idx.ith_column = ith_column;

  return dbo;
}

/**
 * Subroutine to parse printing of value vectors.
 */
static inline DbOperator *_parse_print_valvecs(ParserContext *ctx,
                                               GeneralizedValvecHandle *first,
                                               char *tokenizer, char *to_free) {
  // Allocate memory for the value vector handles; note that we are forcing a
  // hard limit on the number of handles to print instead of dynamically growing
  // the allocated memory
  GeneralizedValvecHandle **valvec_handles = _internal_malloc(
      MAX_PRINT_HANDLES * sizeof(GeneralizedValvecHandle *), ctx->send_message);
  if (valvec_handles == NULL) {
    free(to_free);
    return NULL;
  }
  valvec_handles[0] = first;
  size_t n_tokens = 1;

  // Get the value vector handles to print
  char *token;
  while ((token = strsep(&tokenizer, ",")) != NULL &&
         n_tokens < MAX_PRINT_HANDLES) {
    GeneralizedValvecHandle *valvec_handle =
        lookup_valvec_handle(ctx->context, token, true);
    _THROW_PARSE_ERROR_CUSTOM_CLEANUP_IF(valvec_handle == NULL, VALVEC_ERROR, {
      free(valvec_handles);
      free(to_free);
    });
    _THROW_PARSE_ERROR_CUSTOM_CLEANUP_IF(
        valvec_handle->generalized_valvec.valvec_length !=
            first->generalized_valvec.valvec_length,
        "All value vectors must have the same length.", {
          free(valvec_handles);
          free(to_free);
        });
    valvec_handles[n_tokens++] = valvec_handle;
  }

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(valvec_handles);
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_PRINT;
  dbo->fields.print.is_numval = false;
  dbo->fields.print.n_handles = n_tokens;
  dbo->fields.print.numval_handles = NULL;
  dbo->fields.print.valvec_handles = valvec_handles;

  free(to_free);
  return dbo;
}

/**
 * Subroutine to parse printing of numeric values.
 */
static inline DbOperator *_parse_print_numvals(ParserContext *ctx,
                                               NumericValueHandle *first,
                                               char *tokenizer, char *to_free) {
  // Allocate memory for the numeric value handles; note that we are forcing a
  // hard limit on the number of handles to print instead of dynamically growing
  // the allocated memory
  NumericValueHandle **numval_handles = _internal_malloc(
      MAX_PRINT_HANDLES * sizeof(NumericValueHandle *), ctx->send_message);
  if (numval_handles == NULL) {
    free(to_free);
    return NULL;
  }
  numval_handles[0] = first;
  size_t n_tokens = 1;

  // Get the numeric value handles to print
  char *token;
  while ((token = strsep(&tokenizer, ",")) != NULL &&
         n_tokens < MAX_PRINT_HANDLES) {
    NumericValueHandle *numval_handle =
        lookup_numval_handle(ctx->context, token);
    _THROW_PARSE_ERROR_CUSTOM_CLEANUP_IF(numval_handle == NULL, NUMVAL_ERROR, {
      free(numval_handles);
      free(to_free);
    });
    numval_handles[n_tokens++] = numval_handle;
  }

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(numval_handles);
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_PRINT;
  dbo->fields.print.is_numval = true;
  dbo->fields.print.n_handles = n_tokens;
  dbo->fields.print.valvec_handles = NULL;
  dbo->fields.print.numval_handles = numval_handles;

  free(to_free);
  return dbo;
}

/**
 * Activate a batch context for the client.
 *
 * This is an error if there is already an active batch context.
 */
static void _activate_batch(Message *send_message,
                            BatchContext *batch_context) {
  if (batch_context->is_active) {
    send_message->status = MESSAGE_STATUS_BATCH_ERROR;
    send_message->payload = "Cannot start a new batch while a previous one is "
                            "still in progress.";
    send_message->length = strlen(send_message->payload);
    return;
  }

  // Initialize memory for the batch context
  batch_context->select_ops = _internal_malloc(
      INIT_NUM_OPS_IN_BATCH_CONTEXT * sizeof(DbOperator *), send_message);
  if (batch_context->select_ops == NULL) {
    return;
  }
  batch_context->agg_ops = _internal_malloc(
      INIT_NUM_OPS_IN_BATCH_CONTEXT * sizeof(DbOperator *), send_message);
  if (batch_context->agg_ops == NULL) {
    free(batch_context->select_ops);
    batch_context->select_ops = NULL;
    return;
  }

  // Fully initialize the batch context and mark it as active
  batch_context->n_select_ops = 0;
  batch_context->select_ops_capacity = INIT_NUM_OPS_IN_BATCH_CONTEXT;
  batch_context->n_agg_ops = 0;
  batch_context->agg_ops_capacity = INIT_NUM_OPS_IN_BATCH_CONTEXT;
  batch_context->is_active = true;
}

/**
 * Conclude a batch for the client.
 *
 * This concludes the current active batch and returns a corresponding batch
 * DbOperator. This is an error if there is no active batch context.
 */
static DbOperator *_conclude_batch(Message *send_message,
                                   BatchContext *batch_context) {
  if (!batch_context->is_active) {
    send_message->status = MESSAGE_STATUS_BATCH_ERROR;
    send_message->payload = "No active batch to execute.";
    send_message->length = strlen(send_message->payload);
    return NULL;
  }

  // Construct the batch DbOperator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), send_message);
  if (dbo == NULL) {
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_BATCH;
  dbo->fields.batch.select_ops = (void **)batch_context->select_ops;
  dbo->fields.batch.n_select_ops = batch_context->n_select_ops;
  dbo->fields.batch.agg_ops = (void **)batch_context->agg_ops;
  dbo->fields.batch.n_agg_ops = batch_context->n_agg_ops;
  dbo->fields.batch.flags = batch_context->flags;
  dbo->fields.batch.shared_valvec_handle = batch_context->shared_valvec_handle;
  dbo->fields.batch.shared_posvec_handle = batch_context->shared_posvec_handle;

  // Reset the batch context; note that there is no need to free the internal
  // memory because it has been transferred to the batch operator
  reset_batch_context(batch_context);

  return dbo;
}

/**
 * Parse the arguments of an add/sub command into a DbOperator.
 */
static DbOperator *parse_addsub(ParserContext *ctx) {
  _ERROR_IF_BATCHING;

  _TOKENIZE_ARGS;
  _NEXT_TOKEN(valvec1);
  _NEXT_TOKEN(valvec2);
  _EXPECT_NO_MORE_TOKENS;

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_ADDSUB;
  dbo->fields.addsub.is_add = ctx->flag == 0; // 0 for add, 1 for sub

  // Look up the first generalized value vector
  GeneralizedValvecHandle *valvec_handle1 =
      lookup_valvec_handle(ctx->context, valvec1, true);
  _THROW_PARSE_ERROR_IF(valvec_handle1 == NULL, VALVEC_ERROR);
  dbo->fields.addsub.valvec_handle1 = valvec_handle1;

  // Look up the second generalized value vector
  GeneralizedValvecHandle *valvec_handle2 =
      lookup_valvec_handle(ctx->context, valvec2, true);
  _THROW_PARSE_ERROR_IF(valvec_handle2 == NULL, VALVEC_ERROR);
  dbo->fields.addsub.valvec_handle2 = valvec_handle2;

  // Check that the value vectors have the same length
  _THROW_PARSE_ERROR_IF(valvec_handle1->generalized_valvec.valvec_length !=
                            valvec_handle2->generalized_valvec.valvec_length,
                        "The value vectors must have the same length.");

  _SET_HANDLE_NAME(addsub.out, ctx->handle_name);
  free(to_free);
  return dbo;
}

/**
 * Parse the arguments of a aggregate command into a DbOperator.
 */
static DbOperator *parse_agg(ParserContext *ctx) {
  _TOKENIZE_ARGS;
  _NEXT_TOKEN(valvec);
  _EXPECT_NO_MORE_TOKENS;

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_AGG;
  dbo->fields.agg.agg_type = ctx->flag;

  // Look up the generalized value vector
  GeneralizedValvecHandle *valvec_handle =
      lookup_valvec_handle(ctx->context, valvec, true);
  _THROW_PARSE_ERROR_IF(valvec_handle == NULL, VALVEC_ERROR);
  dbo->fields.agg.valvec_handle = valvec_handle;

  _SET_HANDLE_NAME(agg.out, ctx->handle_name);
  free(to_free);

  BatchContext *batch_context = ctx->batch_context;
  if (!batch_context->is_active) {
    // Return the operator directly if the batch context is not active so that
    // it will be immediately scheduled for execution
    return dbo;
  }

  // Now we are having an active batch context, so we need to store the operator
  // in the batch context instead of returning for immediate execution; we first
  // check if the operator is compatible with the current batch
  if (batch_context->n_select_ops + batch_context->n_agg_ops == 0) {
    // This is the very first operator in the current batch so it is always
    // compatible
    batch_context->shared_valvec_handle = valvec_handle;
  } else if (_valvec_handles_are_equal(batch_context->shared_valvec_handle,
                                       valvec_handle)) {
    // This is not the first operator in the current batch; we must have exactly
    // the same value vector to be compatible (note that aggregate operators do
    // not care about position vectors)
  } else {
    // Except for the above cases, the operator is not compatible with the
    // current batch, so we set an error message and return NULL
    ctx->send_message->status = MESSAGE_STATUS_BATCH_ERROR;
    ctx->send_message->payload =
        "The operator is incompatible with the current "
        "batch.";
    ctx->send_message->length = strlen(ctx->send_message->payload);
    free(dbo);
    return NULL;
  }

  if (batch_context->n_agg_ops + 1 > batch_context->agg_ops_capacity) {
    // Resize the array of aggregate operators if needed
    size_t new_capacity =
        batch_context->agg_ops_capacity * EXPAND_FACTOR_BATCH_CONTEXT;
    DbOperator **new_agg_ops = _internal_realloc(
        batch_context->agg_ops, new_capacity * sizeof(DbOperator *),
        ctx->send_message);
    if (new_agg_ops == NULL) {
      free(dbo);
      return NULL;
    }
    batch_context->agg_ops = new_agg_ops;
    batch_context->agg_ops_capacity = new_capacity;
  }
  batch_context->agg_ops[batch_context->n_agg_ops++] = dbo;

  switch ((AggType)ctx->flag) {
  case AGG_TYPE_MIN:
    batch_context->flags |= SCAN_CALLBACK_MIN_FLAG;
    break;
  case AGG_TYPE_MAX:
    batch_context->flags |= SCAN_CALLBACK_MAX_FLAG;
    break;
  case AGG_TYPE_SUM:
  case AGG_TYPE_AVG:
    batch_context->flags |= SCAN_CALLBACK_SUM_FLAG;
    break;
  }
  return NULL;
}

/**
 * Parse the arguments of a create command into a DbOperator.
 */
static DbOperator *parse_create(ParserContext *ctx) {
  _ERROR_IF_BATCHING;

  _TOKENIZE_ARGS;
  _NEXT_TOKEN(first_token);

  if (strcmp(first_token, "db") == 0) {
    return _parse_create_db(ctx, tokenizer, to_free);
  } else if (strcmp(first_token, "tbl") == 0) {
    return _parse_create_tbl(ctx, tokenizer, to_free);
  } else if (strcmp(first_token, "col") == 0) {
    return _parse_create_col(ctx, tokenizer, to_free);
  } else if (strcmp(first_token, "idx") == 0) {
    return _parse_create_idx(ctx, tokenizer, to_free);
  } else {
    ctx->send_message->status = MESSAGE_STATUS_INVALID_COMMAND;
    free(to_free);
    return NULL;
  }
}

/**
 * Parse the arguments of a delete command into a DbOperator.
 */
static DbOperator *parse_delete(ParserContext *ctx) {
  _ERROR_IF_BATCHING;

  _TOKENIZE_ARGS;
  _NEXT_TOKEN(table_var);
  _NEXT_TOKEN(posvec);
  _EXPECT_NO_MORE_TOKENS;

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_DELETE;

  // Look up the table to delete from
  Table *table;
  DbSchemaStatus lookup_status = lookup_table(table_var, &table);
  _THROW_PARSE_ERROR_IF(lookup_status != DB_SCHEMA_STATUS_OK, TABLE_ERROR);
  dbo->fields.delete.table = table;

  // Look up the generalized position vector
  GeneralizedPosvecHandle *posvec_handle =
      lookup_posvec_handle(ctx->context, posvec);
  _THROW_PARSE_ERROR_IF(posvec_handle == NULL, POSVEC_ERROR);
  dbo->fields.delete.posvec_handle = posvec_handle;

  free(to_free);
  return dbo;
}

/**
 * Parse the arguments of a fetch command into a DbOperator.
 */
static DbOperator *parse_fetch(ParserContext *ctx) {
  _ERROR_IF_BATCHING;

  _TOKENIZE_ARGS;
  _NEXT_TOKEN(valvec);
  _NEXT_TOKEN(posvec);
  _EXPECT_NO_MORE_TOKENS;

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_FETCH;

  // Look up the generalized position vector
  GeneralizedPosvecHandle *posvec_handle =
      lookup_posvec_handle(ctx->context, posvec);
  _THROW_PARSE_ERROR_IF(posvec_handle == NULL, POSVEC_ERROR);
  dbo->fields.fetch.posvec_handle = posvec_handle;

  // Look up the generalized value vector
  GeneralizedValvecHandle *valvec_handle =
      lookup_valvec_handle(ctx->context, valvec, true);
  _THROW_PARSE_ERROR_IF(valvec_handle == NULL, VALVEC_ERROR);
  dbo->fields.fetch.valvec_handle = valvec_handle;

  _SET_HANDLE_NAME(fetch.out, ctx->handle_name);
  free(to_free);
  return dbo;
}

/**
 * Parse the arguments of an insert command into a DbOperator.
 */
static DbOperator *parse_insert(ParserContext *ctx) {
  _ERROR_IF_BATCHING;

  _TOKENIZE_ARGS;
  _NEXT_TOKEN(table_var);

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_INSERT;

  // Get the table to insert into
  Table *table;
  DbSchemaStatus lookup_status = lookup_table(table_var, &table);
  _THROW_PARSE_ERROR_IF(lookup_status != DB_SCHEMA_STATUS_OK, TABLE_ERROR);
  dbo->fields.insert.table = table;

  // Get the values to insert; we need to make sure that the number of values
  // matches the number of columns in the table
  size_t n_values = 0;
  char *value;
  int *values =
      _internal_malloc(table->n_cols * sizeof(int), ctx->send_message);
  if (values == NULL) {
    free(dbo);
    free(to_free);
    return NULL;
  }
  while ((value = strsep(&tokenizer, ",")) != NULL &&
         n_values < table->n_cols) {
    values[n_values++] = atoi(value); // XXX: Cannot detect conversion errors
  }
  _THROW_PARSE_ERROR_CUSTOM_CLEANUP_IF(
      n_values != table->n_cols || value != NULL,
      "The number of values must match the number of columns in the table.", {
        free(dbo);
        free(values);
        free(to_free);
      });
  dbo->fields.insert.values = values;

  free(to_free);
  return dbo;
}

static DbOperator *parse_join(ParserContext *ctx) {
  _ERROR_IF_BATCHING;

  _TOKENIZE_ARGS;
  _NEXT_TOKEN(valvec1);
  _NEXT_TOKEN(posvec1);
  _NEXT_TOKEN(valvec2);
  _NEXT_TOKEN(posvec2);
  _NEXT_TOKEN(alg);
  _EXPECT_NO_MORE_TOKENS;

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_JOIN;

  // Set the join algorithm
  if (strcmp(alg, "nested-loop") == 0) {
    dbo->fields.join.alg = JOIN_ALG_NESTED_LOOP;
  } else if (strcmp(alg, "naive-hash") == 0) {
    dbo->fields.join.alg = JOIN_ALG_NAIVE_HASH;
  } else if (strcmp(alg, "grace-hash") == 0) {
    dbo->fields.join.alg = JOIN_ALG_GRACE_HASH;
  } else if (strcmp(alg, "hash") == 0) {
    dbo->fields.join.alg = JOIN_ALG_HASH;
  } else {
    _THROW_PARSE_ERROR_IF(true, "Invalid join algorithm.");
  }

  // Look up the generalized value vectors
  GeneralizedValvecHandle *valvec_handle1 =
      lookup_valvec_handle(ctx->context, valvec1, true);
  GeneralizedValvecHandle *valvec_handle2 =
      lookup_valvec_handle(ctx->context, valvec2, true);
  _THROW_PARSE_ERROR_IF(valvec_handle1 == NULL, VALVEC_ERROR);
  _THROW_PARSE_ERROR_IF(valvec_handle2 == NULL, VALVEC_ERROR);
  dbo->fields.join.valvec_handle1 = valvec_handle1;
  dbo->fields.join.valvec_handle2 = valvec_handle2;

  // Look up the generalized position vectors
  GeneralizedPosvecHandle *posvec_handle1 =
      lookup_posvec_handle(ctx->context, posvec1);
  GeneralizedPosvecHandle *posvec_handle2 =
      lookup_posvec_handle(ctx->context, posvec2);
  _THROW_PARSE_ERROR_IF(posvec_handle1 == NULL, POSVEC_ERROR);
  _THROW_PARSE_ERROR_IF(posvec_handle2 == NULL, POSVEC_ERROR);
  dbo->fields.join.posvec_handle1 = posvec_handle1;
  dbo->fields.join.posvec_handle2 = posvec_handle2;

  // Split the handle name into two parts and set them in the operator
  char *handle_split = strchr(ctx->handle_name, ',');
  _THROW_PARSE_ERROR_IF(handle_split == NULL, "Two output handles required.");
  *handle_split = '\0';
  _SET_HANDLE_NAME(join.out1, ctx->handle_name);
  _SET_HANDLE_NAME(join.out2, handle_split + 1);

  free(to_free);
  return dbo;
}

/**
 * Parse the arguments of a print command into a DbOperator.
 */
static DbOperator *parse_print(ParserContext *ctx) {
  _ERROR_IF_BATCHING;

  _TOKENIZE_ARGS;
  _NEXT_TOKEN(first_token);

  // The first argument determines whether we are printing value vectors or
  // numeric values; we prioritize looking up as aa numeric value, just for
  // convenience because looking up a value vector will destroy the token so we
  // would have to make a copy in that case
  bool is_numval = true;
  GeneralizedValvecHandle *valvec_handle = NULL;
  NumericValueHandle *numval_handle =
      lookup_numval_handle(ctx->context, first_token);
  if (numval_handle == NULL) {
    is_numval = false;
    valvec_handle = lookup_valvec_handle(ctx->context, first_token, true);
    _THROW_PARSE_ERROR_CUSTOM_CLEANUP_IF(valvec_handle == NULL, VALVEC_ERROR,
                                         { free(to_free); });
  }

  // Construct the operator according to the type of print
  DbOperator *dbo;
  if (is_numval) {
    dbo = _parse_print_numvals(ctx, numval_handle, tokenizer, to_free);
  } else {
    dbo = _parse_print_valvecs(ctx, valvec_handle, tokenizer, to_free);
  }
  return dbo;
}

/**
 * Parse the arguments of a select command into a DbOperator.
 */
static DbOperator *parse_select(ParserContext *ctx) {
  _TOKENIZE_ARGS;
  _NEXT_TOKEN(first_token);
  _NEXT_TOKEN(second_token);
  _NEXT_TOKEN(third_token);

  char *posvec, *valvec, *lower, *upper;
  if (tokenizer == NULL) {
    // There are exactly three arguments, we do not have the position vector
    posvec = NULL;
    valvec = first_token;
    lower = second_token;
    upper = third_token;
  } else {
    // There are at least four arguments, and check we have exactly this many
    _NEXT_TOKEN(fourth_token);
    _EXPECT_NO_MORE_TOKENS;

    posvec = first_token;
    valvec = second_token;
    lower = third_token;
    upper = fourth_token;
  }

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_SELECT;

  // Parse the lower bound
  long lower_bound = _parse_range_bound(ctx, lower, true);
  if (ctx->send_message->status != MESSAGE_STATUS_OK) {
    free(dbo);
    free(to_free);
    return NULL;
  }
  dbo->fields.select.lower_bound = lower_bound;

  // Parse the upper bound
  long upper_bound = _parse_range_bound(ctx, upper, false);
  if (ctx->send_message->status != MESSAGE_STATUS_OK) {
    free(dbo);
    free(to_free);
    return NULL;
  }
  dbo->fields.select.upper_bound = upper_bound;

  // Look up the generalized position vector or set it to NULL
  GeneralizedPosvecHandle *posvec_handle = NULL;
  if (posvec != NULL) {
    posvec_handle = lookup_posvec_handle(ctx->context, posvec);
    _THROW_PARSE_ERROR_IF(posvec_handle == NULL, POSVEC_ERROR);
  }
  dbo->fields.select.posvec_handle = posvec_handle;

  // Look up the generalized value vector
  GeneralizedValvecHandle *valvec_handle =
      lookup_valvec_handle(ctx->context, valvec, true);
  _THROW_PARSE_ERROR_IF(valvec_handle == NULL, VALVEC_ERROR);
  dbo->fields.select.valvec_handle = valvec_handle;

  _SET_HANDLE_NAME(select.out, ctx->handle_name);
  free(to_free);

  BatchContext *batch_context = ctx->batch_context;
  if (!batch_context->is_active) {
    // Return the operator directly if the batch context is not active so that
    // it will be immediately scheduled for execution
    return dbo;
  }

  // Now we are having an active batch context, so we need to store the operator
  // in the batch context instead of returning for immediate execution; we first
  // check if the operator is compatible with the current batch
  if (batch_context->n_select_ops + batch_context->n_agg_ops == 0) {
    // This is the very first operator in the current batch so it is always
    // compatible
    batch_context->shared_posvec_handle = posvec_handle;
    batch_context->shared_valvec_handle = valvec_handle;
  } else if (batch_context->n_select_ops == 0 && batch_context->n_agg_ops > 0 &&
             _valvec_handles_are_equal(batch_context->shared_valvec_handle,
                                       valvec_handle)) {
    // This is the very first select operator in the current batch but there are
    // already aggregate operators, so it is compatible as long as the value
    // vector matches (since aggregate operators do not use position vectors)
    batch_context->shared_posvec_handle = posvec_handle;
  } else if (batch_context->n_select_ops > 0 &&
             batch_context->shared_posvec_handle == posvec_handle &&
             _valvec_handles_are_equal(batch_context->shared_valvec_handle,
                                       valvec_handle)) {
    // This is not the first select operator in the current batch; no matter
    // whether there are aggregate operators or not, we must have exactly the
    // same position and value vectors to be compatible
  } else {
    // Except for the above cases, the operator is not compatible with the
    // current batch, so we set an error message and return NULL
    ctx->send_message->status = MESSAGE_STATUS_BATCH_ERROR;
    ctx->send_message->payload =
        "The operator is incompatible with the current "
        "batch.";
    ctx->send_message->length = strlen(ctx->send_message->payload);
    free(dbo);
    return NULL;
  }

  if (batch_context->n_select_ops + 1 > batch_context->select_ops_capacity) {
    // Resize the array of select operators if needed
    size_t new_capacity =
        batch_context->select_ops_capacity * EXPAND_FACTOR_BATCH_CONTEXT;
    DbOperator **new_select_ops = _internal_realloc(
        batch_context->select_ops, new_capacity * sizeof(DbOperator *),
        ctx->send_message);
    if (new_select_ops == NULL) {
      free(dbo);
      return NULL;
    }
    batch_context->select_ops = new_select_ops;
    batch_context->select_ops_capacity = new_capacity;
  }
  batch_context->select_ops[batch_context->n_select_ops++] = dbo;
  batch_context->flags |= SCAN_CALLBACK_SELECT_FLAG;
  return NULL;
}

/**
 * Parse the arguments of an update command into a DbOperator.
 */
static DbOperator *parse_update(ParserContext *ctx) {
  _ERROR_IF_BATCHING;

  _TOKENIZE_ARGS;
  _NEXT_TOKEN(col_var);
  _NEXT_TOKEN(posvec);
  _NEXT_TOKEN(value_str);
  _EXPECT_NO_MORE_TOKENS;

  // Initialize the operator
  DbOperator *dbo = _internal_malloc(sizeof(DbOperator), ctx->send_message);
  if (dbo == NULL) {
    free(to_free);
    return NULL;
  }
  dbo->type = OPERATOR_TYPE_UPDATE;

  // Look up the table to update
  Table *table;
  size_t ith_column;
  DbSchemaStatus lookup_status = lookup_column(col_var, &table, &ith_column);
  _THROW_PARSE_ERROR_IF(lookup_status != DB_SCHEMA_STATUS_OK, COLUMN_ERROR);
  dbo->fields.update.table = table;
  dbo->fields.update.ith_column = ith_column;

  // Look up the generalized position vector
  GeneralizedPosvecHandle *posvec_handle =
      lookup_posvec_handle(ctx->context, posvec);
  _THROW_PARSE_ERROR_IF(posvec_handle == NULL, POSVEC_ERROR);
  dbo->fields.update.posvec_handle = posvec_handle;

  // Set the value to update to; XXX: cannot detect conversion errors
  dbo->fields.update.value = atoi(value_str);

  free(to_free);
  return dbo;
}

/**
 * The list of command handlers for parsing.
 *
 * Special commands like shutdown, load, single-core/multi-core switch, and
 * batch execution are either handled directly by the server or here with a
 * special routine, so they are not included in this list.
 */
static const CommandHandler commands[] = {
    {parse_addsub, "add(", 4, 0}, /* Addsub */
    {parse_addsub, "sub(", 4, 1},
    {parse_agg, "min(", 4, AGG_TYPE_MIN}, /* Agg    */
    {parse_agg, "max(", 4, AGG_TYPE_MAX},
    {parse_agg, "sum(", 4, AGG_TYPE_SUM},
    {parse_agg, "avg(", 4, AGG_TYPE_AVG},
    {parse_create, "create(", 7, 0},             /* Create */
    {parse_delete, "relational_delete(", 18, 0}, /* Delete */
    {parse_fetch, "fetch(", 6, 0},               /* Fetch  */
    {parse_insert, "relational_insert(", 18, 0}, /* Insert */
    {parse_join, "join(", 5, 0},                 /* Join   */
    {parse_print, "print(", 6, 0},               /* Print  */
    {parse_select, "select(", 7, 0},             /* Select */
    {parse_update, "relational_update(", 18, 0}, /* Update */
};

/**
 * @implements parse_command
 */
DbOperator *parse_command(char *query_command, Message *send_message,
                          int client_socket, ClientContext *context,
                          BatchContext *batch_context) {
  // Check for the leading "--" to see if this is a comment line
  if (strncmp(query_command, "--", 2) == 0) {
    send_message->status = MESSAGE_STATUS_OK;
    return NULL;
  }

  // Trim all whitespaces from the query command using fast and slow pointers
  int slow_ptr = 0;
  for (int fast_ptr = 0; query_command[fast_ptr] != '\0'; fast_ptr++) {
    if (!isspace(query_command[fast_ptr])) {
      query_command[slow_ptr++] = query_command[fast_ptr];
    }
  }
  query_command[slow_ptr] = '\0';

  log_file(stdout, "QUERY: `%s`\n", query_command);

  // Check if there is an equals sign in the command; if so we are assigning
  // the result to the variable pool and the query is the part after that
  char *equals_pointer = strchr(query_command, '=');
  char *handle_name = NULL;
  if (equals_pointer != NULL) {
    handle_name = query_command;
    *equals_pointer = '\0';
    query_command = equals_pointer + 1;
    log_file(stdout, "  [LOG] Handle name: `%s`\n", handle_name);
  }

  // From now on, all commands would require the pair of parentheses so we can
  // first check for the closing parenthesis here; also when matching the
  // commands we can include the opening parenthesis for simplicity
  size_t query_length = strlen(query_command);
  if (query_command[query_length - 1] != ')') {
    send_message->status = MESSAGE_STATUS_INVALID_COMMAND;
    log_file(stdout, "  [ERR] Failed to construct DbOperator [CODE%d]\n",
             MESSAGE_STATUS_INVALID_COMMAND);
    return NULL;
  }
  query_command[query_length - 1] = '\0';

  // Parse the command in different routines based on the prefix of the command;
  // message status is initialized to OK but may be changed in the routines if
  // there is an error
  send_message->status = MESSAGE_STATUS_OK;

  ParserContext ctx = {
      .args = NULL, // Initialize
      .send_message = send_message,
      .context = context,
      .batch_context = batch_context,
      .handle_name = handle_name,
      .flag = 0, // Initialize
  };

  // Find the command handler for the query command
  DbOperator *dbo = NULL;
  bool found = false;
  for (size_t i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
    if (strncmp(query_command, commands[i].prefix, commands[i].length) == 0) {
      found = true;
      ctx.args = query_command + commands[i].length;
      ctx.flag = commands[i].flag;
      dbo = commands[i].parser(&ctx);
      break;
    }
  }

  // If not found in the list, we check for special commands
  if (!found) {
    if (strncmp(query_command, "batch_queries(", 14) == 0) {
      _activate_batch(send_message, batch_context);
    } else if (strncmp(query_command, "batch_execute(", 14) == 0) {
      dbo = _conclude_batch(send_message, batch_context);
    } else {
      send_message->status = MESSAGE_STATUS_INVALID_COMMAND;
    }
  }

  // The status being changed indicates error in command parsing
  if (send_message->status != MESSAGE_STATUS_OK) {
    log_file(stdout, "  [ERR] Failed to construct DbOperator [CODE%d]\n",
             send_message->status);
    return NULL;
  }

  // Set the client file descriptor and context in the operator if it is not
  // NULL; note that NULL can happen in the case of ongoing command batching
  if (dbo != NULL) {
    dbo->client_fd = client_socket;
    dbo->context = context;
  }
  return dbo;
}

/**
 * @implements reset_batch_context
 */
void reset_batch_context(BatchContext *batch_context) {
  batch_context->is_active = false;
  batch_context->select_ops = NULL;
  batch_context->n_select_ops = 0;
  batch_context->select_ops_capacity = 0;
  batch_context->agg_ops = NULL;
  batch_context->n_agg_ops = 0;
  batch_context->agg_ops_capacity = 0;
  batch_context->flags = 0x00;
  batch_context->shared_valvec_handle = NULL;
  batch_context->shared_posvec_handle = NULL;
}
