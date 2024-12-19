/**
 * @file parse.h
 *
 * This header defines the utility for parsing a client message into a
 * DbOperator query.
 */

#ifndef PARSE_H__
#define PARSE_H__

#include "db_operator.h"
#include "message.h"

/**
 * The context of batching queries.
 *
 * The `batch_queries()` and `batch_execute()` commands are wrappers around
 * multiple queries and this context is used to keep track of the state of the
 * batched queries. The query is a batch DbOperator, which is NULL if batching
 * is not started or is terminated, otherwise malloc'ed and updated in the
 * process.
 */
typedef struct BatchContext {
  bool is_active;
  DbOperator **select_ops;
  size_t n_select_ops;
  size_t select_ops_capacity;
  DbOperator **agg_ops;
  size_t n_agg_ops;
  size_t agg_ops_capacity;
  int flags;
  GeneralizedValvecHandle *shared_valvec_handle;
  GeneralizedPosvecHandle *shared_posvec_handle;
} BatchContext;

/**
 * Parse a command string into a DbOperator.
 *
 * The message is not touched if the command is successfully parsed into a valid
 * DbOperator, and that DbOperator will be returned. Otherwise, the message will
 * be set to have whatever appropriate status for special commands and parsing
 * errors with the corresponding payload as needed, and NULL will be returned.
 */
DbOperator *parse_command(char *query_command, Message *send_message,
                          int client, ClientContext *context,
                          BatchContext *batch_context);

/**
 * Set or reset a batch context to initial state.
 *
 * Note that this function will not free internal memory of the batch context
 * but only reset the state, and the caller should free the memory in advance if
 * needed.
 */
void reset_batch_context(BatchContext *batch_context);

#endif /* PARSE_H__ */
