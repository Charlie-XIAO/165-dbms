/**
 * @file db_operator.c
 * @implements db_operator.h
 */

#include <assert.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cmdaddsub.h"
#include "cmdagg.h"
#include "cmdbatch.h"
#include "cmdcreate.h"
#include "cmddelete.h"
#include "cmdfetch.h"
#include "cmdinsert.h"
#include "cmdjoin.h"
#include "cmdload.h"
#include "cmdprint.h"
#include "cmdselect.h"
#include "cmdupdate.h"
#include "db_operator.h"
#include "logging.h"

/**
 * Helper function to free a value vector handle if it wraps a column.
 *
 * This function is used to free a generalized value vector handle if it
 * internally wraps a column. This is because such handles are only for
 * temporary use. The wrapped column belongs to the database and will be freed
 * upon database destruction, but such a handle itself does not belong to either
 * the client context or the database, so it should be freed manually somewhere.
 */
static inline void
_free_if_wraps_column(GeneralizedValvecHandle *valvec_handle) {
  if (valvec_handle->generalized_valvec.valvec_type ==
      GENERALIZED_VALVEC_TYPE_COLUMN) {
    free(valvec_handle);
  }
}

/**
 * Free the operators wrapped in a batch operator.
 */
static inline void _free_batch_ops(BatchOperatorFields *op) {
  for (size_t i = 0; i < op->n_select_ops; i++) {
    free(op->select_ops[i]);
  }
  for (size_t i = 0; i < op->n_agg_ops; i++) {
    free(op->agg_ops[i]);
  }
  free(op->select_ops);
  free(op->agg_ops);
}

/**
 * Execute an add/sub DbOperator.
 */
static inline void execute_addsub(DbOperator *query, Message *send_message) {
  AddsubOperatorFields op = query->fields.addsub;
  if (*op.out == '\0') {
    return; // Not grabbing the result, so no need to execute
  }

  // Perform the addition or subtraction and get the resulting value vector
  DbSchemaStatus status;
  GeneralizedValvec *result =
      cmdaddsub(&op.valvec_handle1->generalized_valvec,
                &op.valvec_handle2->generalized_valvec, op.is_add, &status);
  // Note that the possible freeing must be done here before the insertion
  // because what we are inserting and what we are freeing are the same type of
  // handles (i.e., value vector handles) while insertion may reallocate memory
  // that leads to invalid read of the handles stored in the operator
  _free_if_wraps_column(op.valvec_handle1);
  _free_if_wraps_column(op.valvec_handle2);

  // Insert the value vector into the client context
  DbSchemaStatus insert_status =
      insert_valvec_handle(query->context, op.out, result);
  if (insert_status != DB_SCHEMA_STATUS_OK) {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(insert_status);
    send_message->length = strlen(send_message->payload);
    return;
  }
}

/**
 * Execute an aggregate DbOperator.
 */
static inline void execute_agg(DbOperator *query, Message *send_message) {
  AggOperatorFields op = query->fields.agg;
  if (*op.out == '\0') {
    return; // Not grabbing the result, so no need to execute
  }

  // Determine the type code and result type of the aggregation; note that what
  // the operator carries is the type of the aggregation operation, while the
  // result type is used for distinguishing how to interpret the result
  int type_code;
  NumericValueType numval_type;
  switch (op.agg_type) {
  case AGG_TYPE_MIN:
    type_code = 0;
    numval_type = NUMERIC_VALUE_TYPE_INT;
    break;
  case AGG_TYPE_MAX:
    type_code = 1;
    numval_type = NUMERIC_VALUE_TYPE_INT;
    break;
  case AGG_TYPE_SUM:
    type_code = 2;
    numval_type = NUMERIC_VALUE_TYPE_LONG_LONG;
    break;
  case AGG_TYPE_AVG:
    type_code = 3;
    numval_type = NUMERIC_VALUE_TYPE_DOUBLE;
    break;
  default:
    assert(0 && "Invalid aggregation type.");
  }

  // Aggregate and get the resulting numeric value (and position vector, if
  // applicable)
  DbSchemaStatus agg_status;
  NumericValue result =
      cmdagg(&op.valvec_handle->generalized_valvec, type_code, &agg_status);
  _free_if_wraps_column(op.valvec_handle);

  // Insert the numeric value into the client context
  DbSchemaStatus insert_status =
      insert_numval_handle(query->context, op.out, numval_type, result);
  if (insert_status != DB_SCHEMA_STATUS_OK) {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(insert_status);
    send_message->length = strlen(send_message->payload);
    return;
  }
}

/**
 * Execute a batch DbOperator.
 */
static inline void execute_batch(DbOperator *query, Message *send_message) {
  BatchOperatorFields op = query->fields.batch;
  if (op.n_select_ops + op.n_agg_ops == 0) {
    _free_batch_ops(&op);
    return; // Empty batch, no need to execute
  }
  DbOperator **select_ops = (DbOperator **)op.select_ops;
  DbOperator **agg_ops = (DbOperator **)op.agg_ops;
  size_t valvec_length =
      op.shared_valvec_handle->generalized_valvec.valvec_length;

  // Prepare memory to hold the results
  int min_result = INT_MIN;
  int max_result = INT_MAX;
  long long sum_result = 0;
  double avg_result = 0.0;
  GeneralizedPosvec **select_results = NULL;
  if (op.n_select_ops > 0) {
    select_results = malloc(op.n_select_ops * sizeof(GeneralizedPosvec *));
    if (select_results == NULL) {
      send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
      send_message->payload = "Failed to allocate memory.";
      send_message->length = strlen(send_message->payload);
      _free_batch_ops(&op);
      return;
    }
  }

  // Extract the array of lower bounds and upper bounds for shared select
  long *lower_bound_arr = NULL;
  long *upper_bound_arr = NULL;
  if (op.n_select_ops > 0) {
    lower_bound_arr = malloc(op.n_select_ops * sizeof(long));
    upper_bound_arr = malloc(op.n_select_ops * sizeof(long));
    if (lower_bound_arr == NULL || upper_bound_arr == NULL) {
      send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
      send_message->payload = "Failed to allocate memory.";
      send_message->length = strlen(send_message->payload);
      free(select_results);
      free(lower_bound_arr);
      free(upper_bound_arr);
      _free_batch_ops(&op);
      return;
    }
    for (size_t i = 0; i < op.n_select_ops; i++) {
      lower_bound_arr[i] = select_ops[i]->fields.select.lower_bound;
      upper_bound_arr[i] = select_ops[i]->fields.select.upper_bound;
    }
  }

  // Dispatch the batch of queries
  DbSchemaStatus status =
      cmdbatch(&op.shared_valvec_handle->generalized_valvec,
               op.shared_posvec_handle == NULL
                   ? NULL
                   : &op.shared_posvec_handle->generalized_posvec,
               op.n_select_ops, lower_bound_arr, upper_bound_arr, op.flags,
               select_results, &min_result, &max_result, &sum_result);

  for (size_t i = 0; i < op.n_select_ops; i++) {
    _free_if_wraps_column(select_ops[i]->fields.select.valvec_handle);
  }
  for (size_t i = 0; i < op.n_agg_ops; i++) {
    _free_if_wraps_column(agg_ops[i]->fields.agg.valvec_handle);
  }

  if (status != DB_SCHEMA_STATUS_OK) {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(status);
    send_message->length = strlen(send_message->payload);
    free(lower_bound_arr);
    free(upper_bound_arr);
    free(select_results);
    _free_batch_ops(&op);
    return;
  }
  free(lower_bound_arr);
  free(upper_bound_arr);

  // Insert the select results into the client context
  DbSchemaStatus insert_status;
  for (size_t i = 0; i < op.n_select_ops; i++) {
    insert_status = insert_posvec_handle(
        query->context, select_ops[i]->fields.select.out, select_results[i]);
    if (insert_status != DB_SCHEMA_STATUS_OK) {
      send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
      send_message->payload = format_status(insert_status);
      send_message->length = strlen(send_message->payload);
      free(select_results);
      _free_batch_ops(&op);
      return;
    }
  }
  free(select_results);

  // Insert the aggregation results into the client context
  if (op.flags & SCAN_CALLBACK_SUM_FLAG) {
    // NaN (when length is 0) should be treated as zero
    avg_result = valvec_length == 0 ? 0.0 : (double)sum_result / valvec_length;
  }
  for (size_t i = 0; i < op.n_agg_ops; i++) {
    switch (agg_ops[i]->fields.agg.agg_type) {
    case AGG_TYPE_MIN:
      insert_status = insert_numval_handle(
          query->context, agg_ops[i]->fields.agg.out, NUMERIC_VALUE_TYPE_INT,
          (NumericValue){.int_value = min_result});
      break;
    case AGG_TYPE_MAX:
      insert_status = insert_numval_handle(
          query->context, agg_ops[i]->fields.agg.out, NUMERIC_VALUE_TYPE_INT,
          (NumericValue){.int_value = max_result});
      break;
    case AGG_TYPE_SUM:
      insert_status =
          insert_numval_handle(query->context, agg_ops[i]->fields.agg.out,
                               NUMERIC_VALUE_TYPE_LONG_LONG,
                               (NumericValue){.long_long_value = sum_result});
      break;
    case AGG_TYPE_AVG:
      insert_status = insert_numval_handle(
          query->context, agg_ops[i]->fields.agg.out, NUMERIC_VALUE_TYPE_DOUBLE,
          (NumericValue){.double_value = avg_result});
      break;
    }
    if (insert_status != DB_SCHEMA_STATUS_OK) {
      send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
      send_message->payload = format_status(insert_status);
      send_message->length = strlen(send_message->payload);
      _free_batch_ops(&op);
      return;
    }
  }

  _free_batch_ops(&op);
}

/**
 * Execute a create DbOperator.
 */
static inline void execute_create(DbOperator *query, Message *send_message) {
  DbSchemaStatus status;

  switch (query->fields.create.create_type) {
  case CREATE_TYPE_DB:
    status = cmdcreate_db(query->fields.create.spec.db.name);
    if (status == DB_SCHEMA_STATUS_OK) {
      log_file(stdout, "  [OK] Database created.\n");
    } else {
      send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
      send_message->payload = format_status(status);
      send_message->length = strlen(send_message->payload);
      log_file(stdout, "  [ERR] %s\n", send_message->payload);
    }
    return;
  case CREATE_TYPE_TABLE:
    status = cmdcreate_tbl(query->fields.create.spec.tbl.db,
                           query->fields.create.spec.tbl.name,
                           query->fields.create.spec.tbl.n_cols);
    if (status == DB_SCHEMA_STATUS_OK) {
      log_file(stdout, "  [OK] Table created.\n");
    } else {
      send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
      send_message->payload = format_status(status);
      send_message->length = strlen(send_message->payload);
      log_file(stdout, "  [ERR] %s\n", send_message->payload);
    }
    return;
  case CREATE_TYPE_COLUMN:
    status = cmdcreate_col(query->fields.create.spec.col.table,
                           query->fields.create.spec.col.name);
    if (status == DB_SCHEMA_STATUS_OK) {
      log_file(stdout, "  [OK] Column created.\n");
    } else {
      send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
      send_message->payload = format_status(status);
      send_message->length = strlen(send_message->payload);
      log_file(stdout, "  [ERR] %s\n", send_message->payload);
    }
    return;
  case CREATE_TYPE_INDEX:
    status = cmdcreate_idx(query->fields.create.spec.idx.table,
                           query->fields.create.spec.idx.ith_column,
                           query->fields.create.spec.idx.index_type);
    if (status == DB_SCHEMA_STATUS_OK) {
      log_file(stdout, "  [OK] Index created.\n");
    } else {
      send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
      send_message->payload = format_status(status);
      send_message->length = strlen(send_message->payload);
      log_file(stdout, "  [ERR] %s\n", send_message->payload);
    }
    return;
  }
}

/**
 * Execute a delete DbOperator.
 */
static inline void execute_delete(DbOperator *query, Message *send_message) {
  DbSchemaStatus status =
      cmddelete(query->fields.delete.table,
                &query->fields.delete.posvec_handle->generalized_posvec);
  if (status == DB_SCHEMA_STATUS_OK) {
    log_file(stdout, "  [OK] Rows deleted.\n");
  } else {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(status);
    send_message->length = strlen(send_message->payload);
    log_file(stdout, "  [ERR] %s\n", send_message->payload);
  }
}

/**
 * Execute a fetch DbOperator.
 */
static inline void execute_fetch(DbOperator *query, Message *send_message) {
  FetchOperatorFields op = query->fields.fetch;
  if (*op.out == '\0') {
    return; // Not grabbing the result, so no need to execute
  }

  // Fetch and get the resulting value vector
  DbSchemaStatus fetch_status;
  GeneralizedValvec *valvec =
      cmdfetch(&op.valvec_handle->generalized_valvec,
               &op.posvec_handle->generalized_posvec, &fetch_status);
  if (valvec == NULL) {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(fetch_status);
    send_message->length = strlen(send_message->payload);
    return;
  }
  _free_if_wraps_column(op.valvec_handle);

  // Insert the value vector into the client context
  DbSchemaStatus insert_status =
      insert_valvec_handle(query->context, op.out, valvec);
  if (insert_status != DB_SCHEMA_STATUS_OK) {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(insert_status);
    send_message->length = strlen(send_message->payload);
    return;
  }
}

/**
 * Execute an insert DbOperator.
 */
static inline void execute_insert(DbOperator *query, Message *send_message) {
  DbSchemaStatus status =
      cmdinsert(query->fields.insert.table, query->fields.insert.values);
  if (status == DB_SCHEMA_STATUS_OK) {
    log_file(stdout, "  [OK] Row inserted.\n");
  } else {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(status);
    send_message->length = strlen(send_message->payload);
    log_file(stdout, "  [ERR] %s\n", send_message->payload);
  }
  free(query->fields.insert.values);
}

/**
 * Execute a join DbOperator.
 */
static inline void execute_join(DbOperator *query, Message *send_message) {
  DbSchemaStatus status;
  GeneralizedPosvec *posvec1;
  GeneralizedPosvec *posvec2;

  switch (query->fields.join.alg) {
  case JOIN_ALG_NESTED_LOOP:
    status = cmdjoin_nested_loop(
        &query->fields.join.valvec_handle1->generalized_valvec,
        &query->fields.join.valvec_handle2->generalized_valvec,
        &query->fields.join.posvec_handle1->generalized_posvec,
        &query->fields.join.posvec_handle2->generalized_posvec, &posvec1,
        &posvec2);
    break;
  case JOIN_ALG_NAIVE_HASH:
    status = cmdjoin_naive_hash(
        &query->fields.join.valvec_handle1->generalized_valvec,
        &query->fields.join.valvec_handle2->generalized_valvec,
        &query->fields.join.posvec_handle1->generalized_posvec,
        &query->fields.join.posvec_handle2->generalized_posvec, &posvec1,
        &posvec2);
    break;
  case JOIN_ALG_GRACE_HASH:
    status = cmdjoin_grace_hash(
        &query->fields.join.valvec_handle1->generalized_valvec,
        &query->fields.join.valvec_handle2->generalized_valvec,
        &query->fields.join.posvec_handle1->generalized_posvec,
        &query->fields.join.posvec_handle2->generalized_posvec, &posvec1,
        &posvec2);
    break;
  case JOIN_ALG_HASH:
    status =
        cmdjoin_hash(&query->fields.join.valvec_handle1->generalized_valvec,
                     &query->fields.join.valvec_handle2->generalized_valvec,
                     &query->fields.join.posvec_handle1->generalized_posvec,
                     &query->fields.join.posvec_handle2->generalized_posvec,
                     &posvec1, &posvec2);
    break;
  default:
    assert(0 && "Invalid join algorithm.");
  }

  if (status != DB_SCHEMA_STATUS_OK) {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(status);
    send_message->length = strlen(send_message->payload);
    return;
  }

  // Insert the position vectors into the client context
  DbSchemaStatus insert_status1 =
      insert_posvec_handle(query->context, query->fields.join.out1, posvec1);
  DbSchemaStatus insert_status2 =
      insert_posvec_handle(query->context, query->fields.join.out2, posvec2);
  if (insert_status1 != DB_SCHEMA_STATUS_OK ||
      insert_status2 != DB_SCHEMA_STATUS_OK) {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload =
        format_status(insert_status1 != DB_SCHEMA_STATUS_OK ? insert_status1
                                                            : insert_status2);
    send_message->length = strlen(send_message->payload);
    return;
  }

  _free_if_wraps_column(query->fields.join.valvec_handle1);
  _free_if_wraps_column(query->fields.join.valvec_handle2);
}

/**
 * Execute a load DbOperator.
 *
 * Note that no freeing is done here as the load command does not follow the
 * normal routine and the freeing will be directly managed by the server.
 */
static inline void execute_load(DbOperator *query, Message *send_message) {
  DbSchemaStatus status =
      cmdload_rows(query->fields.load.table, query->fields.load.data,
                   query->fields.load.n_rows);
  if (status == DB_SCHEMA_STATUS_OK) {
    log_file(stdout, "  [OK] %zu rows of CSV data loaded.\n",
             query->fields.load.n_rows);
  } else {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(status);
    send_message->length = strlen(send_message->payload);
    log_file(stdout, "  [ERR] %s\n", send_message->payload);
  }
}

/**
 * Execute a print DbOperator.
 */
static inline void execute_print(DbOperator *query, Message *send_message) {
  DbSchemaStatus status;
  size_t output_len;
  char *output;

  if (query->fields.print.is_numval) {
    output = cmdprint_vals(query->fields.print.numval_handles,
                           query->fields.print.n_handles, &output_len, &status);
    free(query->fields.print.numval_handles);
  } else {
    output = cmdprint_vecs(query->fields.print.valvec_handles,
                           query->fields.print.n_handles, &output_len, &status);
    for (size_t i = 0; i < query->fields.print.n_handles; i++) {
      _free_if_wraps_column(query->fields.print.valvec_handles[i]);
    }
    free(query->fields.print.valvec_handles);
  }

  if (status == DB_SCHEMA_STATUS_OK) {
    send_message->status = MESSAGE_STATUS_OK;
    send_message->payload = output;
    send_message->length = output_len;
    send_message->is_malloced = true;
    log_file(stdout, "  [OK] Formatted print output.\n", output);
  } else {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(status);
    send_message->length = strlen(send_message->payload);
    log_file(stdout, "  [ERR] %s\n", send_message->payload);
  }
}

/**
 * Execute a select DbOperator.
 */
static inline void execute_select(DbOperator *query, Message *send_message) {
  SelectOperatorFields op = query->fields.select;
  if (*op.out == '\0') {
    return; // Not grabbing the result, so no need to execute
  }

  // Select and get the resulting position vector
  DbSchemaStatus select_status;
  GeneralizedPosvec *posvec;
  if (op.valvec_handle->generalized_valvec.valvec_type ==
          GENERALIZED_VALVEC_TYPE_COLUMN &&
      op.valvec_handle->generalized_valvec.valvec_pointer.column->index_type !=
          COLUMN_INDEX_TYPE_NONE) {
    posvec = cmdselect_index(
        op.valvec_handle->generalized_valvec.valvec_pointer.column,
        op.valvec_handle->generalized_valvec.valvec_length,
        op.posvec_handle == NULL ? NULL : &op.posvec_handle->generalized_posvec,
        op.lower_bound, op.upper_bound, &select_status);
  } else {
    posvec = cmdselect_raw(
        &op.valvec_handle->generalized_valvec,
        op.posvec_handle == NULL ? NULL : &op.posvec_handle->generalized_posvec,
        op.lower_bound, op.upper_bound, &select_status);
  }
  if (posvec == NULL) {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(select_status);
    send_message->length = strlen(send_message->payload);
    return;
  }
  _free_if_wraps_column(op.valvec_handle);

  // Insert the position vector into the client context
  DbSchemaStatus insert_status =
      insert_posvec_handle(query->context, op.out, posvec);
  if (insert_status != DB_SCHEMA_STATUS_OK) {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(insert_status);
    send_message->length = strlen(send_message->payload);
    return;
  }
}

/**
 * Execute an update DbOperator.
 */
static inline void execute_update(DbOperator *query, Message *send_message) {
  DbSchemaStatus status =
      cmdupdate(query->fields.update.table, query->fields.update.ith_column,
                &query->fields.update.posvec_handle->generalized_posvec,
                query->fields.update.value);
  if (status == DB_SCHEMA_STATUS_OK) {
    log_file(stdout, "  [OK] Rows updated.\n");
  } else {
    send_message->status = MESSAGE_STATUS_EXECUTION_ERROR;
    send_message->payload = format_status(status);
    send_message->length = strlen(send_message->payload);
    log_file(stdout, "  [ERR] %s\n", send_message->payload);
  }
}

/**
 * @implements execute_db_operator
 */
void execute_db_operator(DbOperator *query, Message *send_message) {
  if (query == NULL) {
    return;
  }

  switch (query->type) {
  case OPERATOR_TYPE_ADDSUB:
    execute_addsub(query, send_message);
    break;
  case OPERATOR_TYPE_AGG:
    execute_agg(query, send_message);
    break;
  case OPERATOR_TYPE_BATCH:
    execute_batch(query, send_message);
    break;
  case OPERATOR_TYPE_CREATE:
    execute_create(query, send_message);
    break;
  case OPERATOR_TYPE_DELETE:
    execute_delete(query, send_message);
    break;
  case OPERATOR_TYPE_FETCH:
    execute_fetch(query, send_message);
    break;
  case OPERATOR_TYPE_INSERT:
    execute_insert(query, send_message);
    break;
  case OPERATOR_TYPE_JOIN:
    execute_join(query, send_message);
    break;
  case OPERATOR_TYPE_LOAD:
    execute_load(query, send_message);
    break;
  case OPERATOR_TYPE_PRINT:
    execute_print(query, send_message);
    break;
  case OPERATOR_TYPE_SELECT:
    execute_select(query, send_message);
    break;
  case OPERATOR_TYPE_UPDATE:
    execute_update(query, send_message);
    break;
  }

  // The load DbOperator is managed directly by the server; free for all others
  if (query->type != OPERATOR_TYPE_LOAD) {
    free(query);
  }
}
