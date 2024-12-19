/**
 * @file db_operator.h
 *
 * This header defines the DbOperator interface, including all types of
 * operators that can be executed in the database system.
 */

#ifndef DB_OPERATOR_H__
#define DB_OPERATOR_H__

#include <stddef.h>

#include "client_context.h"
#include "consts.h"
#include "db_schema.h"
#include "message.h"

/**
 * The enum type of the operation of the aggregation DbOperator.
 */
typedef enum AggType {
  AGG_TYPE_MIN,
  AGG_TYPE_MAX,
  AGG_TYPE_SUM,
  AGG_TYPE_AVG,
} AggType;

/**
 * The enum type of the object to create with the create DbOperator.
 */
typedef enum CreateType {
  CREATE_TYPE_DB,
  CREATE_TYPE_TABLE,
  CREATE_TYPE_COLUMN,
  CREATE_TYPE_INDEX,
} CreateType;

/**
 * The enum type of the algorithm to use for the join DbOperator.
 */
typedef enum JoinAlg {
  JOIN_ALG_NESTED_LOOP,
  JOIN_ALG_NAIVE_HASH,
  JOIN_ALG_GRACE_HASH,
  JOIN_ALG_HASH,
} JoinAlg;

/**
 * The fields of the add/sub DbOperator.
 *
 * This records the name of the output handle, the flag indicating whether it is
 * add or subtract operation, and the generalized value vectors to add/subtract.
 */
typedef struct AddsubOperatorFields {
  char out[HANDLE_MAX_SIZE];
  bool is_add;
  GeneralizedValvecHandle *valvec_handle1;
  GeneralizedValvecHandle *valvec_handle2;
} AddsubOperatorFields;

/**
 * The fields of the aggregate DbOperator.
 *
 * This records the name of the output handle, the type of the aggregation, and
 * the generalized value vector to aggregate on.
 */
typedef struct AggOperatorFields {
  char out[HANDLE_MAX_SIZE];
  AggType agg_type;
  GeneralizedValvecHandle *valvec_handle;
} AggOperatorFields;

/**
 * The fields of the batch DbOperator.
 *
 * For each type of operator that can be batched, this records the operators
 * batched and the number of them. Note that the operator pointers are void
 * pointers to avoid circular dependencies. They should be casted to the correct
 * types before use. This also records the shared scan flags to be used,
 * indicating which flags should be enabled on the shared scan for executing
 * the batched operators. The shared value vector and position vector handles
 * are recorded just for easy access - relevant information is also accessible
 * from each operator.
 */
typedef struct BatchOperatorFields {
  void **select_ops;
  size_t n_select_ops;
  void **agg_ops;
  size_t n_agg_ops;
  int flags;
  GeneralizedValvecHandle *shared_valvec_handle;
  GeneralizedPosvecHandle *shared_posvec_handle;
} BatchOperatorFields;

/**
 * The fields of the create DbOperator.
 *
 * This records the type of object to create and type-specific fields. The
 * fields are as follows:
 *
 * - Database: Name of the database to create.
 * - Table: Name of the table to create, the database it belongs to, and the
 *   number of columns in the table to create.
 * - Column: Name of the column to create, the table it belongs to, and the
 *   database the table belongs to.
 * - Index: The table in which to create index on its i-th column, and the type
 *   of the index to create.
 */
typedef struct CreateOperatorFields {
  CreateType create_type;
  union {
    struct {
      char name[MAX_SIZE_NAME];
    } db;
    struct {
      char name[MAX_SIZE_NAME];
      Db *db;
      size_t n_cols;
    } tbl;
    struct {
      char name[MAX_SIZE_NAME];
      Db *db;
      Table *table;
    } col;
    struct {
      Table *table;
      size_t ith_column;
      ColumnIndexType index_type;
    } idx;
  } spec;
} CreateOperatorFields;

/**
 * The fields of the delete DbOperator.
 *
 * This records the table to delete from and the generalized position vector
 * that specifies the rows to delete.
 */
typedef struct DeleteOperatorFields {
  Table *table;
  GeneralizedPosvecHandle *posvec_handle;
} DeleteOperatorFields;

/**
 * The fields of the fetch DbOperator.
 *
 * This records the name of the output handle, the generalized value vector to
 * fetch from, and the generalized position vector that specifies the positions
 * to fetch.
 */
typedef struct FetchOperatorFields {
  char out[HANDLE_MAX_SIZE];
  GeneralizedValvecHandle *valvec_handle;
  GeneralizedPosvecHandle *posvec_handle;
} FetchOperatorFields;

/**
 * The fields of the insert DbOperator.
 *
 * This records the table to insert in and the values to insert. The number of
 * values are guaranteed to match the number of columns in the table.
 */
typedef struct InsertOperatorFields {
  Table *table;
  int *values;
} InsertOperatorFields;

/**
 * The fields of the join DbOperator.
 *
 * This records the name of the output handles, the generalized value vectors to
 * join, the generalized position vectors that maps the rows to join, and the
 * internal algorithm to use for the join.
 */
typedef struct JoinOperatorFields {
  char out1[HANDLE_MAX_SIZE];
  char out2[HANDLE_MAX_SIZE];
  GeneralizedValvecHandle *valvec_handle1;
  GeneralizedValvecHandle *valvec_handle2;
  GeneralizedPosvecHandle *posvec_handle1;
  GeneralizedPosvecHandle *posvec_handle2;
  JoinAlg alg;
} JoinOperatorFields;

/**
 * The fields of the load DbOperator.
 *
 * This records the table to load into and in particular the columns in that
 * table to insert data in. `data` is 2D array of data flattened in row-major
 * order, with the first dimension corresponding to the number of rows and the
 * second dimension corresponding to the number of columns. This DbOperator will
 * be executed in multiple steps, with data and number of rows being updated
 * between steps.
 */
typedef struct LoadOperatorFields {
  Table *table;
  int *data;
  size_t n_cols;
  size_t n_rows;
} LoadOperatorFields;

/**
 * The fields of the print DbOperator.
 *
 * This records the number of handles to print, whether the handles are value
 * vectors or numeric values, and the handles themselves. Only one of the two
 * types of handles will be used, depending on the `is_numval` flag.
 */
typedef struct PrintOperatorFields {
  size_t n_handles;
  bool is_numval;
  GeneralizedValvecHandle **valvec_handles;
  NumericValueHandle **numval_handles;
} PrintOperatorFields;

/**
 * The fields of the select DbOperator.
 *
 * This records the name of the output handle, the lower and upper bounds for
 * the selection range, and the generalized value vector to select from. The
 * generalized position vector is optional. If it is not provided, then the
 * selected indices corrspond to the indices of the value vector; otherwise the
 * selected indices will be looked up from the position vector.
 */
typedef struct SelectOperatorFields {
  char out[HANDLE_MAX_SIZE];
  long lower_bound;
  long upper_bound;
  GeneralizedValvecHandle *valvec_handle;
  GeneralizedPosvecHandle *posvec_handle;
} SelectOperatorFields;

/**
 * The fields of the update DbOperator.
 *
 * This records the column to be updated (the i-th column in the given table),
 * the generalized position vector that specifies the rows to update, and the
 * value to update the selected rows of the column to.
 */
typedef struct UpdateOperatorFields {
  Table *table;
  size_t ith_column;
  GeneralizedPosvecHandle *posvec_handle;
  int value;
} UpdateOperatorFields;

/**
 * Union type of fields of all available DbOperator types.
 */
typedef union OperatorFields {
  AddsubOperatorFields addsub;
  AggOperatorFields agg;
  BatchOperatorFields batch;
  CreateOperatorFields create;
  DeleteOperatorFields delete;
  FetchOperatorFields fetch;
  InsertOperatorFields insert;
  JoinOperatorFields join;
  LoadOperatorFields load;
  PrintOperatorFields print;
  SelectOperatorFields select;
  UpdateOperatorFields update;
} OperatorFields;

/**
 * The enum type of the DbOperator.
 */
typedef enum OperatorType {
  OPERATOR_TYPE_ADDSUB,
  OPERATOR_TYPE_AGG,
  OPERATOR_TYPE_BATCH,
  OPERATOR_TYPE_CREATE,
  OPERATOR_TYPE_DELETE,
  OPERATOR_TYPE_FETCH,
  OPERATOR_TYPE_INSERT,
  OPERATOR_TYPE_JOIN,
  OPERATOR_TYPE_LOAD,
  OPERATOR_TYPE_PRINT,
  OPERATOR_TYPE_SELECT,
  OPERATOR_TYPE_UPDATE,
} OperatorType;

/**
 * The DbOperator struct.
 *
 * This records the type of the DbOperator, the fields of the operator, the file
 * descriptor of the client that this operator will return to, and the client
 * context of the operator in question. The client context should hold the local
 * results of the client in question.
 */
typedef struct DbOperator {
  OperatorType type;
  OperatorFields fields;
  int client_fd;
  ClientContext *context;
} DbOperator;

/**
 * Execute the DbOperator query and return the result.
 *
 * The message is not touched if the query is executed successfully, and the
 * execution result will be set as the payload. Otherwise, the message will be
 * set with an execution error status and the error message will be set as the
 * payload.
 *
 * This function will also free the DbOperator query after execution, as well
 * as any internal memory that are not reachable from outside the query (i.e.,
 * not belonging to the client context or the database). A special case is the
 * load DbOperator which is managed directly by the server.
 */
void execute_db_operator(DbOperator *query, Message *send_message);

#endif /* DB_OPERATOR_H__ */
