/**
 * @file client_context.h
 *
 * This header defines functionalities and interfaces related to the client
 * context, as an extension to the basic database schema.
 */

#ifndef CLIENT_CONTEXT_H__
#define CLIENT_CONTEXT_H__

#include "bitvector.h"
#include "db_schema.h"

/**
 * The partial column struct.
 *
 * This struct contains a pointer to the values. It represents partial data in a
 * column, commonly filtered by certain conditions.
 */
typedef struct PartialColumn {
  int *values;
} PartialColumn;

/**
 * The enum type of a generalized value vector.
 *
 * This enum allows us to differentiate between a column and a value vector.
 */
typedef enum GeneralizedValvecType {
  GENERALIZED_VALVEC_TYPE_COLUMN,
  GENERALIZED_VALVEC_TYPE_PARTIAL_COLUMN,
} GeneralizedValvecType;

/**
 * Union type of a generalized value vector pointer.
 *
 * This union allows us to refer to either a column or a partial column, thus
 * enabling interoperability between the two.
 */
typedef union GeneralizedValvecPointer {
  Column *column;
  PartialColumn *partial_column;
} GeneralizedValvecPointer;

/**
 * A generalized value vector.
 *
 * This struct contains the length of the value vector and its type, which
 * indicates whether the pointer should be interpreted as a column or a partial
 * column.
 */
typedef struct GeneralizedValvec {
  GeneralizedValvecType valvec_type;
  GeneralizedValvecPointer valvec_pointer;
  size_t valvec_length;
} GeneralizedValvec;

/**
 * A handle used to refer to a generalized value vector.
 *
 * This struct contains the name of the handle and the generalized value vector
 * that it refers to.
 */
typedef struct GeneralizedValvecHandle {
  char name[HANDLE_MAX_SIZE];
  GeneralizedValvec generalized_valvec;
} GeneralizedValvecHandle;

/**
 * The index array struct.
 *
 * This struct contains a pointer to the indices and the number of them. It
 * represents the matching indices according to certain filtering conditions.
 */
typedef struct IndexArray {
  size_t n_indices;
  size_t *indices;
} IndexArray;

/**
 * The boolean mask struct.
 *
 * This struct contains a pointer to a bit vector. It represents the boolean
 * mask that indicates whether a certain position satisfies certain filtering
 * conditions. It also records the number of bits set to true.
 */
typedef struct BooleanMask {
  size_t n_set;
  BitVector *mask;
} BooleanMask;

/**
 * The enum type of a generalized position vector.
 *
 * This enum allows us to differentiate between an index array and a boolean
 * mask.
 */
typedef enum GeneralizedPosvecType {
  GENERALIZED_POSVEC_TYPE_INDEX_ARRAY,
  GENERALIZED_POSVEC_TYPE_BOOLEAN_MASK,
} GeneralizedPosvecType;

/**
 * Union type of a generalized position vector pointer.
 *
 * This union allows us to refer to either an index array or a boolean mask,
 * thus enabling interoperability between the two.
 */
typedef union GeneralizedPosvecPointer {
  IndexArray *index_array;
  BooleanMask *boolean_mask;
} GeneralizedPosvecPointer;

/**
 * A generalized position vector.
 *
 * This struct contains the type of the position vector, which indicates whether
 * the pointer should be interpreted as an index array or a boolean mask.
 */
typedef struct GeneralizedPosvec {
  GeneralizedPosvecType posvec_type;
  GeneralizedPosvecPointer posvec_pointer;
} GeneralizedPosvec;

/**
 * A handle used to refer to a generalized position vector.
 *
 * This struct contains the name of the handle and the generalized position
 * vector that it refers to.
 */
typedef struct GeneralizedPosvecHandle {
  char name[HANDLE_MAX_SIZE];
  GeneralizedPosvec generalized_posvec;
} GeneralizedPosvecHandle;

/**
 * The enum type of a numeric value.
 *
 * This enum allows us to differentiate between different types of numeric
 * values, such as integers, long long integers, and doubles.
 */
typedef enum NumericValueType {
  NUMERIC_VALUE_TYPE_INT,
  NUMERIC_VALUE_TYPE_LONG_LONG,
  NUMERIC_VALUE_TYPE_DOUBLE,
} NumericValueType;

/**
 * Union type of a numeric value.
 *
 * This union allows us to refer to a numeric value as an integer, a long long
 * integer, or a double, thus enabling interoperability between the three.
 */
typedef union NumericValue {
  int int_value;
  long long long_long_value;
  double double_value;
} NumericValue;

/**
 * A handle used to refer to a numeric value.
 *
 * This struct contains the name of the handle, the type of the numeric value,
 * and the numeric value itself.
 */
typedef struct NumericValueHandle {
  char name[HANDLE_MAX_SIZE];
  NumericValueType type;
  NumericValue value;
} NumericValueHandle;

/**
 * The client context struct.
 *
 * This struct records the column handles in the current client context, the
 * number of them, and the capacity of the client context, i.e., how many column
 * handles can be stored.
 */
typedef struct ClientContext {
  GeneralizedValvecHandle *valvec_handles;
  size_t n_valvec_handles;
  size_t valvec_handles_capacity;
  GeneralizedPosvecHandle *posvec_handles;
  size_t n_posvec_handles;
  size_t posvec_handles_capacity;
  NumericValueHandle *numval_handles;
  size_t n_numval_handles;
  size_t numval_handles_capacity;
} ClientContext;

/**
 * Initialize a new client context.
 *
 * The pointer to the created client context is returned on success, and NULL is
 * returned on failure.
 */
ClientContext *init_client_context();

/**
 * Free the memory allocated for a client context.
 *
 * This not only frees the memory of the handles and the client context itself,
 * but also frees any internals that are owned by the client context.
 */
void free_client_context(ClientContext *context);

/**
 * Look up a value vector handle by name.
 *
 * This function returns the pointer to the value vector handle if found, and
 * NULL otherwise. If `consider_column` is true, the function will not only
 * search the client context for the value vector handle that matches the name,
 * but also consider `name` as a column variable and search in the database, and
 * wrap in a value vector handle if found.
 */
GeneralizedValvecHandle *lookup_valvec_handle(ClientContext *context,
                                              char *name, bool consider_column);

/**
 * Look up a position vector handle by name.
 *
 * This function returns the pointer to the position vector handle if found, and
 * NULL otherwise.
 */
GeneralizedPosvecHandle *lookup_posvec_handle(ClientContext *context,
                                              char *name);

/**
 * Look up a numeric value handle by name.
 *
 * This function returns the pointer to the numeric value handle if found, and
 * NULL otherwise.
 */
NumericValueHandle *lookup_numval_handle(ClientContext *context, char *name);

/**
 * Create and insert a value vector handle into the client context.
 *
 * This function returns the status code of the operation. It first wraps the
 * value vector into a value vector handle with the specified name, then inserts
 * the handle into the context or overwrites an existing handle with the same
 * name. Note that the value vector is freed after the handle is created and the
 * handle will take over the ownership.
 */
DbSchemaStatus insert_valvec_handle(ClientContext *context, char *name,
                                    GeneralizedValvec *valvec);

/**
 * Create and insert a position vector handle into the client context.
 *
 * This function returns the status code of the operation. It first wraps the
 * position vector into a position vector handle with the specified name, then
 * inserts the handle into the context or overwrites an existing handle with the
 * same name. Note that the position vector is freed after the handle is created
 * and the handle will take over the ownership.
 */
DbSchemaStatus insert_posvec_handle(ClientContext *context, char *name,
                                    GeneralizedPosvec *posvec);

/**
 * Create and insert a numeric value handle into the client context.
 *
 * This function returns the status code of the operation. It first creates a
 * numeric value handle with the specified name and type, then inserts the
 * handle into the context or overwrites an existing handle with the same name.
 */
DbSchemaStatus insert_numval_handle(ClientContext *context, char *name,
                                    NumericValueType type, NumericValue value);

/**
 * Wrap an index array into a generalized position vector.
 */
GeneralizedPosvec *wrap_index_array(size_t *indices, size_t n_indices,
                                    DbSchemaStatus *status);

/**
 * Wrap an array of data into a generalized value vector.
 */
GeneralizedValvec *wrap_partial_column(int *values, size_t length,
                                       DbSchemaStatus *status);

#endif /* CLIENT_CONTEXT_H__ */
