/**
 * @file cmdjoin.c
 * @implements cmdjoin.h
 */

#include <assert.h>
#include <string.h>

#include "cmdjoin.h"
#include "join.h"

/**
 * Helper macro to prepare the data for join operations.
 *
 * This extracts `datax`, `indicesx` and `sizex` from the given value and
 * position vectors, where `x=1,2`. This also declares `resultx` where `x=1,2`
 * and `count`, prepared for the join outputs.
 */
#define _PREPARE_DATA                                                          \
  int *data1 = valvec1->valvec_type == GENERALIZED_VALVEC_TYPE_COLUMN          \
                   ? valvec1->valvec_pointer.column->data                      \
                   : valvec1->valvec_pointer.partial_column->values;           \
  int *data2 = valvec2->valvec_type == GENERALIZED_VALVEC_TYPE_COLUMN          \
                   ? valvec2->valvec_pointer.column->data                      \
                   : valvec2->valvec_pointer.partial_column->values;           \
  size_t *indices1 = posvec1->posvec_pointer.index_array->indices;             \
  size_t *indices2 = posvec2->posvec_pointer.index_array->indices;             \
  size_t size1 = posvec1->posvec_pointer.index_array->n_indices;               \
  size_t size2 = posvec2->posvec_pointer.index_array->n_indices;               \
  size_t *result1, *result2, count;

/**
 * Helper function to wrap the join results into position vectors.
 */
static inline DbSchemaStatus _wrap_results(size_t *result1, size_t *result2,
                                           size_t count,
                                           GeneralizedPosvec **posvec_out1,
                                           GeneralizedPosvec **posvec_out2) {
  result1 = realloc(result1, count * sizeof(size_t));
  result2 = realloc(result2, count * sizeof(size_t));
  if (result1 == NULL || result2 == NULL) {
    free(result1);
    free(result2);
    return DB_SCHEMA_STATUS_REALLOC_FAILED;
  }

  DbSchemaStatus status;
  *posvec_out1 = wrap_index_array(result1, count, &status);
  if (status != DB_SCHEMA_STATUS_OK) {
    free(result1);
    free(result2);
    return status;
  }
  *posvec_out2 = wrap_index_array(result2, count, &status);
  if (status != DB_SCHEMA_STATUS_OK) {
    free(result1);
    free(result2);
    return status;
  }
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements cmdjoin_nested_loop
 */
DbSchemaStatus cmdjoin_nested_loop(GeneralizedValvec *valvec1,
                                   GeneralizedValvec *valvec2,
                                   GeneralizedPosvec *posvec1,
                                   GeneralizedPosvec *posvec2,
                                   GeneralizedPosvec **posvec_out1,
                                   GeneralizedPosvec **posvec_out2) {
  _PREPARE_DATA;
  DbSchemaStatus status =
      join_nested_loop(data1, data2, indices1, indices2, size1, size2, &result1,
                       &result2, &count);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }
  return _wrap_results(result1, result2, count, posvec_out1, posvec_out2);
}

/**
 * @implements cmdjoin_naive_hash
 */
DbSchemaStatus cmdjoin_naive_hash(GeneralizedValvec *valvec1,
                                  GeneralizedValvec *valvec2,
                                  GeneralizedPosvec *posvec1,
                                  GeneralizedPosvec *posvec2,
                                  GeneralizedPosvec **posvec_out1,
                                  GeneralizedPosvec **posvec_out2) {
  _PREPARE_DATA;
  DbSchemaStatus status =
      join_naive_hash(data1, data2, indices1, indices2, size1, size2, &result1,
                      &result2, &count);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }
  return _wrap_results(result1, result2, count, posvec_out1, posvec_out2);
}

/**
 * @implements cmdjoin_grace_hash
 */
DbSchemaStatus cmdjoin_grace_hash(GeneralizedValvec *valvec1,
                                  GeneralizedValvec *valvec2,
                                  GeneralizedPosvec *posvec1,
                                  GeneralizedPosvec *posvec2,
                                  GeneralizedPosvec **posvec_out1,
                                  GeneralizedPosvec **posvec_out2) {
  _PREPARE_DATA;
  DbSchemaStatus status =
      join_radix_hash(data1, data2, indices1, indices2, size1, size2, &result1,
                      &result2, &count);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }
  return _wrap_results(result1, result2, count, posvec_out1, posvec_out2);
}

/**
 * @implements cmdjoin_hash
 */
DbSchemaStatus
cmdjoin_hash(GeneralizedValvec *valvec1, GeneralizedValvec *valvec2,
             GeneralizedPosvec *posvec1, GeneralizedPosvec *posvec2,
             GeneralizedPosvec **posvec_out1, GeneralizedPosvec **posvec_out2) {
  _PREPARE_DATA;

  // Use naive-hash for small data sizes and grace-hash for large data sizes
  DbSchemaStatus status;
  size_t msize = size1 > size2 ? size1 : size2;
  if (msize < NAIVE_GRACE_JOIN_THRESHOLD) {
    status = join_naive_hash(data1, data2, indices1, indices2, size1, size2,
                             &result1, &result2, &count);
  } else {
    status = join_radix_hash(data1, data2, indices1, indices2, size1, size2,
                             &result1, &result2, &count);
  }
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }
  return _wrap_results(result1, result2, count, posvec_out1, posvec_out2);
}
