/**
 * @file join.h
 *
 * This header defines the joining functionalities.
 */

#ifndef JOIN_H__
#define JOIN_H__

#include <stddef.h>

#include "db_schema.h"

/**
 * The data for a hash join task.
 *
 * This data is used for tasks in the hash join task queue in multi-threaded
 * execution. The result arrays and the result size will be written and do not
 * need to be initialized.
 */
typedef struct HashJoinTaskData {
  int *data1;
  int *data2;
  size_t *indices1;
  size_t *indices2;
  size_t size1;
  size_t size2;
  size_t *result1;
  size_t *result2;
  size_t result_size;
} HashJoinTaskData;

/**
 * Worker subroutine for hash join.
 */
DbSchemaStatus hash_join_subroutine(HashJoinTaskData *task_data);

/**
 * The nested loop join algorithm.
 */
DbSchemaStatus join_nested_loop(int *data1, int *data2, size_t *indices1,
                                size_t *indices2, size_t size1, size_t size2,
                                size_t **out1, size_t **out2, size_t *out_size);

/**
 * The naive hash join algorithm.
 *
 * Naive hash join consists of only a build phase and a probe phase, directly
 * on the whole input data.
 */
DbSchemaStatus join_naive_hash(int *data1, int *data2, size_t *indices1,
                               size_t *indices2, size_t size1, size_t size2,
                               size_t **out1, size_t **out2, size_t *out_size);

/**
 * The radix hash join algorithm.
 *
 * Radix hash join consists of a partition phase, then build and probe phases
 * on each partition in parallel, and finally a result merge phase. The
 * partitioning is based on a certain number of least significant bits of the
 * keys.
 */
DbSchemaStatus join_radix_hash(int *data1, int *data2, size_t *indices1,
                               size_t *indices2, size_t size1, size_t size2,
                               size_t **out1, size_t **out2, size_t *out_size);

#endif /* JOIN_H__ */
