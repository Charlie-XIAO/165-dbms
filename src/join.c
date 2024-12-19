/**
 * @file join.c
 * @implements join.h
 */

#include <assert.h>
#include <string.h>

#include "join.h"
#include "logging.h"
#include "thread_pool.h"
#include "uthash.h"
#include "utlist.h"

/**
 * The number of buckets resulting from radix partitioning.
 *
 * This should be 2 to the power of the number of least significant bits used
 * for radix partitioning.
 */
static size_t RADIX_JOIN_NUM_BUCKETS;

/**
 * Index node structure for the hash table entry.
 */
typedef struct _IndexNode {
  size_t index;
  struct _IndexNode *next;
} _IndexNode;

/**
 * Hash table entry structure for hash join algorithms.
 *
 * Each key in the hash table is an integer (which is some data in the array to
 * join), and each value is a linked list of index nodes because the array to
 * join can contain duplicates.
 */
typedef struct _HashEntry {
  int key;
  struct _IndexNode *head;
  UT_hash_handle hh;
} _HashEntry;

/**
 * Helper macro to compute the radix partitioning hash function.
 *
 * This hash is based on the least significant bits of the key, in particular,
 * log2 of `RADIX_JOIN_NUM_BUCKETS` least significant bits.
 */
#define _RADIX_HASH_FUNC(key) ((key) & (RADIX_JOIN_NUM_BUCKETS - 1))

/**
 * Helper macro to initialize the result arrays.
 *
 * This will initialize the result arrays `result1` and `result2` with an
 * initial capacity `result_capacity`. It also depends on `size1` and `size2`
 * to decide the initial capacity.
 */
#define _INIT_RESULTS                                                          \
  size_t result_capacity = INIT_NUM_ELEMS_IN_JOIN_RESULT < size1 * size2       \
                               ? INIT_NUM_ELEMS_IN_JOIN_RESULT                 \
                               : size1 * size2;                                \
  size_t *result1 = malloc(result_capacity * sizeof(size_t));                  \
  size_t *result2 = malloc(result_capacity * sizeof(size_t));                  \
  if (result1 == NULL || result2 == NULL) {                                    \
    free(result1);                                                             \
    free(result2);                                                             \
    return DB_SCHEMA_STATUS_ALLOC_FAILED;                                      \
  }

/**
 * Helper function to perform radix partitioning for radix hash join.
 *
 * This function will partition the data array into `RADIX_JOIN_NUM_BUCKETS`
 * many buckets with the `RADIX_HASH_FUNC` hash function. It will allocate
 * memory for the partitioned data and indices and fill them accordingly. It
 * will also allocate memory for the histogram and prefix sum arrays, where the
 * former indicates the number of elements in each bucket and the latter
 * indicates the offsets of each bucket in the partitioned data and indices.
 */
static inline DbSchemaStatus
_radix_partition(int *data, size_t *indices, size_t size,
                 int **partitioned_data, size_t **partitioned_indices,
                 size_t **histogram, size_t **prefix_sum) {
  // Compute the histogram
  *histogram = calloc(RADIX_JOIN_NUM_BUCKETS, sizeof(size_t));
  if (*histogram == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  for (size_t i = 0; i < size; i++) {
    (*histogram)[_RADIX_HASH_FUNC(data[i])]++;
  }

  // Compute the prefix sum
  *prefix_sum = malloc(RADIX_JOIN_NUM_BUCKETS * sizeof(size_t));
  if (*prefix_sum == NULL) {
    free(*histogram);
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  size_t sum = 0;
  for (size_t i = 0; i < RADIX_JOIN_NUM_BUCKETS; i++) {
    (*prefix_sum)[i] = sum;
    sum += (*histogram)[i];
  }

  // Copy prefix sum to a temporary array for partitioning
  size_t *prefix_sum_copy = malloc(RADIX_JOIN_NUM_BUCKETS * sizeof(size_t));
  if (prefix_sum_copy == NULL) {
    free(*histogram);
    free(*prefix_sum);
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  memcpy(prefix_sum_copy, *prefix_sum, RADIX_JOIN_NUM_BUCKETS * sizeof(size_t));

  // Allocate memory for the partitioned data and indices
  *partitioned_data = malloc(size * sizeof(int));
  if (*partitioned_data == NULL) {
    free(*histogram);
    free(*prefix_sum);
    free(prefix_sum_copy);
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  *partitioned_indices = malloc(size * sizeof(size_t));
  if (*partitioned_indices == NULL) {
    free(*histogram);
    free(*prefix_sum);
    free(prefix_sum_copy);
    free(*partitioned_data);
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }

  // Perform the partitioning
  size_t bucket, offset;
  for (size_t i = 0; i < size; i++) {
    bucket = _RADIX_HASH_FUNC(data[i]);
    offset = prefix_sum_copy[bucket];
    (*partitioned_data)[offset] = data[i];
    (*partitioned_indices)[offset] = indices[i];
    prefix_sum_copy[bucket]++;
  }

  free(prefix_sum_copy);
  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to expand the result arrays if necessary.
 */
static inline DbSchemaStatus _maybe_expand_results(size_t **result1,
                                                   size_t **result2,
                                                   size_t count,
                                                   size_t *capacity) {
  if (count == *capacity) {
    *capacity *= EXPAND_FACTOR_JOIN_RESULT;
    *result1 = realloc(*result1, *capacity * sizeof(size_t));
    *result2 = realloc(*result2, *capacity * sizeof(size_t));
    if (*result1 == NULL || *result2 == NULL) {
      free(*result1);
      free(*result2);
      return DB_SCHEMA_STATUS_REALLOC_FAILED;
    }
  }
  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to free a hash table and its internal structures.
 */
static inline void _free_hash_table(_HashEntry *hash_table) {
  _HashEntry *current_entry, *tmp_entry;
  HASH_ITER(hh, hash_table, current_entry, tmp_entry) {
    _IndexNode *current_node, *tmp_node;
    LL_FOREACH_SAFE(current_entry->head, current_node, tmp_node) {
      LL_DELETE(current_entry->head, current_node);
      free(current_node);
    }
    HASH_DEL(hash_table, current_entry);
    free(current_entry);
  }
}

/**
 * Helper function to perform hash and probe for hash join.
 *
 * This will build a hash table with the smaller data and probe the hash table
 * with the larger data. It will populate the result arrays with the matching
 * indices (join results).
 */
static inline DbSchemaStatus _hash_and_probe(int *data1, int *data2,
                                             size_t *indices1, size_t *indices2,
                                             size_t size1, size_t size2,
                                             size_t **out1, size_t **out2,
                                             size_t *out_size) {
  _INIT_RESULTS;

  // Distinguish the smaller and larger data
  int *data_small, *data_large;
  size_t *indices_small, *indices_large;
  size_t size_small, size_large;
  size_t *result_small, *result_large;
  if (size1 < size2) {
    data_small = data1;
    data_large = data2;
    indices_small = indices1;
    indices_large = indices2;
    size_small = size1;
    size_large = size2;
    result_small = result1;
    result_large = result2;
  } else {
    data_small = data2;
    data_large = data1;
    indices_small = indices2;
    indices_large = indices1;
    size_small = size2;
    size_large = size1;
    result_small = result2;
    result_large = result1;
  }

  // Build phase: populate the hash table with the smaller data
  _HashEntry *hash_table = NULL, *entry;
  for (size_t i = 0; i < size_small; i++) {
    HASH_FIND_INT(hash_table, &data_small[i], entry);
    if (entry == NULL) {
      entry = malloc(sizeof(_HashEntry));
      if (entry == NULL) {
        free(result1);
        free(result2);
        return DB_SCHEMA_STATUS_ALLOC_FAILED;
      }
      entry->key = data_small[i];
      entry->head = NULL;
      HASH_ADD_INT(hash_table, key, entry);
    }
    _IndexNode *node = malloc(sizeof(_IndexNode));
    if (node == NULL) {
      free(result1);
      free(result2);
      return DB_SCHEMA_STATUS_ALLOC_FAILED;
    }
    node->index = indices_small[i];
    LL_PREPEND(entry->head, node);
  }

  // Probe phase: search from matches in the hash table using the larger data
  size_t count = 0;
  DbSchemaStatus status;
  for (size_t i = 0; i < size_large; i++) {
    HASH_FIND_INT(hash_table, &data_large[i], entry);
    if (entry != NULL) {
      _IndexNode *node;
      LL_FOREACH(entry->head, node) {
        status = _maybe_expand_results(&result_small, &result_large, count,
                                       &result_capacity);
        if (status != DB_SCHEMA_STATUS_OK) {
          free(result_small);
          free(result_large);
          _free_hash_table(hash_table);
          return status;
        }
        result_small[count] = node->index;
        result_large[count] = indices_large[i];
        count++;
      }
    }
  }
  _free_hash_table(hash_table);

  // Note that we need to distinguish the smaller and larger data again because
  // the result arrays may have been reallocated causing the original pointers
  // to change
  if (size1 < size2) {
    *out1 = result_small;
    *out2 = result_large;
  } else {
    *out1 = result_large;
    *out2 = result_small;
  }
  *out_size = count;
  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to resize the output arrays to the actual size.
 */
static inline DbSchemaStatus _resize_outputs(size_t **out1, size_t **out2,
                                             size_t out_size) {
  *out1 = realloc(*out1, out_size * sizeof(size_t));
  *out2 = realloc(*out2, out_size * sizeof(size_t));
  if (*out1 == NULL || *out2 == NULL) {
    free(*out1);
    free(*out2);
    return DB_SCHEMA_STATUS_REALLOC_FAILED;
  }
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements hash_join_subroutine
 */
DbSchemaStatus hash_join_subroutine(HashJoinTaskData *task_data) {
  return _hash_and_probe(
      task_data->data1, task_data->data2, task_data->indices1,
      task_data->indices2, task_data->size1, task_data->size2,
      &task_data->result1, &task_data->result2, &task_data->result_size);
}

/**
 * @implements join_nested_loop
 */
DbSchemaStatus join_nested_loop(int *data1, int *data2, size_t *indices1,
                                size_t *indices2, size_t size1, size_t size2,
                                size_t **out1, size_t **out2,
                                size_t *out_size) {
  _INIT_RESULTS;

  // Perform the nested loop join
  DbSchemaStatus status;
  size_t count = 0;
  for (size_t i = 0; i < size1; i++) {
    for (size_t j = 0; j < size2; j++) {
      if (data1[i] == data2[j]) {
        status =
            _maybe_expand_results(&result1, &result2, count, &result_capacity);
        if (status != DB_SCHEMA_STATUS_OK) {
          free(result1);
          free(result2);
          return status;
        }
        result1[count] = indices1[i];
        result2[count] = indices2[j];
        count++;
      }
    }
  }

  *out1 = result1;
  *out2 = result2;
  *out_size = count;
  return _resize_outputs(out1, out2, *out_size);
}

/**
 * @implements join_naive_hash
 */
DbSchemaStatus join_naive_hash(int *data1, int *data2, size_t *indices1,
                               size_t *indices2, size_t size1, size_t size2,
                               size_t **out1, size_t **out2, size_t *out_size) {
  // Build and probe: directly on the whole input data
  DbSchemaStatus status = _hash_and_probe(data1, data2, indices1, indices2,
                                          size1, size2, out1, out2, out_size);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }
  return _resize_outputs(out1, out2, *out_size);
}

/**
 * @implements join_radix_hash
 */
DbSchemaStatus join_radix_hash(int *data1, int *data2, size_t *indices1,
                               size_t *indices2, size_t size1, size_t size2,
                               size_t **out1, size_t **out2, size_t *out_size) {
  // Determine partition scheme; NOTE: this is a hardcoded heuristic
  // https://github.com/giorgospan/Radix-Hash-Join/blob/1e81f4d8e87ea542e4bf25f4292f925f7f7eb29f/final/src/Joiner.c#L150-L166
  size_t msize = size1 > size2 ? size1 : size2;
  if (msize < 500000) {
    RADIX_JOIN_NUM_BUCKETS = 16; // 4 LSB
  } else if (msize < 2000000) {
    RADIX_JOIN_NUM_BUCKETS = 32; // 5 LSB
  } else {
    RADIX_JOIN_NUM_BUCKETS = 256; // 8 LSB
  }

  // Partition phase: partition both data arrays and indices arrays with radix
  // partitioning; their respective prefix sums give the offsets and their
  // respective histograms give the sizes of the buckets
  int *partitioned_data1, *partitioned_data2;
  size_t *partitioned_indices1, *partitioned_indices2;
  size_t *histogram1, *histogram2;
  size_t *prefix_sum1, *prefix_sum2;
  DbSchemaStatus status =
      _radix_partition(data1, indices1, size1, &partitioned_data1,
                       &partitioned_indices1, &histogram1, &prefix_sum1);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }
  status = _radix_partition(data2, indices2, size2, &partitioned_data2,
                            &partitioned_indices2, &histogram2, &prefix_sum2);
  if (status != DB_SCHEMA_STATUS_OK) {
    free(partitioned_data1);
    free(partitioned_indices1);
    free(prefix_sum1);
    return status;
  }

  // Build and probe: embarrassingly parallelized on each partition
  thread_pool_reset_queue_completion(__thread_pool__);
  HashJoinTaskData task_data[RADIX_JOIN_NUM_BUCKETS];
  for (size_t i = 0; i < RADIX_JOIN_NUM_BUCKETS; i++) {
    task_data[i].data1 = partitioned_data1 + prefix_sum1[i];
    task_data[i].data2 = partitioned_data2 + prefix_sum2[i];
    task_data[i].indices1 = partitioned_indices1 + prefix_sum1[i];
    task_data[i].indices2 = partitioned_indices2 + prefix_sum2[i];
    task_data[i].size1 = histogram1[i];
    task_data[i].size2 = histogram2[i];
    task_data[i].result1 = NULL;
    task_data[i].result2 = NULL;
    task_data[i].result_size = 0;
    ThreadTask task = {.id = next_task_id(),
                       .type = THREAD_TASK_TYPE_HASH_JOIN,
                       .data = &task_data[i]};
    thread_pool_enqueue_task(__thread_pool__, &task);
    log_file(stdout, "  [LOG] Enqueued hash join task %d\n", task.id);
  }
  thread_pool_wait_queue_completion(__thread_pool__, RADIX_JOIN_NUM_BUCKETS);
  log_file(stdout, "  [LOG] Hash joins completed\n");

  // Get the size of the final result as the sum of all partition results
  size_t count = 0;
  for (size_t i = 0; i < RADIX_JOIN_NUM_BUCKETS; i++) {
    count += task_data[i].result_size;
  }
  size_t *result1 = malloc(count * sizeof(size_t));
  size_t *result2 = malloc(count * sizeof(size_t));
  if (result1 == NULL || result2 == NULL) {
    free(result1);
    free(result2);
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }

  // Merge all partition results into the final result
  size_t offset = 0;
  for (size_t i = 0; i < RADIX_JOIN_NUM_BUCKETS; i++) {
    memcpy(result1 + offset, task_data[i].result1,
           task_data[i].result_size * sizeof(size_t));
    memcpy(result2 + offset, task_data[i].result2,
           task_data[i].result_size * sizeof(size_t));
    offset += task_data[i].result_size;
    free(task_data[i].result1);
    free(task_data[i].result2);
  }

  free(partitioned_data1);
  free(partitioned_data2);
  free(partitioned_indices1);
  free(partitioned_indices2);
  free(histogram1);
  free(histogram2);
  free(prefix_sum1);
  free(prefix_sum2);

  *out1 = result1;
  *out2 = result2;
  *out_size = count;
  return DB_SCHEMA_STATUS_OK;
}
