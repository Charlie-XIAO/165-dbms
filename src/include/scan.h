/**
 * @file scan.h
 *
 * This header defines the scanning utilities over value vectors.
 */

#ifndef SCAN_H__
#define SCAN_H__

#include <stddef.h>

#include "client_context.h"

/**
 * The context of a shared scanning.
 *
 * Different scan operations needs different information and outputs different
 * results. This context collects all of them, where we allow not initializing
 * all fields of the struct.
 *
 * NOTE: `selected_indices_arr` is `n_select_queries` pointers, each pointing to
 * the selected indices data. It is meant to be used by the caller of the shared
 * scan. `selected_indices_arr_flattened` is a flattened and "transposed" array,
 * which can be viewed as shape (data_length, n_select_queries). It is used
 * internally for better cache locality to reduce cache write misses.
 */
typedef struct ScanContext {
  long *lower_bound_arr;
  long *upper_bound_arr;
  size_t **selected_indices_arr;
  size_t *selected_indices_arr_flattened;
  size_t *n_selected_indices_arr;
  size_t n_select_queries;
  int min_result;
  int max_result;
  long long sum_result;
} ScanContext;

/**
 * The shared scan function type.
 */
typedef void (*SharedScanFunc)(GeneralizedValvec *, GeneralizedPosvec *,
                               ScanContext *, size_t start, size_t end);

/**
 * The data for a shared scan task.
 *
 * This data is used for tasks in the shared scan task queue in multi-threaded
 * execution. The start and end indices mark the range of the value vector to
 * scan through, with starting index inclusive and ending index exclusive.
 */
typedef struct SharedScanTaskData {
  SharedScanFunc shared_scan_func;
  GeneralizedValvec *valvec;
  GeneralizedPosvec *posvec;
  size_t start;
  size_t end;
  ScanContext *ctx;
} SharedScanTaskData;

/**
 * Initialize an empty scan context.
 *
 * This function will return a scan context with all fields initialized to
 * reasonable default values.
 */
ScanContext init_empty_scan_context();

/**
 * Worker subroutine for shared scan.
 */
void shared_scan_subroutine(SharedScanTaskData *task_data);

/**
 * The main shared scan function.
 *
 * Given the flags, this function will dispatch a suitable shared scan routine
 * either in parallel or sequentially depending on the global configuration.
 * The scan context should be properly initialized before calling this function,
 * which will be updated with the results on successful return. The function
 * will return the status code of the operation.
 */
DbSchemaStatus shared_scan(GeneralizedValvec *valvec, GeneralizedPosvec *posvec,
                           ScanContext *ctx, int flags);

#endif /* SCAN_H__ */
