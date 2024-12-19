/**
 * @file cmdbatch.c
 * @implements cmdbatch.h
 */

#include "cmdbatch.h"
#include "scan.h"

/**
 * @implements cmdbatch
 */
DbSchemaStatus cmdbatch(GeneralizedValvec *valvec, GeneralizedPosvec *posvec,
                        size_t n_select_queries, long *lower_bound_arr,
                        long *upper_bound_arr, int flags,
                        GeneralizedPosvec **select_results, int *min_result,
                        int *max_result, long long *sum_result) {
  ScanContext ctx = init_empty_scan_context();
  ctx.n_select_queries = n_select_queries;
  ctx.lower_bound_arr = lower_bound_arr;
  ctx.upper_bound_arr = upper_bound_arr;

  DbSchemaStatus status = shared_scan(valvec, posvec, &ctx, flags);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }

  // Process the results of select queries
  for (size_t i = 0; i < n_select_queries; i++) {
    // Wrap the indices in a position vector
    GeneralizedPosvec *new_posvec = wrap_index_array(
        ctx.selected_indices_arr[i], ctx.n_selected_indices_arr[i], &status);
    if (new_posvec == NULL) {
      // Free all previously allocated memory
      for (size_t j = 0; j < i; j++) {
        free(select_results[j]->posvec_pointer.index_array->indices);
        free(select_results[j]->posvec_pointer.index_array);
        free(select_results[j]);
      }
      free(ctx.selected_indices_arr[i]);
      free(ctx.selected_indices_arr);
      free(ctx.n_selected_indices_arr);
      return status;
    }
    select_results[i] = new_posvec;
  }
  free(ctx.selected_indices_arr);
  free(ctx.n_selected_indices_arr);

  // Process the results of aggregation queries
  if (flags & SCAN_CALLBACK_MIN_FLAG) {
    *min_result = ctx.min_result;
  }
  if (flags & SCAN_CALLBACK_MAX_FLAG) {
    *max_result = ctx.max_result;
  }
  if (flags & SCAN_CALLBACK_SUM_FLAG) {
    *sum_result = ctx.sum_result;
  }

  return DB_SCHEMA_STATUS_OK;
}
