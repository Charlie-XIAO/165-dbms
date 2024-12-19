/**
 * @file cmdselect.c
 * @implements cmdselect.h
 */

#include <assert.h>

#include "binsearch.h"
#include "bptree.h"
#include "cmdselect.h"
#include "scan.h"

/**
 * Helper function to select from a column with an unclustered sorted index.
 */
static inline DbSchemaStatus
_select_unclustered_sorted(Column *column, size_t n_rows, long lower_bound,
                           long upper_bound, GeneralizedPosvec *posvec,
                           size_t *n_selected_indices,
                           size_t **selected_indices) {
  size_t *selected = malloc(sizeof(size_t) * n_rows);
  if (selected == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  // Binary search the lower bound, aligned left to avoid missing duplicates
  size_t lower_ind =
      abinsearch(column->data, lower_bound, column->index.sorter, n_rows, true);

  // Linear scan starting from the lower bound
  size_t count = 0;
  size_t ind;
  if (posvec == NULL) {
    for (size_t i = lower_ind; i < n_rows; i++) {
      ind = column->index.sorter[i];
      if (column->data[ind] >= upper_bound) {
        break;
      }
      selected[count++] = ind;
    }
  } else {
    for (size_t i = lower_ind; i < n_rows; i++) {
      ind = column->index.sorter[i];
      if (column->data[ind] >= upper_bound) {
        break;
      }
      selected[count++] = posvec->posvec_pointer.index_array->indices[ind];
    }
  }

  *selected_indices = realloc(selected, sizeof(size_t) * count);
  if (*selected_indices == NULL && count > 0) {
    return DB_SCHEMA_STATUS_REALLOC_FAILED;
  }
  *n_selected_indices = count;
  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to select from a column with an unclustered B+ tree index.
 */
static inline DbSchemaStatus
_select_unclustered_btree(Column *column, size_t n_rows, long lower_bound,
                          long upper_bound, GeneralizedPosvec *posvec,
                          size_t *n_selected_indices,
                          size_t **selected_indices) {
  size_t *selected = malloc(sizeof(size_t) * n_rows);
  if (selected == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }

  // Range search the B+ tree
  size_t count = bplus_tree_search_range(column->index.tree, lower_bound,
                                         upper_bound, selected);
  if (posvec != NULL) {
    for (size_t i = 0; i < count; i++) {
      selected[i] = posvec->posvec_pointer.index_array->indices[selected[i]];
    }
  }

  *selected_indices = realloc(selected, sizeof(size_t) * count);
  if (*selected_indices == NULL && count > 0) {
    return DB_SCHEMA_STATUS_REALLOC_FAILED;
  }
  *n_selected_indices = count;
  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to select from a column with a clustered sorted index.
 */
static inline DbSchemaStatus
_select_clustered_sorted(Column *column, size_t n_rows, long lower_bound,
                         long upper_bound, GeneralizedPosvec *posvec,
                         size_t *n_selected_indices,
                         size_t **selected_indices) {
  // Binary search the lower bound and the upper bound; lower bound is aligned
  // left to avoid missing duplicates, while upper bound is aligned left so that
  // excluding it excludes duplicates
  size_t lower_ind = binsearch(column->data, lower_bound, n_rows, true);
  size_t upper_ind = binsearch(column->data, upper_bound, n_rows, true);

  size_t *selected = malloc(sizeof(size_t) * (upper_ind - lower_ind));
  if (selected == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }

  // Directly materialize the selected indices since data is already sorted
  if (posvec == NULL) {
    for (size_t i = lower_ind; i < upper_ind; i++) {
      selected[i - lower_ind] = i;
    }
  } else {
    for (size_t i = lower_ind; i < upper_ind; i++) {
      selected[i - lower_ind] = posvec->posvec_pointer.index_array->indices[i];
    }
  }

  *selected_indices = selected;
  *n_selected_indices = upper_ind - lower_ind;
  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to select from a column with a clustered sorted index.
 */
static inline DbSchemaStatus
_select_clustered_btree(Column *column, long lower_bound, long upper_bound,
                        GeneralizedPosvec *posvec, size_t *n_selected_indices,
                        size_t **selected_indices) {
  // Range search the B+ tree assuming contiguous indices because the data is
  // already sorted
  size_t *selected = bplus_tree_search_range_cont(
      column->index.tree, lower_bound, upper_bound, n_selected_indices);
  if (selected == NULL) {
    return DB_SCHEMA_STATUS_INTERNAL_ERROR;
  }
  if (posvec != NULL) {
    for (size_t i = 0; i < *n_selected_indices; i++) {
      selected[i] = posvec->posvec_pointer.index_array->indices[selected[i]];
    }
  }

  *selected_indices = selected;
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements cmdselect_raw
 */
GeneralizedPosvec *cmdselect_raw(GeneralizedValvec *valvec,
                                 GeneralizedPosvec *posvec, long lower_bound,
                                 long upper_bound, DbSchemaStatus *status) {
  // Scan with only the select flag enabled
  ScanContext ctx = init_empty_scan_context();
  ctx.n_select_queries = 1;
  long lower_bound_arr[1] = {lower_bound};
  long upper_bound_arr[1] = {upper_bound};
  ctx.lower_bound_arr = lower_bound_arr;
  ctx.upper_bound_arr = upper_bound_arr;

  *status = shared_scan(valvec, posvec, &ctx, SCAN_CALLBACK_SELECT_FLAG);
  if (*status != DB_SCHEMA_STATUS_OK) {
    return NULL;
  }

  // Wrap the indices into a position vector
  GeneralizedPosvec *new_posvec = wrap_index_array(
      ctx.selected_indices_arr[0], ctx.n_selected_indices_arr[0], status);
  if (*status != DB_SCHEMA_STATUS_OK) {
    free(ctx.selected_indices_arr[0]);
    free(ctx.selected_indices_arr);
    free(ctx.n_selected_indices_arr);
    return NULL;
  }
  free(ctx.selected_indices_arr);
  free(ctx.n_selected_indices_arr);
  return new_posvec;
}

/**
 * @implements cmdselect_index
 */
GeneralizedPosvec *cmdselect_index(Column *column, size_t n_rows,
                                   GeneralizedPosvec *posvec, long lower_bound,
                                   long upper_bound, DbSchemaStatus *status) {
  size_t n_selected_indices = 0;
  size_t *selected_indices = NULL;

  switch (column->index_type) {
  case COLUMN_INDEX_TYPE_NONE:
    assert(0 && "Invalid routine");
  case COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED:
    *status = _select_unclustered_sorted(
        column, n_rows, lower_bound, upper_bound, posvec, &n_selected_indices,
        &selected_indices);
    break;
  case COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE:
    *status = _select_unclustered_btree(column, n_rows, lower_bound,
                                        upper_bound, posvec,
                                        &n_selected_indices, &selected_indices);
    break;
  case COLUMN_INDEX_TYPE_CLUSTERED_SORTED:
    *status = _select_clustered_sorted(column, n_rows, lower_bound, upper_bound,
                                       posvec, &n_selected_indices,
                                       &selected_indices);
    break;
  case COLUMN_INDEX_TYPE_CLUSTERED_BTREE:
    *status = _select_clustered_btree(column, lower_bound, upper_bound, posvec,
                                      &n_selected_indices, &selected_indices);
    break;
  }

  if (*status != DB_SCHEMA_STATUS_OK) {
    return NULL;
  }

  // Wrap the indices into a position vector
  GeneralizedPosvec *new_posvec =
      wrap_index_array(selected_indices, n_selected_indices, status);
  if (*status != DB_SCHEMA_STATUS_OK) {
    free(selected_indices);
    return NULL;
  }
  return new_posvec;
}
