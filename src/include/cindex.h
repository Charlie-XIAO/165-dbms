/**
 * @file cindex.h
 *
 * This header contains utilities for managing and utilizing column indexes.
 */

#ifndef CINDEX_H_
#define CINDEX_H_

#include "db_schema.h"
#include "sort.h"

/**
 * Fill the [start, end) range in an array with their indices.
 */
static inline void fill_range(size_t *arr, size_t start, size_t end) {
  for (size_t i = start; i < end; i++) {
    arr[i] = i;
  }
}

/**
 * Initialize a sorter.
 *
 * This function takes the data and an uninitialized sorted with sufficient
 * capacity. The first `n_rows` rows will be initialized with the sorted order
 * of the data. This function returns 0 on success and -1 on failure.
 */
static inline int init_sorter(int *arr, size_t *sorter, size_t n_rows) {
  fill_range(sorter, 0, n_rows);
  return aquicksort(arr, sorter, n_rows);
}

/**
 * Update a sorter.
 *
 * This function takes the data and the corresponding sorter. The sorter is
 * sorted for the first `n_rows` rows. The next `new_n_rows` rows in the sorter
 * are uninitialized. The sorter should have enough capacity to hold
 * `n_rows + new_n_rows` rows. This function will first argsort the new rows and
 * merge with the existing sorter. It returns the status code of the operation.
 */
DbSchemaStatus update_sorter(int *arr, size_t *sorter, size_t n_rows,
                             size_t new_n_rows);

/**
 * Propagate the order of a sorter to all columns in a table.
 *
 * This function returns the status code of the operation.
 */
DbSchemaStatus propagate_sorter(Table *table, size_t *sorter);

/**
 * Rebuild the B+ tree structure for a B+ tree index.
 *
 * Depending on whether the index is clustered or not, the column data itself or
 * the sorter on the index must be sorted. Calling this function on a column
 * that does not have a B+ tree index is invalid. `n_rows` is the number of rows
 * in the column. This function returns the status code of the operation.
 */
DbSchemaStatus build_index_btree(Column *column, size_t n_rows);

/**
 * Helper function to reconstruct any unclustered indexes in a table.
 *
 * This function loops through all columns in the table. For the columns with
 * unclustered indexes, it will free the current indexes and reconstruct them
 * from scratch based on current column data. It will silently skip other
 * columns.
 */
DbSchemaStatus reconstruct_unclustered_indexes(Table *table);

/**
 * Initialize a column index.
 *
 * This function initializes the index of a column based on the type of index
 * specified. If `skip_sorting` is true, for clustered indexes this function
 * will assume that the underlying data is already sorted by the primary column.
 * This function returns the status code of the operation.
 */
DbSchemaStatus init_cindex(Table *table, Column *column, bool skip_sorting);

/**
 * Resize the index of a column.
 */
DbSchemaStatus resize_cindex(Column *column, size_t new_capacity);

/**
 * Free the index of a column.
 */
void free_cindex(Column *column);

#endif /* CINDEX_H_ */
