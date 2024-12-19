/**
 * @file cmddelete.c
 * @implements cmddelete.h
 */

#include <assert.h>

#include "bitvector.h"
#include "cindex.h"
#include "cmddelete.h"

/**
 * Helper function to delete from a column with no index.
 */
static inline DbSchemaStatus _delete_from_raw(Table *table, Column *column,
                                              size_t *indices,
                                              size_t n_indices) {
  // Create a boolean mask that is true for rows to be removed; this will play
  // the role of a hashset to allow O(1) lookup of whether a row should be
  // removed when iterating over the rows
  BitVector *removal_mask = bitvector_create(table->n_rows);
  if (removal_mask == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  for (size_t i = 0; i < n_indices; i++) {
    bitvector_set(removal_mask, indices[i]);
  }

  // Remove rows in-place using fast and slow pointers
  size_t slow = 0;
  for (size_t i = 0; i < table->n_rows; i++) {
    if (!bitvector_test(removal_mask, i)) {
      column->data[slow++] = column->data[i];
    }
  }

  bitvector_free(removal_mask);
  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to delete from a column with an unclustered sorted index.
 */
static inline DbSchemaStatus _delete_from_unclustered_sorted(Table *table,
                                                             Column *column,
                                                             size_t *indices,
                                                             size_t n_indices) {
  // Create an array that simulates a hashmap from old positions to new
  // positions after removing the rows; first mark the rows to be removed by
  // __SIZE_MAX__ (similar to the bitvector in _delete_from_raw)
  size_t *old_to_new = calloc(table->n_rows, sizeof(size_t));
  if (old_to_new == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  for (size_t i = 0; i < n_indices; i++) {
    old_to_new[indices[i]] = __SIZE_MAX__;
  }

  // Remove rows in-place using fast and slow pointers; meanwhile update the
  // mapping; e.g., if we remove rows at index 1 and 3 from 6 rows, the mapping
  // would become [0, __SIZE_MAX__, 1, __SIZE_MAX__, 2, 3]
  size_t slow = 0;
  for (size_t i = 0; i < table->n_rows; i++) {
    if (old_to_new[i] != __SIZE_MAX__) {
      column->data[slow] = column->data[i];
      old_to_new[i] = slow++;
    }
  }

  // Update the sorter
  slow = 0;
  for (size_t i = 0; i < table->n_rows; i++) {
    if (old_to_new[i] != __SIZE_MAX__) {
      column->index.sorter[slow++] = old_to_new[column->index.sorter[i]];
    }
  }

  free(old_to_new);
  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to delete from a column with an unclustered B+ tree index.
 */
static inline DbSchemaStatus _delete_from_unclustered_btree(Table *table,
                                                            Column *column,
                                                            size_t *indices,
                                                            size_t n_indices) {
  DbSchemaStatus status =
      _delete_from_unclustered_sorted(table, column, indices, n_indices);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }

  bplus_tree_free(column->index.tree);
  return build_index_btree(column, table->n_rows);
}

/**
 * Helper function to delete from a column with a clustered sorted index.
 */
static inline DbSchemaStatus
_delete_from_clustered_sorted(Table *table, size_t *indices, size_t n_indices) {
  DbSchemaStatus status;

  // There is no difference from deleting rows with no index, since the given
  // indices would also be with respect to sorted physical data
  for (size_t i = 0; i < table->n_cols; i++) {
    status = _delete_from_raw(table, &table->columns[i], indices, n_indices);
    if (status != DB_SCHEMA_STATUS_OK) {
      return status;
    }
  }

  table->n_rows -= n_indices;
  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to delete from a column with a clustered B+ tree index.
 */
static inline DbSchemaStatus
_delete_from_clustered_btree(Table *table, size_t *indices, size_t n_indices) {
  DbSchemaStatus status =
      _delete_from_clustered_sorted(table, indices, n_indices);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }

  Column *column = &table->columns[table->primary];
  bplus_tree_free(column->index.tree);
  return build_index_btree(column, table->n_rows);
}

/**
 * @implements cmddelete
 */
DbSchemaStatus cmddelete(Table *table, GeneralizedPosvec *posvec) {
  DbSchemaStatus status = DB_SCHEMA_STATUS_OK;
  size_t *indices = posvec->posvec_pointer.index_array->indices;
  size_t n_indices = posvec->posvec_pointer.index_array->n_indices;

  // There is a clustered index in the table
  if (table->primary != __SIZE_MAX__) {
    switch (table->columns[table->primary].index_type) {
    case COLUMN_INDEX_TYPE_NONE:
      assert(0 && "Unreachable code");
    case COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED:
      assert(0 && "Unreachable code");
    case COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE:
      assert(0 && "Unreachable code");
    case COLUMN_INDEX_TYPE_CLUSTERED_SORTED:
      status = _delete_from_clustered_sorted(table, indices, n_indices);
      break;
    case COLUMN_INDEX_TYPE_CLUSTERED_BTREE:
      status = _delete_from_clustered_btree(table, indices, n_indices);
      break;
    }
    return status != DB_SCHEMA_STATUS_OK
               ? status
               : reconstruct_unclustered_indexes(table);
  }

  // There is no clustered index in the table
  for (size_t i = 0; i < table->n_cols; i++) {
    Column *column = &table->columns[i];
    switch (table->columns[i].index_type) {
    case COLUMN_INDEX_TYPE_NONE:
      status = _delete_from_raw(table, column, indices, n_indices);
      if (status != DB_SCHEMA_STATUS_OK) {
        return status;
      }
      break;
    case COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED:
      status =
          _delete_from_unclustered_sorted(table, column, indices, n_indices);
      if (status != DB_SCHEMA_STATUS_OK) {
        return status;
      }
      break;
    case COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE:
      status =
          _delete_from_unclustered_btree(table, column, indices, n_indices);
      if (status != DB_SCHEMA_STATUS_OK) {
        return status;
      }
      break;
    case COLUMN_INDEX_TYPE_CLUSTERED_SORTED:
      assert(0 && "Unreachable code");
    case COLUMN_INDEX_TYPE_CLUSTERED_BTREE:
      assert(0 && "Unreachable code");
    }
  }

  table->n_rows -= n_indices;
  return maybe_shrink_table(table);
}
