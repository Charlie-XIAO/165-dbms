/**
 * @file cmdinsert.c
 * @implements cmdinsert.h
 */

#include <assert.h>
#include <string.h>

#include "binsearch.h"
#include "cindex.h"
#include "cmdinsert.h"

/**
 * Helper function to insert a row into the table at a specific position.
 */
static inline void _insert_at(Table *table, size_t ind, int *values) {
  if (ind == table->n_rows) {
    // If the insert position is the end of the table, we can directly append
    for (size_t i = 0; i < table->n_cols; i++) {
      table->columns[i].data[ind] = values[i];
    }
    table->n_rows++;
    return;
  }

  // Shift all rows after the insert position to the right by one
  for (size_t i = 0; i < table->n_cols; i++) {
    memmove(table->columns[i].data + ind + 1, table->columns[i].data + ind,
            sizeof(int) * (table->n_rows - ind));
    table->columns[i].data[ind] = values[i];
  }
  table->n_rows++;
}

/**
 * Helper to insert a row into an unclustered sorted column.
 */
static inline DbSchemaStatus
_insert_unclustered_sorted(Table *table, Column *column, int value) {
  // Insert the new position into the sorted positions array; note that we are
  // simply appending the new value to the end of the physical data array, so
  // the new position is the last position; the insert point can be found by arg
  // binary search, and we are aligning right so as to insert as the last if
  // encountering equal values
  size_t ind = abinsearch(column->data, value, column->index.sorter,
                          table->n_rows, false);
  memmove(column->index.sorter + ind + 1, column->index.sorter + ind,
          sizeof(size_t) * (table->n_rows - ind));
  column->index.sorter[ind] = table->n_rows;

  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper to insert a row into an unclustered B+ tree column.
 */
static inline DbSchemaStatus
_insert_unclustered_btree(Table *table, Column *column, int value) {
  DbSchemaStatus status = _insert_unclustered_sorted(table, column, value);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }

  // Insert the value into the B+ tree; note that we are simply appending the
  // new value to the end of the physical data array, so the new position is the
  // last position
  return bplus_tree_insert(column->index.tree, value, table->n_rows) == -1
             ? DB_SCHEMA_STATUS_INTERNAL_ERROR
             : DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to insert a row into a clustered sorted column.
 */
static inline DbSchemaStatus _insert_clustered_sorted(Table *table,
                                                      int *values) {
  // Use binary search to find the insert position for the new row; we are
  // aligning right so that new rows with duplicate values appear after existing
  // duplicates
  size_t ind = binsearch(table->columns[table->primary].data,
                         values[table->primary], table->n_rows, false);
  _insert_at(table, ind, values);

  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to insert a row into a clustered B+ tree column.
 */
static inline DbSchemaStatus _insert_clustered_btree(Table *table,
                                                     int *values) {
  Column *column = &table->columns[table->primary];
  int value = values[table->primary];

  // Use B+ tree point search to find the insert position for the new row; we
  // are aligning right so that new rows with duplicate values appear after
  // existing duplicates
  size_t ind = bplus_tree_search_cont(column->index.tree, value, false);
  _insert_at(table, ind, values);

  // Insert the value into the B+ tree
  return bplus_tree_insert(column->index.tree, value, ind) == -1
             ? DB_SCHEMA_STATUS_INTERNAL_ERROR
             : DB_SCHEMA_STATUS_OK;
}

/**
 * @implements cmdinsert
 */
DbSchemaStatus cmdinsert(Table *table, int *values) {
  if (table == NULL) {
    return DB_SCHEMA_STATUS_TABLE_NOT_EXIST;
  }

  // Check if the table has all columns initialized so that it can be ready for
  // insertion
  if (table->n_inited_cols != table->n_cols) {
    return DB_SCHEMA_STATUS_TABLE_NOT_FULL;
  }

  // Resize the table if necessary
  DbSchemaStatus expand_status = maybe_expand_table(table, 1);
  if (expand_status != DB_SCHEMA_STATUS_OK) {
    return expand_status;
  }

  DbSchemaStatus status = DB_SCHEMA_STATUS_OK;

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
      status = _insert_clustered_sorted(table, values);
      break;
    case COLUMN_INDEX_TYPE_CLUSTERED_BTREE:
      status = _insert_clustered_btree(table, values);
      break;
    }
    return status != DB_SCHEMA_STATUS_OK
               ? status
               : reconstruct_unclustered_indexes(table);
  }

  // There is no clustered index in the table
  for (size_t i = 0; i < table->n_cols; i++) {
    Column *column = &table->columns[i];
    column->data[table->n_rows] = values[i];

    switch (column->index_type) {
    case COLUMN_INDEX_TYPE_NONE:
      break;
    case COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED:
      status = _insert_unclustered_sorted(table, column, values[i]);
      if (status != DB_SCHEMA_STATUS_OK) {
        return status;
      }
      break;
    case COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE:
      status = _insert_unclustered_btree(table, column, values[i]);
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

  table->n_rows++;
  return DB_SCHEMA_STATUS_OK;
}
