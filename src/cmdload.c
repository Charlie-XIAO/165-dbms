/**
 * @file cmdload.c
 * @implements cmdload.h
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "cindex.h"
#include "cmdload.h"
#include "sort.h"

/**
 * Helper to conclude loading of an unclustered sorted column.
 */
static inline DbSchemaStatus
_conclude_unclustered_sorted(Table *table, Column *column, size_t n_cumu_rows) {
  return update_sorter(column->data, column->index.sorter,
                       table->n_rows - n_cumu_rows, n_cumu_rows);
}

/**
 * Helper to conclude loading of an unclustered B+ tree column.
 */
static inline DbSchemaStatus
_conclude_unclustered_btree(Table *table, Column *column, size_t n_cumu_rows) {
  DbSchemaStatus status =
      _conclude_unclustered_sorted(table, column, n_cumu_rows);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }
  bplus_tree_free(column->index.tree);
  return build_index_btree(column, table->n_rows);
}

/**
 * Helper function to conclude loading of a clustered sorted column.
 */
static inline DbSchemaStatus _conclude_clustered_sorted(Table *table,
                                                        size_t n_cumu_rows) {
  Column *column = &table->columns[table->primary];

  size_t *sorter = malloc(sizeof(size_t) * table->n_rows);
  if (sorter == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  fill_range(sorter, 0, table->n_rows);

  DbSchemaStatus status = update_sorter(
      column->data, sorter, table->n_rows - n_cumu_rows, n_cumu_rows);
  if (status != DB_SCHEMA_STATUS_OK) {
    free(sorter);
    return status;
  }

  status = propagate_sorter(table, sorter);
  free(sorter);
  return status;
}

/**
 * Helper function to conclude loading of a clustered B+ tree column.
 */
static inline DbSchemaStatus _conclude_clustered_btree(Table *table,
                                                       size_t n_cumu_rows) {
  DbSchemaStatus status = _conclude_clustered_sorted(table, n_cumu_rows);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }
  Column *column = &table->columns[table->primary];
  bplus_tree_free(column->index.tree);
  return build_index_btree(column, table->n_rows);
}

/**
 * @implements cmdload_validate_header
 */
DbSchemaStatus cmdload_validate_header(char *header, size_t n_cols,
                                       Table **table) {
  // We need a tokenizer so that we can free the copied header later since
  // strsep destroys its input
  char *header_copy = strdup(header);
  char *tokenizer = header_copy;

  Table *current_table;
  size_t current_ith_column;
  for (size_t i = 0; i < n_cols; i++) {
    // Get the next column variable and look it up; we are not doing any
    // additional checks here since the number of columns stored in the CSV is
    // exactly counted by the number of commas
    char *col_var = strsep(&tokenizer, ",");
    DbSchemaStatus status =
        lookup_column(col_var, &current_table, &current_ith_column);

    // If the column does not exist, error out
    if (status != DB_SCHEMA_STATUS_OK) {
      free(header_copy);
      return DB_SCHEMA_STATUS_CSV_INVALID_HEADER;
    }

    // The order of the columns in the CSV must match the order of the columns
    // in the table
    if (current_ith_column != i) {
      free(header_copy);
      return DB_SCHEMA_STATUS_CSV_INVALID_HEADER;
    }

    if (i == 0) {
      // This is the first column, and we record the table as long as it is
      // fully initialized
      if (current_table->n_inited_cols != current_table->n_cols) {
        free(header_copy);
        return DB_SCHEMA_STATUS_TABLE_NOT_FULL;
      }
      *table = current_table;
    } else {
      // This is not the first column, and we check if the column belongs to the
      // same table as the first column
      if (current_table != *table) {
        free(header_copy);
        return DB_SCHEMA_STATUS_CSV_INVALID_HEADER;
      }
    }
  }

  free(header_copy);
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements cmdload_rows
 */
DbSchemaStatus cmdload_rows(Table *table, int *data, size_t n_rows) {
  if (table == NULL) {
    return DB_SCHEMA_STATUS_TABLE_NOT_EXIST;
  }

  // Check if the table has all columns initialized so that it can be ready for
  // insertion
  if (table->n_inited_cols != table->n_cols) {
    return DB_SCHEMA_STATUS_TABLE_NOT_FULL;
  }

  // Expand the table if necessary
  DbSchemaStatus expand_status = maybe_expand_table(table, n_rows);
  if (expand_status != DB_SCHEMA_STATUS_OK) {
    return expand_status;
  }

  // Insert the rows into the table; we are not caring about column indexes
  // here, because they will be dealt with when concluding the load command
  for (size_t i = 0; i < table->n_cols; i++) {
    for (size_t j = 0; j < n_rows; j++) {
      table->columns[i].data[table->n_rows + j] = data[j * table->n_cols + i];
    }
  }

  table->n_rows += n_rows;
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements cmdload_conclude
 */
DbSchemaStatus cmdload_conclude(Table *table, size_t n_cumu_rows) {
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
      status = _conclude_clustered_sorted(table, n_cumu_rows);
      break;
    case COLUMN_INDEX_TYPE_CLUSTERED_BTREE:
      status = _conclude_clustered_btree(table, n_cumu_rows);
      break;
    }
    return status != DB_SCHEMA_STATUS_OK
               ? status
               : reconstruct_unclustered_indexes(table);
  }

  // There is no clustered index in the table
  for (size_t i = 0; i < table->n_cols; i++) {
    Column *column = &table->columns[i];
    switch (column->index_type) {
    case COLUMN_INDEX_TYPE_NONE:
      break;
    case COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED:
      status = _conclude_unclustered_sorted(table, column, n_cumu_rows);
      if (status != DB_SCHEMA_STATUS_OK) {
        return status;
      }
      break;
    case COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE:
      status = _conclude_unclustered_btree(table, column, n_cumu_rows);
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

  return DB_SCHEMA_STATUS_OK;
}
