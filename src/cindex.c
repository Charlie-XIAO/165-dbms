/**
 * @file cindex.c
 * @implements cindex.h
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "bptree.h"
#include "cindex.h"

/**
 * Helper function to initialize an unclustered sorted index.
 */
static inline DbSchemaStatus _init_unclustered_sorted(Table *table,
                                                      Column *column) {
  column->index.sorter = malloc(sizeof(size_t) * table->capacity);
  if (column->index.sorter == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }

  int status = init_sorter(column->data, column->index.sorter, table->n_rows);
  if (status == -1) {
    free(column->index.sorter);
    column->index.sorter = NULL;
    return DB_SCHEMA_STATUS_INTERNAL_ERROR;
  }
  return DB_SCHEMA_STATUS_OK;
}

/**
 * Helper function to initialize an unclustered B+ tree index.
 */
static inline DbSchemaStatus _init_unclustered_btree(Table *table,
                                                     Column *column) {
  DbSchemaStatus status = _init_unclustered_sorted(table, column);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }
  return build_index_btree(column, table->n_rows);
}

/**
 * Helper function to initialize a clustered sorted index.
 */
static inline DbSchemaStatus
_init_clustered_sorted(Table *table, Column *column, bool skip_sorting) {
  if (table->n_rows == 0 || skip_sorting) {
    // There is nothing to do for a clustered sorted index if there is no data
    // or we want to skip the sorting step, since it does not carry additional
    // structures
    return DB_SCHEMA_STATUS_OK;
  }

  size_t *sorter = malloc(sizeof(size_t) * table->n_rows);
  if (sorter == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }

  int init_status = init_sorter(column->data, sorter, table->n_rows);
  if (init_status == -1) {
    return DB_SCHEMA_STATUS_INTERNAL_ERROR;
  }

  DbSchemaStatus status = propagate_sorter(table, sorter);
  free(sorter);
  return status;
}

/**
 * Helper function to initialize a clustered B+ tree index.
 */
static inline DbSchemaStatus _init_clustered_btree(Table *table, Column *column,
                                                   bool skip_sorting) {
  DbSchemaStatus status = _init_clustered_sorted(table, column, skip_sorting);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }
  return build_index_btree(column, table->n_rows);
}

/**
 * @implements update_sorter
 */
DbSchemaStatus update_sorter(int *arr, size_t *sorter, size_t n_rows,
                             size_t new_n_rows) {
  // Argsort the new rows
  fill_range(sorter, n_rows, n_rows + new_n_rows);
  int status = aquicksort(arr, sorter + n_rows, new_n_rows);
  if (status != 0) {
    return DB_SCHEMA_STATUS_INTERNAL_ERROR;
  }

  // Merge sorters of the original and new rows
  status = amerge(arr, sorter + n_rows, n_rows, new_n_rows);
  if (status != 0) {
    return DB_SCHEMA_STATUS_INTERNAL_ERROR;
  }

  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements propagate_sorter
 */
DbSchemaStatus propagate_sorter(Table *table, size_t *sorter) {
  int *old_data = malloc(sizeof(int) * table->n_rows);
  if (old_data == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }

  for (size_t i = 0; i < table->n_cols; i++) {
    memcpy(old_data, table->columns[i].data, sizeof(int) * table->n_rows);
    for (size_t j = 0; j < table->n_rows; j++) {
      table->columns[i].data[j] = old_data[sorter[j]];
    }
  }
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements build_index_btree
 */
DbSchemaStatus build_index_btree(Column *column, size_t n_rows) {
  switch (column->index_type) {
  case COLUMN_INDEX_TYPE_NONE:
    assert(0 && "Unreachable code");
  case COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED:
    assert(0 && "Unreachable code");
  case COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE:
    column->index.tree =
        bplus_tree_create(column->data, column->index.sorter, n_rows);
    break;
  case COLUMN_INDEX_TYPE_CLUSTERED_SORTED:
    assert(0 && "Unreachable code");
  case COLUMN_INDEX_TYPE_CLUSTERED_BTREE:
    column->index.tree = bplus_tree_create(column->data, NULL, n_rows);
    break;
  }

  if (column->index.tree == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements reconstruct_unclustered_indexes
 */
DbSchemaStatus reconstruct_unclustered_indexes(Table *table) {
  DbSchemaStatus status;

  for (size_t i = 0; i < table->n_cols; i++) {
    if (i == table->primary) {
      continue; // Skip the primary column
    }

    switch (table->columns[i].index_type) {
    case COLUMN_INDEX_TYPE_NONE:
      break;
    case COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED:
      free(table->columns[i].index.sorter);
      status = _init_unclustered_sorted(table, &table->columns[i]);
      if (status != DB_SCHEMA_STATUS_OK) {
        return status;
      }
      break;
    case COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE:
      free(table->columns[i].index.sorter);
      bplus_tree_free(table->columns[i].index.tree);
      status = _init_unclustered_btree(table, &table->columns[i]);
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

/**
 * @implements init_cindex
 */
DbSchemaStatus init_cindex(Table *table, Column *column, bool skip_sorting) {
  DbSchemaStatus status;

  switch (column->index_type) {
  case COLUMN_INDEX_TYPE_NONE:
    return DB_SCHEMA_STATUS_OK;
  case COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED:
    return _init_unclustered_sorted(table, column);
  case COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE:
    return _init_unclustered_btree(table, column);
  case COLUMN_INDEX_TYPE_CLUSTERED_SORTED:
    status = _init_clustered_sorted(table, column, skip_sorting);
    if (status != DB_SCHEMA_STATUS_OK) {
      return status;
    }
    return skip_sorting ? DB_SCHEMA_STATUS_OK
                        : reconstruct_unclustered_indexes(table);
  case COLUMN_INDEX_TYPE_CLUSTERED_BTREE:
    status = _init_clustered_btree(table, column, skip_sorting);
    if (status != DB_SCHEMA_STATUS_OK) {
      return status;
    }
    return skip_sorting ? DB_SCHEMA_STATUS_OK
                        : reconstruct_unclustered_indexes(table);
  }

  assert(0 && "Unreachable code");
}

/**
 * @implements resize_cindex
 */
DbSchemaStatus resize_cindex(Column *column, size_t new_capacity) {
  // B+ trees do not need to be resize because they are dynamically allocated;
  // only the sorters need to be resized, which are carried by the two types of
  // unclustered indexes
  if (column->index_type == COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED ||
      column->index_type == COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE) {
    column->index.sorter =
        realloc(column->index.sorter, sizeof(size_t) * new_capacity);
    if (column->index.sorter == NULL) {
      return DB_SCHEMA_STATUS_REALLOC_FAILED;
    }
  }
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements free_cindex
 */
void free_cindex(Column *column) {
  switch (column->index_type) {
  case COLUMN_INDEX_TYPE_NONE:
    break;
  case COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED:
    free(column->index.sorter);
    column->index.sorter = NULL;
    break;
  case COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE:
    free(column->index.sorter);
    bplus_tree_free(column->index.tree);
    column->index.sorter = NULL;
    column->index.tree = NULL;
    break;
  case COLUMN_INDEX_TYPE_CLUSTERED_SORTED:
    // Clustered sorted index does not carry extra structures
    break;
  case COLUMN_INDEX_TYPE_CLUSTERED_BTREE:
    bplus_tree_free(column->index.tree);
    column->index.tree = NULL;
    break;
  }
}
