/**
 * @file cmdcreate.c
 * @implements cmdcreate.h
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "cindex.h"
#include "cmdcreate.h"
#include "io.h"

/**
 * @implements cmdcreate_db
 */
DbSchemaStatus cmdcreate_db(char *name) {
  // If a database already exists, we will free it and create a new one since
  // our database system supports only one database at a time
  if (__DB__ != NULL) {
    free_db();
    clear_db_persistence_dir();
  }

  // Allocate memory for the database
  __DB__ = malloc(sizeof(Db));
  if (__DB__ == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }

  // Assign the name of the database, ensuring null-terminated
  strncpy(__DB__->name, name, MAX_SIZE_NAME - 1);
  __DB__->name[MAX_SIZE_NAME - 1] = '\0';

  // Allocate the initial memory for the tables
  __DB__->tables = malloc(sizeof(Table) * INIT_NUM_TABLES_IN_DB);
  if (__DB__->tables == NULL) {
    free(__DB__);
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  __DB__->n_tables = 0;
  __DB__->capacity = INIT_NUM_TABLES_IN_DB;

  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements cmdcreate_tbl
 */
DbSchemaStatus cmdcreate_tbl(Db *db, char *name, size_t n_cols) {
  if (db == NULL) {
    return DB_SCHEMA_STATUS_DB_NOT_EXIST;
  }

  // Check if the table already exists in the database
  for (size_t i = 0; i < db->n_tables; i++) {
    if (strcmp(db->tables[i].name, name) == 0) {
      return DB_SCHEMA_STATUS_TABLE_ALREADY_EXISTS;
    }
  }

  // Assign the name of the table, ensuring null-terminated
  Table table;
  strncpy(table.name, name, MAX_SIZE_NAME - 1);
  table.name[MAX_SIZE_NAME - 1] = '\0';

  // Initialize the table with the given number of columns
  table.columns = malloc(sizeof(Column) * n_cols);
  if (table.columns == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }
  table.n_cols = n_cols;
  table.n_inited_cols = 0;
  table.n_rows = 0;
  table.capacity = INIT_NUM_ROWS_IN_TABLE;
  table.primary = __SIZE_MAX__;

  // Check if the database needs to be resized to accommodate the new table
  if (db->n_tables >= db->capacity) {
    size_t new_capacity = db->capacity * EXPAND_FACTOR_DB;

    Table *new_tables = realloc(db->tables, sizeof(Table) * new_capacity);
    if (new_tables == NULL) {
      free(table.columns);
      return DB_SCHEMA_STATUS_ALLOC_EXPAND_FAILED;
    }
    db->capacity = new_capacity;
    db->tables = new_tables;
  }

  // Add the table to the database
  db->tables[db->n_tables++] = table;
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements cmdcreate_col
 */
DbSchemaStatus cmdcreate_col(Table *table, char *name) {
  if (table == NULL) {
    return DB_SCHEMA_STATUS_TABLE_NOT_EXIST;
  }

  // Check if the column already exists in the table
  for (size_t i = 0; i < table->n_inited_cols; i++) {
    if (strcmp(table->columns[i].name, name) == 0) {
      return DB_SCHEMA_STATUS_COLUMN_ALREADY_EXISTS;
    }
  }

  // Check if the table has enough capacity for the new column
  if (table->n_inited_cols >= table->n_cols) {
    return DB_SCHEMA_STATUS_TABLE_FULL;
  }

  // Assign the name of the column, ensuring null-terminated
  Column column;
  strncpy(column.name, name, MAX_SIZE_NAME - 1);
  column.name[MAX_SIZE_NAME - 1] = '\0';

  // Column is not clustered by default
  column.index_type = COLUMN_INDEX_TYPE_NONE;
  column.index.sorter = NULL;
  column.index.tree = NULL;

  // Create a mmap'ed file for the column data
  column.data =
      mmap_column_file(table->name, name, table->capacity, &column.fd);
  if (column.data == NULL) {
    return DB_SCHEMA_STATUS_ALLOC_FAILED;
  }

  // Add the column to the table
  table->columns[table->n_inited_cols++] = column;
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements cmdcreate_idx
 */
DbSchemaStatus cmdcreate_idx(Table *table, size_t ith_column,
                             ColumnIndexType type) {
  // Check if the column already has an index
  Column *column = &table->columns[ith_column];
  if (column->index_type != COLUMN_INDEX_TYPE_NONE) {
    return DB_SCHEMA_STATUS_INDEX_ALREADY_EXISTS;
  }

  if ((type == COLUMN_INDEX_TYPE_CLUSTERED_SORTED ||
       type == COLUMN_INDEX_TYPE_CLUSTERED_BTREE)) {
    if (table->primary != __SIZE_MAX__) {
      // There cannot be multiple clustered indexes in a table
      return DB_SCHEMA_STATUS_CLUSTERED_INDEX_ALREADY_EXISTS;
    }
    table->primary = ith_column;
  }

  column->index_type = type;
  return init_cindex(table, column, false);
}
