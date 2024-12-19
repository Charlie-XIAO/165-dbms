/**
 * @file db_schema.c
 * @implements db_schema.h
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cindex.h"
#include "db_schema.h"
#include "io.h"

/**
 * Convenience macro to return -1 on fread failure.
 */
#define _CHECKED_FREAD(ptr, size, count, stream)                               \
  do {                                                                         \
    if (fread((ptr), (size), (count), (stream)) != (count)) {                  \
      return -1;                                                               \
    }                                                                          \
  } while (0)

/**
 * Convenience macro to return -1 on fwrite failure.
 */
#define _CHECKED_FWRITE(ptr, size, count, stream)                              \
  do {                                                                         \
    if (fwrite((ptr), (size), (count), (stream)) != (count)) {                 \
      return -1;                                                               \
    }                                                                          \
  } while (0)

/**
 * Helper function to look up a table with database and table names.
 */
static inline DbSchemaStatus _help_lookup_table(char *db_name, char *table_name,
                                                Table **table) {
  // Look up the database
  DbSchemaStatus status = lookup_db(db_name);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }

  // Look up the table
  for (size_t i = 0; i < __DB__->n_tables; i++) {
    if (strcmp(__DB__->tables[i].name, table_name) == 0) {
      *table = &__DB__->tables[i];
      return DB_SCHEMA_STATUS_OK;
    }
  }
  return DB_SCHEMA_STATUS_TABLE_NOT_EXIST;
}

/**
 * Helper function to resize a table.
 *
 * Note that the already-done reallocations of the columns are not rolled back
 * on failure.
 */
static inline DbSchemaStatus _resize_table(Table *table, size_t new_capacity) {
  DbSchemaStatus status;

  for (size_t i = 0; i < table->n_cols; i++) {
    int *new_data = mremap_column_file(table->columns[i].data, table->capacity,
                                       new_capacity, table->columns[i].fd);
    if (new_data == NULL) {
      return DB_SCHEMA_STATUS_ALLOC_EXPAND_FAILED;
    }
    table->columns[i].data = new_data;

    status = resize_cindex(&table->columns[i], new_capacity);
    if (status != DB_SCHEMA_STATUS_OK) {
      return status;
    }
  }

  table->capacity = new_capacity;
  return DB_SCHEMA_STATUS_OK;
}

Db *__DB__;

/**
 * @implements format_status
 */
char *format_status(DbSchemaStatus status) {
  switch (status) {
  case DB_SCHEMA_STATUS_OK:
    return NULL; // We do not need an interpretation for success
  case DB_SCHEMA_STATUS_DB_ALREADY_EXISTS:
    return "Database already exists.";
  case DB_SCHEMA_STATUS_DB_NOT_EXIST:
    return "Database does not exist.";
  case DB_SCHEMA_STATUS_TABLE_ALREADY_EXISTS:
    return "Table already exists.";
  case DB_SCHEMA_STATUS_TABLE_NOT_EXIST:
    return "Table does not exist.";
  case DB_SCHEMA_STATUS_TABLE_FULL:
    return "Table cannot hold more columns.";
  case DB_SCHEMA_STATUS_TABLE_NOT_FULL:
    return "Table does not have the specified number of columns initialized.";
  case DB_SCHEMA_STATUS_COLUMN_ALREADY_EXISTS:
    return "Column already exists.";
  case DB_SCHEMA_STATUS_COLUMN_NOT_EXIST:
    return "Column does not exist.";
  case DB_SCHEMA_STATUS_INDEX_ALREADY_EXISTS:
    return "Index already exists on the column.";
  case DB_SCHEMA_STATUS_CLUSTERED_INDEX_ALREADY_EXISTS:
    return "Clustered index already exists on some column in the table.";
  case DB_SCHEMA_STATUS_VAR_NO_TABLE:
    return "Variable does not include a table component.";
  case DB_SCHEMA_STATUS_VAR_NO_COLUMN:
    return "Variable does not include a column component.";
  case DB_SCHEMA_STATUS_ALLOC_FAILED:
    return "Memory allocation failed.";
  case DB_SCHEMA_STATUS_ALLOC_EXPAND_FAILED:
    return "Memory expansion (realloc) failed.";
  case DB_SCHEMA_STATUS_ALLOC_SHRINK_FAILED:
    return "Memory shrinking (realloc) failed.";
  case DB_SCHEMA_STATUS_REALLOC_FAILED:
    return "Memory reallocation failed.";
  case DB_SCHEMA_STATUS_CSV_INVALID_HEADER:
    return "CSV header is not valid for loading.";
  case DB_SCHEMA_STATUS_PARALLEL_NOT_INITIALIZED:
    return "Parallelization requested but not initialized.";
  case DB_SCHEMA_STATUS_INTERNAL_ERROR:
    return "Internal execution error.";
  }
  return "Unknown status code.";
}

/**
 * @implements system_launch
 */
int system_launch() {
  int status;
  FILE *catalog = get_catalog_file(false, &status);
  if (status < 0) {
    return -1;
  }

  // Status code 1 corresponds to an empty or previously non-existent catalog
  // file, which means that we need to do nothing
  if (status == 1) {
    __DB__ = NULL;
    return 0;
  }

  // Construct the database from the catalog
  __DB__ = malloc(sizeof(Db));
  if (__DB__ == NULL) {
    return -1;
  }
  _CHECKED_FREAD(__DB__->name, sizeof(char), MAX_SIZE_NAME, catalog);
  _CHECKED_FREAD(&__DB__->n_tables, sizeof(size_t), 1, catalog);
  _CHECKED_FREAD(&__DB__->capacity, sizeof(size_t), 1, catalog);
  __DB__->tables = malloc(sizeof(Table) * __DB__->capacity);

  // Construct the tables from the catalog
  for (size_t i = 0; i < __DB__->n_tables; i++) {
    Table *table = &__DB__->tables[i];
    _CHECKED_FREAD(table->name, sizeof(char), MAX_SIZE_NAME, catalog);
    _CHECKED_FREAD(&table->n_cols, sizeof(size_t), 1, catalog);
    _CHECKED_FREAD(&table->n_inited_cols, sizeof(size_t), 1, catalog);
    _CHECKED_FREAD(&table->n_rows, sizeof(size_t), 1, catalog);
    _CHECKED_FREAD(&table->capacity, sizeof(size_t), 1, catalog);
    _CHECKED_FREAD(&table->primary, sizeof(size_t), 1, catalog);
    table->columns = malloc(sizeof(Column) * table->n_cols);

    // Construct the columns from the catalog
    for (size_t j = 0; j < table->n_inited_cols; j++) {
      Column *column = &table->columns[j];
      _CHECKED_FREAD(column->name, sizeof(char), MAX_SIZE_NAME, catalog);
      _CHECKED_FREAD(&column->index_type, sizeof(ColumnIndexType), 1, catalog);
      column->data = mmap_column_file(table->name, column->name,
                                      table->capacity, &column->fd);
      if (column->data == NULL) {
        return -1;
      }

      // Initialize the column index; in this case the underlying data for
      // clustered indexes is already sorted (if any), so we take short-circuit,
      // and we do not need to deal with clustered first since it will not
      // modify underlying data
      DbSchemaStatus init_status = init_cindex(table, column, true);
      if (init_status != DB_SCHEMA_STATUS_OK) {
        return -1;
      }
    }
  }

  fclose(catalog);
  return 0;
}

/**
 * @implements system_shutdown
 */
int system_shutdown() {
  int status;

  // Open the catalog file for writing
  FILE *catalog = get_catalog_file(true, &status);
  if (status != 0) {
    return -1; // The catalog file must exist and be openable
  }

  // If the current database is NULL, we leave the catalog empty
  if (__DB__ == NULL) {
    fclose(catalog);
    return 0;
  }

  // Write the database to the catalog
  _CHECKED_FWRITE(__DB__->name, sizeof(char), MAX_SIZE_NAME, catalog);
  _CHECKED_FWRITE(&__DB__->n_tables, sizeof(size_t), 1, catalog);
  _CHECKED_FWRITE(&__DB__->capacity, sizeof(size_t), 1, catalog);

  // Write the tables to the catalog
  for (size_t i = 0; i < __DB__->n_tables; i++) {
    Table *table = &__DB__->tables[i];
    _CHECKED_FWRITE(table->name, sizeof(char), MAX_SIZE_NAME, catalog);
    _CHECKED_FWRITE(&table->n_cols, sizeof(size_t), 1, catalog);
    _CHECKED_FWRITE(&table->n_inited_cols, sizeof(size_t), 1, catalog);
    _CHECKED_FWRITE(&table->n_rows, sizeof(size_t), 1, catalog);
    _CHECKED_FWRITE(&table->capacity, sizeof(size_t), 1, catalog);
    _CHECKED_FWRITE(&table->primary, sizeof(size_t), 1, catalog);

    // Write the columns to the catalog
    for (size_t j = 0; j < table->n_inited_cols; j++) {
      Column *column = &table->columns[j];
      _CHECKED_FWRITE(column->name, sizeof(char), MAX_SIZE_NAME, catalog);
      _CHECKED_FWRITE(&column->index_type, sizeof(ColumnIndexType), 1, catalog);
    }
  }
  fclose(catalog);

  free_db();
  return 0;
}

/**
 * @implements lookup_db
 */
DbSchemaStatus lookup_db(char *db_var) {
  if (__DB__ == NULL || strcmp(__DB__->name, db_var) != 0) {
    return DB_SCHEMA_STATUS_DB_NOT_EXIST;
  }
  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements lookup_table
 */
DbSchemaStatus lookup_table(char *table_var, Table **table) {
  char *db_name = strsep(&table_var, ".");
  if (table_var == NULL || *table_var == '\0') {
    return DB_SCHEMA_STATUS_VAR_NO_TABLE;
  }
  return _help_lookup_table(db_name, table_var, table);
}

/**
 * @implements lookup_column
 */
DbSchemaStatus lookup_column(char *column_var, Table **table,
                             size_t *ith_column) {
  char *db_name = strsep(&column_var, ".");
  char *table_name = strsep(&column_var, ".");
  if (table_name == NULL || *table_name == '\0') {
    return DB_SCHEMA_STATUS_VAR_NO_TABLE;
  }
  if (column_var == NULL || *column_var == '\0') {
    return DB_SCHEMA_STATUS_VAR_NO_COLUMN;
  }

  // Lookup the table (and implicitly the database)
  DbSchemaStatus status = _help_lookup_table(db_name, table_name, table);
  if (status != DB_SCHEMA_STATUS_OK) {
    return status;
  }

  // Look up the column
  for (size_t i = 0; i < (*table)->n_inited_cols; i++) {
    if (strcmp((*table)->columns[i].name, column_var) == 0) {
      *ith_column = i;
      return DB_SCHEMA_STATUS_OK;
    }
  }
  return DB_SCHEMA_STATUS_COLUMN_NOT_EXIST;
}

/**
 * @implements maybe_expand_table
 */
DbSchemaStatus maybe_expand_table(Table *table, size_t increment) {
  if (table->n_rows + increment <= table->capacity) {
    return DB_SCHEMA_STATUS_OK;
  }

  size_t new_capacity = table->capacity;
  while (table->n_rows + increment > new_capacity) {
    new_capacity *= EXPAND_FACTOR_TABLE;
  }
  return _resize_table(table, new_capacity);
}

/**
 * @implements maybe_shrink_table
 */
DbSchemaStatus maybe_shrink_table(Table *table) {
  if (table->n_rows >=
      table->capacity / SHRINK_FACTOR_TABLE / EXPAND_FACTOR_TABLE) {
    return DB_SCHEMA_STATUS_OK;
  }

  size_t new_capacity = table->capacity / SHRINK_FACTOR_TABLE;
  while (table->n_rows <
         new_capacity / SHRINK_FACTOR_TABLE / EXPAND_FACTOR_TABLE) {
    new_capacity /= SHRINK_FACTOR_TABLE;
  }
  return _resize_table(table, new_capacity);
}

/**
 * @implements free_db
 */
void free_db() {
  if (__DB__ == NULL) {
    return;
  }

  // Free tables, unmap column files, and free column indexes
  for (size_t i = 0; i < __DB__->n_tables; i++) {
    Table *table = &__DB__->tables[i];
    for (size_t j = 0; j < table->n_inited_cols; j++) {
      munmap_column_file(table->columns[j].data, table->capacity,
                         table->columns[j].fd);
      free_cindex(&table->columns[j]);
    }
    free(table->columns);
  }

  // Free the database and reset to NULL
  free(__DB__->tables);
  free(__DB__);
  __DB__ = NULL;
}
