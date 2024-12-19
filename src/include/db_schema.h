/**
 * @file db_schema.h
 *
 * This header defines the core structures used in the database system,
 * including database, table, column, and related metadata. It also defines
 * basic manipulations on the database structures.
 */

#ifndef DB_SCHEMA_H__
#define DB_SCHEMA_H__

#include <stddef.h>

#include "bptree.h"
#include "consts.h"

/**
 * The type of a column index.
 *
 * Specially, `COLUMN_INDEX_TYPE_NONE` means no index is created on the column.
 */
typedef enum ColumnIndexType {
  COLUMN_INDEX_TYPE_NONE = 0,
  COLUMN_INDEX_TYPE_UNCLUSTERED_SORTED = 1,
  COLUMN_INDEX_TYPE_UNCLUSTERED_BTREE = 2,
  COLUMN_INDEX_TYPE_CLUSTERED_SORTED = 3,
  COLUMN_INDEX_TYPE_CLUSTERED_BTREE = 4,
} ColumnIndexType;

/**
 * The extra information carried by a column index.
 *
 * Clustered sorted index carries no extra information, while unclustered sorted
 * index carries the sorter array of the column data. Clustered and unclustered
 * B+ tree indexes carry an additional B+ tree structure on top of clustered and
 * unclustered sorted indexes, respectively.
 */
typedef struct ColumnIndex {
  size_t *sorter;
  BPlusTree *tree;
} ColumnIndex;

/**
 * A column in a table.
 *
 * This struct contains the name of the column, a pointer to the column data,
 * the file descriptor of the column data, and the column index (NULL if not
 * exist). The column data is always mmap'ed (instead of malloc'ed).
 */
typedef struct Column {
  char name[MAX_SIZE_NAME];
  int *data;
  int fd;
  ColumnIndexType index_type;
  ColumnIndex index;
} Column;

/**
 * A table in the database.
 *
 * This struct contains the name of the table, a pointer to the array of
 * columns, the number of columns, the number of rows (i.e., the length of each
 * column, which should be all the same), and the capacity (i.e., the number of
 * rows it can hold). Note that once created, the number of columns cannot be
 * changed. `n_inited_cols` is used to track how many columns have been
 * initialized, which is useful when we iteratively create the columns of a new
 * table. The capacity of the table, however, will be adjusted dynamically as
 * needed. `primary` is the index of the column with a clustered index, or
 * `__SIZE_MAX__` if no column has a clustered index.
 */
typedef struct Table {
  char name[MAX_SIZE_NAME];
  Column *columns;
  size_t n_cols;
  size_t n_inited_cols;
  size_t n_rows;
  size_t capacity;
  size_t primary;
} Table;

/**
 * The database struct.
 *
 * This struct contains the name of the database, a pointer to the array of
 * tables, the number of tables, and the capacity of how many tables can be
 * stored in the current database. The capacity of the database will be adjusted
 * dynamically as needed.
 */
typedef struct Db {
  char name[MAX_SIZE_NAME];
  Table *tables;
  size_t n_tables;
  size_t capacity;
} Db;

/**
 * The current database in use.
 *
 * In our system we support only one database at a time, thus making this a
 * global pointer. Creation of database means initializing this pointer.
 */
extern Db *__DB__;

typedef enum DbSchemaStatus {
  // The operation was successful.
  DB_SCHEMA_STATUS_OK,
  // The database already exists while it should not.
  DB_SCHEMA_STATUS_DB_ALREADY_EXISTS,
  // The database does not exist while it should.
  DB_SCHEMA_STATUS_DB_NOT_EXIST,
  // The table already exists while it should not.
  DB_SCHEMA_STATUS_TABLE_ALREADY_EXISTS,
  // The table does not exist while it should.
  DB_SCHEMA_STATUS_TABLE_NOT_EXIST,
  // The table has no more capacity for new columns.
  DB_SCHEMA_STATUS_TABLE_FULL,
  // The table does not have the specified number of columns initialized.
  DB_SCHEMA_STATUS_TABLE_NOT_FULL,
  // The column already exists in the table while it should not.
  DB_SCHEMA_STATUS_COLUMN_ALREADY_EXISTS,
  // The column does not exist while it should.
  DB_SCHEMA_STATUS_COLUMN_NOT_EXIST,
  // An index already exists on the column while it should not.
  DB_SCHEMA_STATUS_INDEX_ALREADY_EXISTS,
  // A clustered index already exists in the table while it should not.
  DB_SCHEMA_STATUS_CLUSTERED_INDEX_ALREADY_EXISTS,
  // The variable does not include a table component while it should.
  DB_SCHEMA_STATUS_VAR_NO_TABLE,
  // The variable does not include a column component while it should.
  DB_SCHEMA_STATUS_VAR_NO_COLUMN,
  // Failed to allocate memory for some object.
  DB_SCHEMA_STATUS_ALLOC_FAILED,
  // Failed to expand memory for some object.
  DB_SCHEMA_STATUS_ALLOC_EXPAND_FAILED,
  // Failed to shrink memory for some object.
  DB_SCHEMA_STATUS_ALLOC_SHRINK_FAILED,
  // Failed to reallocate memory for some object.
  DB_SCHEMA_STATUS_REALLOC_FAILED,
  // CSV header is not valid for loading.
  DB_SCHEMA_STATUS_CSV_INVALID_HEADER,
  // Parallelization is requested by not initialized.
  DB_SCHEMA_STATUS_PARALLEL_NOT_INITIALIZED,
  // Internal execution error for none of the reasons above.
  DB_SCHEMA_STATUS_INTERNAL_ERROR,
} DbSchemaStatus;

/**
 * Launch the database system.
 *
 * This function loads the database if it is persisted. It returns 0 on success
 * and -1 on failure.
 */
int system_launch();

/**
 * Shutdown the database system.
 *
 * Before actually shutting down this function performs cleanup operations,
 * including database persistence and memory deallocation. It returns 0 on
 * success and -1 on failure.
 */
int system_shutdown();

/**
 * Format the status code into a human-readable string.
 */
char *format_status(DbSchemaStatus status);

/**
 * Look up the database specified by the database variable.
 *
 * This function returns the status code of the operation. On success, the found
 * database is the global pointer __DB__.
 */
DbSchemaStatus lookup_db(char *db_var);

/**
 * Look up the table specified by the table variable.
 *
 * This function returns the status code of the operation and sets the pointer
 * to the found table on success.
 */
DbSchemaStatus lookup_table(char *table_var, Table **table);

/**
 * Look up the column specified by the column variable.
 *
 * This function returns the status code of the operation and sets the pointers
 * to the table that the found column belongs to, and the i indicating that the
 * column is the ith initialized column in the table, respectively on success.
 */
DbSchemaStatus lookup_column(char *column_var, Table **table,
                             size_t *ith_column);

/**
 * Expand the table if the increment exceeds the capacity.
 *
 * The increment is the number of rows that the table needs to newly accomodate
 * on top of the current status. If that exceeds the current capacity, the table
 * will be expanded exponentially by the predefined factor until there is
 * sufficient capacity to accomodate the need. If the table is already large
 * enough, this function is no-op. This function returns the status code of the
 * operation.
 */
DbSchemaStatus maybe_expand_table(Table *table, size_t increment);

/**
 * Shrink the table if the number of rows is too small compared to the capacity.
 *
 * This function will shrink the table if the number of rows multiplied by the
 * expand factor, is smaller than the current capacity divided by the shrink
 * factor. In this case the table capacity will be shrunk exponentially by the
 * predefined factor until the condition is satisfied. If the table is already
 * small enough, this function is no-op. This function returns the status code
 * of the operation.
 */
DbSchemaStatus maybe_shrink_table(Table *table);

/**
 * Free the current database.
 *
 * This will free all internal structures, including tables, columns, and column
 * indexes, then free the database itself and set back to NULL.
 */
void free_db();

#endif /* DB_SCHEMA_H__ */
