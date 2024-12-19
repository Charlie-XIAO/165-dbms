/**
 * @file cmdcreate.h
 *
 * This header contains utilities related to the create command.
 */

#ifndef CMDCREATE_H__
#define CMDCREATE_H__

#include "db_schema.h"

/**
 * Create a new database.
 *
 * This function creates a new database with the given name. We support only one
 * database at a time, so this essentially just initializes the global pointer
 * to the only database in the system. This function will return the status code
 * of the operation.
 */
DbSchemaStatus cmdcreate_db(char *name);

/**
 * Create a new table in the database.
 *
 * This function creates a new table with the given name and number of columns
 * in the given database and returns the status code of the operation.
 */
DbSchemaStatus cmdcreate_tbl(Db *db, char *name, size_t n_cols);

/**
 * Create a new column in the table.
 *
 * This function creates a new column with the given name in the given table and
 * returns the status code of the operation.
 */
DbSchemaStatus cmdcreate_col(Table *table, char *name);

/**
 * Create an index on the column.
 *
 * This function creates an index on the column with the given type and returns
 * the status code of the operation. If a column already has an index, this is
 * an error. If some column in the table has a clustered index and the type to
 * create is also clustered, this is again an error.
 */
DbSchemaStatus cmdcreate_idx(Table *table, size_t ith_column,
                             ColumnIndexType type);

#endif /* CMDCREATE_H__ */
