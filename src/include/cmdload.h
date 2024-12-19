/**
 * @file cmdload.h
 *
 * This header contains utilities related to the load command.
 */

#ifndef CMDLOAD_H__
#define CMDLOAD_H__

#include "db_schema.h"

/**
 * Validate the header string of a loaded CSV and grab the table and columns.
 *
 * This function will check that the columns specified in the CSV header are
 * all columns of the same table in the same order. It will store the table in
 * the given pointer. This function returns the status code of the operation.
 */
DbSchemaStatus cmdload_validate_header(char *header, size_t n_cols,
                                       Table **table);

/**
 * Insert multiple new rows into the table.
 *
 * This function returns the status code of the operation. The data is flattened
 * in row-major order (n_rows x n_cols), and the second dimension should match
 * the number of columns in the table. Refer to the load DbOperator for more
 * details on the flattened data array.
 */
DbSchemaStatus cmdload_rows(Table *table, int *data, size_t n_rows);

/**
 * Conclude a load command.
 *
 * This function serves as the post-processing step after all batches of rows
 * are loaded. `n_rows` is the number of rows inserted during the whole load
 * process, i.e., originally the table has `table->n_rows - n_rows` rows and now
 * it has `table->n_rows` rows. This function returns the status code of the
 * operation.
 */
DbSchemaStatus cmdload_conclude(Table *table, size_t n_cumu_rows);

#endif /* CMDLOAD_H__ */
