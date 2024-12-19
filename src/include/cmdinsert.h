/**
 * @file cmdinsert.h
 *
 * This header contains utilities related to the relational insert command.
 */

#ifndef CMDINSERT_H__
#define CMDINSERT_H__

#include "db_schema.h"

/**
 * Insert a new row into the table.
 *
 * This function returns the status code of the operation. The values need to
 * match the number of columns in the table.
 */
DbSchemaStatus cmdinsert(Table *table, int *values);

#endif /* CMDINSERT_H__ */
