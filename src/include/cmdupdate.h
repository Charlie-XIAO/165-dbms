/**
 * @file cmdupdate.h
 *
 * This header contains utilities related to the relational update command.
 */

#ifndef CMDUPDATE_H__
#define CMDUPDATE_H__

#include "client_context.h"

/**
 * Update specific rows in a column to a new value.
 *
 * The column to update is specified by `table` and `ith_column`. The rows to
 * update are specified by `posvec`. The specific rows are update to the same
 * new given value. This function returns the status code of the operation.
 */
DbSchemaStatus cmdupdate(Table *table, size_t ith_column,
                         GeneralizedPosvec *posvec, int value);

#endif /* CMDUPDATE_H__ */
