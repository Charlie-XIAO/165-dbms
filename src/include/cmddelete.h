/**
 * @file cmddelete.h
 *
 * This header contains utilities related to the relational delete command.
 */

#ifndef CMDDELETE_H__
#define CMDDELETE_H__

#include "client_context.h"

/**
 * Delete rows at specified positions from a table.
 *
 * This function deletes the rows at the specified positions from the given
 * table and returns the status code of the operation.
 */
DbSchemaStatus cmddelete(Table *table, GeneralizedPosvec *posvec);

#endif /* CMDDELETE_H__ */
