/**
 * @file cmdselect.h
 *
 * This header contains utilities related to the select command.
 */

#ifndef CMDSELECT_H__
#define CMDSELECT_H__

#include "client_context.h"

/**
 * Select positions from a value vector.
 *
 * This function filters the value vector with the specified lower bound
 * (inclusive) and upper bound (exclusive) and returns the resulting position
 * vector. If the position vector is not provided, the selected positions
 * correspond to the indices of the value vector; otherwise the selected
 * positions mapped by the give position vector. This function returns NULL if
 * the operation fails. The status code is properly set.
 */
GeneralizedPosvec *cmdselect_raw(GeneralizedValvec *valvec,
                                 GeneralizedPosvec *posvec, long lower_bound,
                                 long upper_bound, DbSchemaStatus *status);

/**
 * Select positions from an indexed column.
 *
 * This function has the same functionality as `cmdselect_raw`, but it is
 * specifically designed for value vectors that wrap actual columns which are
 * indexed. This function returns NULL if the operation fails. The status code
 * is properly set.
 */
GeneralizedPosvec *cmdselect_index(Column *column, size_t n_rows,
                                   GeneralizedPosvec *posvec, long lower_bound,
                                   long upper_bound, DbSchemaStatus *status);

#endif /* CMDSELECT_H__ */
