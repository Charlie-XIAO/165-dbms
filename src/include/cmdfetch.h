/**
 * @file cmdfetch.h
 *
 * This header contains utilities related to the fetch command.
 */

#ifndef CMDFETCH_H__
#define CMDFETCH_H__

#include "client_context.h"

/**
 * Fetch the values at specified positions of a value vector.
 *
 * This function returns the fetched values as a generalized value vector
 * wrapping a partial column. If the operation fails, NULL is returned. The
 * status code is properly set.
 */
GeneralizedValvec *cmdfetch(GeneralizedValvec *valvec,
                            GeneralizedPosvec *posvec, DbSchemaStatus *status);

#endif /* CMDFETCH_H__ */
