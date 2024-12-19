/**
 * @file cmdaddsub.h
 *
 * This header contains utilities related to the add and sub commands.
 */

#ifndef CMDADDSUB_H__
#define CMDADDSUB_H__

#include "client_context.h"

/**
 * Add or subtract two value vectors.
 *
 * This function adds or subtracts the values in the two value vectors and
 * returns the wrapped result values as a generalized value vector wrapping a
 * partial column. The `is_add` flag determines whether addition or subtraction
 * is performed. If the operation fails, NULL is returned. The status code is
 * properly set.
 */
GeneralizedValvec *cmdaddsub(GeneralizedValvec *valvec1,
                             GeneralizedValvec *valvec2, bool is_add,
                             DbSchemaStatus *status);

#endif /* CMDADDSUB_H__ */
