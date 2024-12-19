/**
 * @file cmdjoin.h
 *
 * This header contains utilities related to the join command.
 */

#ifndef CMDJOIN_H__
#define CMDJOIN_H__

#include "client_context.h"

/**
 * Inner join two value vectors using nested-loop algorithm.
 *
 * This function takes two value vectors to join and two position vectors that
 * maps to the positions of the values in the value vectors. The function set
 * the two output position vectors such that they map to the positions of the
 * joined values, respectively. This function returns the status of the
 * operation.
 */
DbSchemaStatus cmdjoin_nested_loop(GeneralizedValvec *valvec1,
                                   GeneralizedValvec *valvec2,
                                   GeneralizedPosvec *posvec1,
                                   GeneralizedPosvec *posvec2,
                                   GeneralizedPosvec **posvec_out1,
                                   GeneralizedPosvec **posvec_out2);

/**
 * Inner join two value vectors using naive-hash algorithm.
 *
 * This function takes two value vectors to join and two position vectors that
 * maps to the positions of the values in the value vectors. The function set
 * the two output position vectors such that they map to the positions of the
 * joined values, respectively. This function returns the status of the
 * operation.
 */
DbSchemaStatus cmdjoin_naive_hash(GeneralizedValvec *valvec1,
                                  GeneralizedValvec *valvec2,
                                  GeneralizedPosvec *posvec1,
                                  GeneralizedPosvec *posvec2,
                                  GeneralizedPosvec **posvec_out1,
                                  GeneralizedPosvec **posvec_out2);

/**
 * Inner join two value vectors using grace-hash algorithm.
 *
 * The grace-hash algorithm uses the radix hash join algorithm internally. This
 * function takes two value vectors to join and two position vectors that maps
 * to the positions of the values in the value vectors. The function set the two
 * output position vectors such that they map to the positions of the joined
 * values, respectively. This function returns the status of the operation.
 */
DbSchemaStatus cmdjoin_grace_hash(GeneralizedValvec *valvec1,
                                  GeneralizedValvec *valvec2,
                                  GeneralizedPosvec *posvec1,
                                  GeneralizedPosvec *posvec2,
                                  GeneralizedPosvec **posvec_out1,
                                  GeneralizedPosvec **posvec_out2);

/**
 * Inner join two value vectors using hash algorithm.
 *
 * The hash algorithm uses naive-hash for small data sizes and grace-hash for
 * large data sizes. This function takes two value vectors to join and two
 * position vectors that maps to the positions of the values in the value
 * vectors. The function set the two output position vectors such that they map
 * to the positions of the joined values, respectively. This function returns
 * the status of the operation.
 */
DbSchemaStatus
cmdjoin_hash(GeneralizedValvec *valvec1, GeneralizedValvec *valvec2,
             GeneralizedPosvec *posvec1, GeneralizedPosvec *posvec2,
             GeneralizedPosvec **posvec_out1, GeneralizedPosvec **posvec_out2);

#endif /* CMDJOIN_H__ */
