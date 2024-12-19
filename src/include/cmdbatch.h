/**
 * @file cmdbatch.h
 *
 * This header contains utilities related to the batch execute command.
 */

#ifndef CMDBATCH_H__
#define CMDBATCH_H__

#include "client_context.h"

/**
 * Execute a batch of shared scan operations.
 *
 * The generalized value vector and generalized position vector should be shared
 * by all scan operations in the batch to execute. The number of select queries
 * and their lower and upper bounds need to be given. The flag indicates which
 * shared scan operations to be performed. The select results and aggregation
 * results are written to the provided pointers. The status code of the
 * operation is returned.
 */
DbSchemaStatus cmdbatch(GeneralizedValvec *valvec, GeneralizedPosvec *posvec,
                        size_t n_select_queries, long *lower_bound_arr,
                        long *upper_bound_arr, int flags,
                        GeneralizedPosvec **select_results, int *min_result,
                        int *max_result, long long *sum_result);

#endif /* CMDBATCH_H__ */
