/**
 * @file cmdprint.h
 *
 * This header contains utilities related to the print command.
 */

#ifndef CMDPRINT_H__
#define CMDPRINT_H__

#include "client_context.h"

/**
 * Format the value vectors in tabular format.
 *
 * This function makes a tabular printout of the generalized value vector
 * handles, each representing a column in the printout. It also sets the output
 * length, which is the actual length of the formatted printout, which can be
 * smaller than the allocated memory. Note that this function assumes that all
 * value vectors have the same length. If the operation fails, NULL is returned
 * and output length is set to 0. The status code is properly set.
 */
char *cmdprint_vecs(GeneralizedValvecHandle **valvec_handles,
                    size_t n_valvec_handles, size_t *output_len,
                    DbSchemaStatus *status);

/**
 * Format the numeric values in array format.
 *
 * This function makes a comma-separated string representation of the numeric
 * value handles. It also sets the output length, which is the actual length of
 * the formatted printout, which can be smaller than the allocated memory.
 * Floating-point numbers are formatted by rounding to two decimal places. If
 * the operation fails, NULL is returned and output length is set to 0. The
 * status code is properly set.
 */
char *cmdprint_vals(NumericValueHandle **numval_handles,
                    size_t n_numval_handles, size_t *output_len,
                    DbSchemaStatus *status);

#endif /* CMDPRINT_H__ */
