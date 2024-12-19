/**
 * @file cmdagg.h
 *
 * This header contains utilities related to the aggregate commands, including
 * min, max, sum, and avg.
 */

#ifndef CMDAGG_H__
#define CMDAGG_H__

#include "client_context.h"

/**
 * Aggregate the values in a value vector.
 *
 * This function aggregates the values in the value vector and returns the
 * aggregation result wrapped as a numeric value. The type code determines the
 * type of the aggregation operation, where 0, 1, 2, 3 correspond to MIN, MAX,
 * SUM, AVG respectively. Average of no elements will be treated as 0.0 instead
 * of NaN. If the operation fails, an all-zero numeric value is returned. The
 * status code is properly set.
 */
NumericValue cmdagg(GeneralizedValvec *valvec, int type_code,
                    DbSchemaStatus *status);

#endif /* CMDAGG_H__ */
