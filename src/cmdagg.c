/**
 * @file cmdagg.c
 * @implements cmdagg.h
 */

#include <assert.h>

#include "cmdagg.h"
#include "scan.h"

/**
 * @implements cmdagg
 */
NumericValue cmdagg(GeneralizedValvec *valvec, int type_code,
                    DbSchemaStatus *status) {
  ScanContext ctx = init_empty_scan_context();

  // Determine the (single) flag to use for the shared scan function
  int flag;
  switch (type_code) {
  case 0:
    flag = SCAN_CALLBACK_MIN_FLAG;
    break;
  case 1:
    flag = SCAN_CALLBACK_MAX_FLAG;
    break;
  case 2:
  case 3:
    // SUM and AVG both fall into this category; for AVG we will do additional
    // post-processing after the scan
    flag = SCAN_CALLBACK_SUM_FLAG;
    break;
  default:
    assert(0 && "Invalid type code");
  }

  // Perform the actual scanning and aggregation
  *status = shared_scan(valvec, NULL, &ctx, flag);
  if (*status != DB_SCHEMA_STATUS_OK) {
    return (NumericValue){0};
  }

  // Post-processing
  NumericValue agg_result;
  switch (type_code) {
  case 0:
    agg_result.int_value = ctx.min_result;
    break;
  case 1:
    agg_result.int_value = ctx.max_result;
    break;
  case 2:
    agg_result.long_long_value = ctx.sum_result;
    break;
  case 3:
    // NaN (when length is 0) should be treated as zero
    agg_result.double_value =
        valvec->valvec_length == 0
            ? 0.0
            : (double)ctx.sum_result / valvec->valvec_length;
    break;
  default:
    assert(0 && "Invalid type code");
  }

  return agg_result;
}
