/**
 * @file cmdaddsub.c
 * @implements cmdaddsub.h
 */

#include "cmdaddsub.h"

/**
 * @implements cmdaddsub
 */
GeneralizedValvec *cmdaddsub(GeneralizedValvec *valvec1,
                             GeneralizedValvec *valvec2, bool is_add,
                             DbSchemaStatus *status) {
  int *data1 = valvec1->valvec_type == GENERALIZED_VALVEC_TYPE_COLUMN
                   ? valvec1->valvec_pointer.column->data
                   : valvec1->valvec_pointer.partial_column->values;
  int *data2 = valvec2->valvec_type == GENERALIZED_VALVEC_TYPE_COLUMN
                   ? valvec2->valvec_pointer.column->data
                   : valvec2->valvec_pointer.partial_column->values;

  // Allocate memory for the result values according to the meta information
  // stored in the generalized value vectors
  size_t length = valvec1->valvec_length;
  int *values = malloc(length * sizeof(int));
  if (values == NULL) {
    *status = DB_SCHEMA_STATUS_ALLOC_FAILED;
    return NULL;
  }

  // Perform the addition or subtraction
  if (is_add) {
    for (size_t i = 0; i < length; i++) {
      values[i] = data1[i] + data2[i];
    }
  } else {
    for (size_t i = 0; i < length; i++) {
      values[i] = data1[i] - data2[i];
    }
  }

  // Wrap the result values in a value vector
  GeneralizedValvec *new_valvec = wrap_partial_column(values, length, status);
  if (*status != DB_SCHEMA_STATUS_OK) {
    free(values);
    return NULL;
  }
  return new_valvec;
}
