/**
 * @file cmdfetch.c
 * @implements cmdfetch.h
 */

#include "cmdfetch.h"

/**
 * @implements cmdfetch
 */
GeneralizedValvec *cmdfetch(GeneralizedValvec *valvec,
                            GeneralizedPosvec *posvec, DbSchemaStatus *status) {
  int *data = valvec->valvec_type == GENERALIZED_VALVEC_TYPE_COLUMN
                  ? valvec->valvec_pointer.column->data
                  : valvec->valvec_pointer.partial_column->values;

  // Allocate memory for the values according to meta information stored in the
  // generalized position vector
  size_t length = posvec->posvec_pointer.index_array->n_indices;
  int *values = malloc(length * sizeof(int));
  if (values == NULL) {
    *status = DB_SCHEMA_STATUS_ALLOC_FAILED;
    return NULL;
  }

  // Fetch the values at the specified positions
  for (size_t i = 0; i < length; i++) {
    values[i] = data[posvec->posvec_pointer.index_array->indices[i]];
  }

  // Wrap the values in a value vector
  GeneralizedValvec *new_valvec = wrap_partial_column(values, length, status);
  if (*status != DB_SCHEMA_STATUS_OK) {
    free(values);
    return NULL;
  }
  return new_valvec;
}
