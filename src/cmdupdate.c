/**
 * @file cmdupdate.c
 * @implements cmdupdate.h
 */

#include <assert.h>

#include "cindex.h"
#include "cmdupdate.h"

DbSchemaStatus cmdupdate(Table *table, size_t ith_column,
                         GeneralizedPosvec *posvec, int value) {
  Column *column = &table->columns[ith_column];
  size_t *indices = posvec->posvec_pointer.index_array->indices;
  size_t n_indices = posvec->posvec_pointer.index_array->n_indices;

  for (size_t i = 0; i < n_indices; i++) {
    column->data[indices[i]] = value;
  }

  // Free the old index and reinitialize it; we cannot skip sorting for
  // clustered indexes because our update does not preserve sorted data
  free_cindex(column);
  return init_cindex(table, column, false);
}
