/**
 * @file client_context.c
 * @implements client_context.h
 */

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "client_context.h"
#include "io.h"

/**
 * @implements init_client_context
 */
ClientContext *init_client_context() {
  ClientContext *context = malloc(sizeof(ClientContext));
  if (context == NULL) {
    return NULL;
  }

  // Initialize the value vector handles
  context->valvec_handles = malloc(sizeof(GeneralizedValvecHandle) *
                                   INIT_NUM_HANDLES_IN_CLIENT_CONTEXT);
  if (context->valvec_handles == NULL) {
    free(context);
    return NULL;
  }
  context->n_valvec_handles = 0;
  context->valvec_handles_capacity = INIT_NUM_HANDLES_IN_CLIENT_CONTEXT;

  // Initialize the position vector handles
  context->posvec_handles = malloc(sizeof(GeneralizedPosvecHandle) *
                                   INIT_NUM_HANDLES_IN_CLIENT_CONTEXT);
  if (context->posvec_handles == NULL) {
    free(context->valvec_handles);
    free(context);
    return NULL;
  }
  context->n_posvec_handles = 0;
  context->posvec_handles_capacity = INIT_NUM_HANDLES_IN_CLIENT_CONTEXT;

  // Initialize the numeric value handles
  context->numval_handles =
      malloc(sizeof(NumericValueHandle) * INIT_NUM_HANDLES_IN_CLIENT_CONTEXT);
  if (context->numval_handles == NULL) {
    free(context->valvec_handles);
    free(context->posvec_handles);
    free(context);
    return NULL;
  }
  context->n_numval_handles = 0;
  context->numval_handles_capacity = INIT_NUM_HANDLES_IN_CLIENT_CONTEXT;

  return context;
}

/**
 * @implements free_client_context
 */
void free_client_context(ClientContext *context) {
  // Free internals of each value vector handle and the handles themselves
  for (size_t i = 0; i < context->n_valvec_handles; i++) {
    switch (context->valvec_handles[i].generalized_valvec.valvec_type) {
    case GENERALIZED_VALVEC_TYPE_COLUMN:
      // Client context do not own the column, and this case is theoretically
      // unreachable since such value vectors are only temporary
      break;
    case GENERALIZED_VALVEC_TYPE_PARTIAL_COLUMN:
      free(context->valvec_handles[i]
               .generalized_valvec.valvec_pointer.partial_column->values);
      free(context->valvec_handles[i]
               .generalized_valvec.valvec_pointer.partial_column);
      break;
    }
  }
  free(context->valvec_handles);

  // Free internals of each position vector handle and the handles themselves
  for (size_t i = 0; i < context->n_posvec_handles; i++) {
    switch (context->posvec_handles[i].generalized_posvec.posvec_type) {
    case GENERALIZED_POSVEC_TYPE_INDEX_ARRAY:
      free(context->posvec_handles[i]
               .generalized_posvec.posvec_pointer.index_array->indices);
      free(context->posvec_handles[i]
               .generalized_posvec.posvec_pointer.index_array);
      break;
    case GENERALIZED_POSVEC_TYPE_BOOLEAN_MASK:
      free(context->posvec_handles[i]
               .generalized_posvec.posvec_pointer.boolean_mask->mask);
      free(context->posvec_handles[i]
               .generalized_posvec.posvec_pointer.boolean_mask);
      break;
    }
  }
  free(context->posvec_handles);

  // Free the numeric value handles (no internals to free)
  free(context->numval_handles);

  // Free the client context itself
  free(context);
}

/**
 * @implements lookup_valvec_handle
 */
GeneralizedValvecHandle *
lookup_valvec_handle(ClientContext *context, char *name, bool consider_column) {
  for (size_t i = 0; i < context->n_valvec_handles; i++) {
    if (strcmp(context->valvec_handles[i].name, name) == 0) {
      return &context->valvec_handles[i];
    }
  }

  // If we do not want to consider columns as well, we can return NULL already
  if (!consider_column) {
    return NULL;
  }

  // Look up the column in the database
  Table *table;
  size_t ith_column; // Unused placeholder
  DbSchemaStatus lookup_status = lookup_column(name, &table, &ith_column);
  if (lookup_status != DB_SCHEMA_STATUS_OK) {
    return NULL;
  }
  GeneralizedValvecHandle *valvec_handle =
      malloc(sizeof(GeneralizedValvecHandle));
  valvec_handle->generalized_valvec.valvec_type =
      GENERALIZED_VALVEC_TYPE_COLUMN;
  valvec_handle->generalized_valvec.valvec_length = table->n_rows;
  valvec_handle->generalized_valvec.valvec_pointer.column =
      &table->columns[ith_column];
  return valvec_handle;
}

/**
 * @implements lookup_posvec_handle
 */
GeneralizedPosvecHandle *lookup_posvec_handle(ClientContext *context,
                                              char *name) {
  for (size_t i = 0; i < context->n_posvec_handles; i++) {
    if (strcmp(context->posvec_handles[i].name, name) == 0) {
      return &context->posvec_handles[i];
    }
  }
  return NULL;
}

/**
 * @implements lookup_numval_handle
 */
NumericValueHandle *lookup_numval_handle(ClientContext *context, char *name) {
  for (size_t i = 0; i < context->n_numval_handles; i++) {
    if (strcmp(context->numval_handles[i].name, name) == 0) {
      return &context->numval_handles[i];
    }
  }
  return NULL;
}

/**
 * @implements insert_valvec_handle
 */
DbSchemaStatus insert_valvec_handle(ClientContext *context, char *name,
                                    GeneralizedValvec *valvec) {
  // Note that we are not considering columns here, since we can only overwrite
  // partial columns in the client context but not touch the actual database
  GeneralizedValvecHandle *valvec_handle =
      lookup_valvec_handle(context, name, false);

  if (valvec_handle != NULL) {
    // There is already a handle with the same name so we need to overwrite it;
    // we free the original first then point to the new one
    switch (valvec_handle->generalized_valvec.valvec_type) {
    case GENERALIZED_VALVEC_TYPE_COLUMN:
      // Unreachable because we are not considering columns
      assert(0 && "Unreachable code");
      break;
    case GENERALIZED_VALVEC_TYPE_PARTIAL_COLUMN:
      free(valvec_handle->generalized_valvec.valvec_pointer.partial_column
               ->values);
      free(valvec_handle->generalized_valvec.valvec_pointer.partial_column);
      valvec_handle->generalized_valvec = *valvec;
      free(valvec);
      break;
    }
  } else {
    // There is no handle with the same name so we need to create a new one;
    // before that we need to check if we have enough capacity
    if (context->n_valvec_handles == context->valvec_handles_capacity) {
      size_t new_capacity =
          context->valvec_handles_capacity * EXPAND_FACTOR_CLIENT_CONTEXT;
      context->valvec_handles =
          realloc(context->valvec_handles,
                  sizeof(GeneralizedValvecHandle) * new_capacity);
      if (context->valvec_handles == NULL) {
        return DB_SCHEMA_STATUS_REALLOC_FAILED;
      }
      context->valvec_handles_capacity = new_capacity;
    }

    // Initialize a new handle
    GeneralizedValvecHandle valvec_handle;
    strncpy(valvec_handle.name, name, HANDLE_MAX_SIZE - 1);
    valvec_handle.name[HANDLE_MAX_SIZE - 1] = '\0';
    valvec_handle.generalized_valvec = *valvec;
    free(valvec);

    // Add the handle to the context
    context->valvec_handles[context->n_valvec_handles++] = valvec_handle;
  }

  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements insert_posvec_handle
 */
DbSchemaStatus insert_posvec_handle(ClientContext *context, char *name,
                                    GeneralizedPosvec *posvec) {
  GeneralizedPosvecHandle *posvec_handle = lookup_posvec_handle(context, name);

  if (posvec_handle != NULL) {
    // There is already a handle with the same name so we need to overwrite it;
    // we free the original first then point to the new one
    switch (posvec_handle->generalized_posvec.posvec_type) {
    case GENERALIZED_POSVEC_TYPE_INDEX_ARRAY:
      free(posvec_handle->generalized_posvec.posvec_pointer.index_array
               ->indices);
      free(posvec_handle->generalized_posvec.posvec_pointer.index_array);
      break;
    case GENERALIZED_POSVEC_TYPE_BOOLEAN_MASK:
      free(posvec_handle->generalized_posvec.posvec_pointer.boolean_mask->mask);
      free(posvec_handle->generalized_posvec.posvec_pointer.boolean_mask);
      break;
    }
    posvec_handle->generalized_posvec = *posvec;
    free(posvec);
  } else {
    // There is no handle with the same name so we need to create a new one;
    // before that we need to check if we have enough capacity
    if (context->n_posvec_handles == context->posvec_handles_capacity) {
      size_t new_capacity =
          context->posvec_handles_capacity * EXPAND_FACTOR_CLIENT_CONTEXT;
      context->posvec_handles =
          realloc(context->posvec_handles,
                  sizeof(GeneralizedPosvecHandle) * new_capacity);
      if (context->posvec_handles == NULL) {
        return DB_SCHEMA_STATUS_REALLOC_FAILED;
      }
      context->posvec_handles_capacity = new_capacity;
    }

    // Initialize a new handle
    GeneralizedPosvecHandle posvec_handle;
    strncpy(posvec_handle.name, name, HANDLE_MAX_SIZE - 1);
    posvec_handle.name[HANDLE_MAX_SIZE - 1] = '\0';
    posvec_handle.generalized_posvec = *posvec;
    free(posvec);

    // Add the handle to the context
    context->posvec_handles[context->n_posvec_handles++] = posvec_handle;
  }

  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements insert_numval_handle
 */
DbSchemaStatus insert_numval_handle(ClientContext *context, char *name,
                                    NumericValueType type, NumericValue value) {
  NumericValueHandle *numval_handle = lookup_numval_handle(context, name);

  if (numval_handle != NULL) {
    // There is already a handle with the same name so we need to overwrite it
    numval_handle->type = type;
    numval_handle->value = value;
  } else {
    // There is no handle with the same name so we need to create a new one;
    // before that we need to check if we have enough capacity
    if (context->n_numval_handles == context->numval_handles_capacity) {
      size_t new_capacity =
          context->numval_handles_capacity * EXPAND_FACTOR_CLIENT_CONTEXT;
      context->numval_handles = realloc(
          context->numval_handles, sizeof(NumericValueHandle) * new_capacity);
      if (context->numval_handles == NULL) {
        return DB_SCHEMA_STATUS_REALLOC_FAILED;
      }
      context->numval_handles_capacity = new_capacity;
    }

    // Initialize a new handle
    NumericValueHandle numval_handle;
    strncpy(numval_handle.name, name, HANDLE_MAX_SIZE - 1);
    numval_handle.name[HANDLE_MAX_SIZE - 1] = '\0';
    numval_handle.type = type;
    numval_handle.value = value;

    // Add the handle to the context
    context->numval_handles[context->n_numval_handles++] = numval_handle;
  }

  return DB_SCHEMA_STATUS_OK;
}

/**
 * @implements wrap_index_array
 */
GeneralizedPosvec *wrap_index_array(size_t *indices, size_t n_indices,
                                    DbSchemaStatus *status) {
  IndexArray *index_array = malloc(sizeof(IndexArray));
  if (index_array == NULL) {
    *status = DB_SCHEMA_STATUS_ALLOC_FAILED;
    return NULL;
  }
  index_array->indices = indices;
  index_array->n_indices = n_indices;

  GeneralizedPosvec *posvec = malloc(sizeof(GeneralizedPosvec));
  if (posvec == NULL) {
    *status = DB_SCHEMA_STATUS_ALLOC_FAILED;
    free(index_array);
    return NULL;
  }
  posvec->posvec_type = GENERALIZED_POSVEC_TYPE_INDEX_ARRAY;
  posvec->posvec_pointer.index_array = index_array;

  *status = DB_SCHEMA_STATUS_OK;
  return posvec;
}

/**
 * @implements wrap_partial_column
 */
GeneralizedValvec *wrap_partial_column(int *values, size_t length,
                                       DbSchemaStatus *status) {
  PartialColumn *partial_column = malloc(sizeof(PartialColumn));
  if (partial_column == NULL) {
    *status = DB_SCHEMA_STATUS_ALLOC_FAILED;
    return NULL;
  }
  partial_column->values = values;

  GeneralizedValvec *valvec = malloc(sizeof(GeneralizedValvec));
  if (valvec == NULL) {
    *status = DB_SCHEMA_STATUS_ALLOC_FAILED;
    free(partial_column);
    return NULL;
  }
  valvec->valvec_type = GENERALIZED_VALVEC_TYPE_PARTIAL_COLUMN;
  valvec->valvec_pointer.partial_column = partial_column;
  valvec->valvec_length = length;

  *status = DB_SCHEMA_STATUS_OK;
  return valvec;
}
