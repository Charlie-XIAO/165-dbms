/**
 * @file cmdprint.c
 * @implements cmdprint.h
 */

#include <stdio.h>
#include <string.h>

#include "cmdprint.h"

/**
 * @implements cmdprint_vecs
 */
char *cmdprint_vecs(GeneralizedValvecHandle **valvec_handles,
                    size_t n_valvec_handles, size_t *output_len,
                    DbSchemaStatus *status) {
  size_t result_buffer_size = DEFAULT_BUFFER_SIZE;
  char *result = malloc(result_buffer_size * sizeof(char));
  if (result == NULL) {
    *status = DB_SCHEMA_STATUS_ALLOC_FAILED;
    *output_len = 0;
    return NULL;
  }

  // Temporary buffer to hold each integer value as a string:
  // - 10 characters for the integer value
  // - 1 character for the sign
  // - 1 character for the comma separator or newline character
  // - 1 character for the null terminator
  char temp[13];
  result[0] = '\0';
  size_t current_len = 0;

  for (size_t i = 0; i < valvec_handles[0]->generalized_valvec.valvec_length;
       i++) {
    for (size_t j = 0; j < n_valvec_handles; j++) {
      int *data =
          valvec_handles[j]->generalized_valvec.valvec_type ==
                  GENERALIZED_VALVEC_TYPE_COLUMN
              ? valvec_handles[j]
                    ->generalized_valvec.valvec_pointer.column->data
              : valvec_handles[j]
                    ->generalized_valvec.valvec_pointer.partial_column->values;

      // Format the integer as a string
      int len;
      if (j == n_valvec_handles - 1) {
        len = snprintf(temp, sizeof(temp), "%d\n", data[i]);
      } else {
        len = snprintf(temp, sizeof(temp), "%d,", data[i]);
      }

      // Check if the result buffer needs to grow
      if (current_len + len + 1 >= result_buffer_size) {
        result_buffer_size *= 2; // XXX: hard-coded factor
        result = realloc(result, result_buffer_size * sizeof(char));
        if (result == NULL) {
          *status = DB_SCHEMA_STATUS_REALLOC_FAILED;
          *output_len = 0;
          return NULL;
        }
      }

      // Append the formatted integer to the result buffer; note we use memcpy
      // instead of strncpy, because we do not need null-termination in the
      // middle (it will be guaranteed at the end), and do not want GCC to warn
      // -Wstringop-truncation under high optimization levels
      memcpy(result + current_len, temp, len * sizeof(char));
      current_len += len;
    }
  }

  // Remove the trailing newline character; in the corner case where the value
  // vectors are of length 0 this is no-op
  if (current_len > 0) {
    result[current_len - 1] = '\0';
  }

  *status = DB_SCHEMA_STATUS_OK;
  *output_len = current_len;
  return result;
}

/**
 * @implements cmdprint_vals
 */
char *cmdprint_vals(NumericValueHandle **numval_handles,
                    size_t n_numval_handles, size_t *output_len,
                    DbSchemaStatus *status) {
  // XXX: hard-coded size that is definitely sufficient for int, long long int,
  // and double (with two decimal places)
  size_t result_buffer_size = DEFAULT_BUFFER_SIZE;
  char *result = malloc(result_buffer_size * sizeof(char));
  if (result == NULL) {
    *status = DB_SCHEMA_STATUS_ALLOC_FAILED;
    *output_len = 0;
    return NULL;
  }

  // Temporary buffer to hold each numeric value as a string; the largest double
  // can be ~10^308 which is much larger than integer types:
  // - 309 characters for integer part
  // - 2 characters for the two decimal places
  // - 1 character for the decimal point
  // - 1 character for the sign
  // - 1 character for the comma separator
  // - 1 character for the null terminator
  char temp[315];
  result[0] = '\0';
  size_t current_len = 0;

  for (size_t i = 0; i < n_numval_handles; i++) {
    // Format the numeric value according to its type; except for the last value
    // all are followed by a comma separator; the last value is automatically
    // null-terminated
    int len = 0;
    if (i == n_numval_handles - 1) {
      switch (numval_handles[i]->type) {
      case NUMERIC_VALUE_TYPE_INT:
        len = snprintf(temp, sizeof(temp), "%d",
                       numval_handles[i]->value.int_value);
        break;
      case NUMERIC_VALUE_TYPE_LONG_LONG:
        len = snprintf(temp, sizeof(temp), "%lld",
                       numval_handles[i]->value.long_long_value);
        break;
      case NUMERIC_VALUE_TYPE_DOUBLE:
        len = snprintf(temp, sizeof(temp), "%.2f",
                       numval_handles[i]->value.double_value);
        break;
      }
    } else {
      switch (numval_handles[i]->type) {
      case NUMERIC_VALUE_TYPE_INT:
        len = snprintf(temp, sizeof(temp), "%d,",
                       numval_handles[i]->value.int_value);
        break;
      case NUMERIC_VALUE_TYPE_LONG_LONG:
        len = snprintf(temp, sizeof(temp), "%lld,",
                       numval_handles[i]->value.long_long_value);
        break;
      case NUMERIC_VALUE_TYPE_DOUBLE:
        len = snprintf(temp, sizeof(temp), "%.2f,",
                       numval_handles[i]->value.double_value);
        break;
      }
    }

    // Check if the result buffer needs to grow
    if (current_len + len + 1 >= result_buffer_size) {
      result_buffer_size *= 2; // XXX: hard-coded factor
      result = realloc(result, result_buffer_size * sizeof(char));
      if (result == NULL) {
        *status = DB_SCHEMA_STATUS_REALLOC_FAILED;
        *output_len = 0;
        return NULL;
      }
    }

    // Append the formatted numeric value to the result buffer
    strncpy(result + current_len, temp, len);
    current_len += len;
  }

  *status = DB_SCHEMA_STATUS_OK;
  *output_len = current_len;
  return result;
}
