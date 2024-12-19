/**
 * @file binsearch.c
 * @implements binsearch.h
 */

#include <limits.h>

#include "binsearch.h"

/**
 * Macro for generating the binary search functions.
 */
#define _BINSEARCH(name, comparison_op)                                        \
  size_t name(int *arr, long key, size_t size) {                               \
    if (key == LONG_MAX) {                                                     \
      return size;                                                             \
    } else if (key == LONG_MIN) {                                              \
      return 0;                                                                \
    }                                                                          \
                                                                               \
    size_t min_idx = 0, max_idx = size;                                        \
    while (min_idx < max_idx) {                                                \
      size_t mid_idx = min_idx + ((max_idx - min_idx) >> 1);                   \
      int mid_val = arr[mid_idx];                                              \
                                                                               \
      if (mid_val comparison_op key) {                                         \
        min_idx = mid_idx + 1;                                                 \
      } else {                                                                 \
        max_idx = mid_idx;                                                     \
      }                                                                        \
    }                                                                          \
                                                                               \
    return min_idx;                                                            \
  }

/**
 * Macro for generating the arg binary search functions.
 */
#define _ABINSEARCH(name, comparison_op)                                       \
  size_t name(int *arr, long key, size_t *sort, size_t size) {                 \
    if (key == LONG_MAX) {                                                     \
      return size;                                                             \
    } else if (key == LONG_MIN) {                                              \
      return 0;                                                                \
    }                                                                          \
                                                                               \
    size_t min_idx = 0, max_idx = size;                                        \
    while (min_idx < max_idx) {                                                \
      size_t mid_idx = min_idx + ((max_idx - min_idx) >> 1);                   \
      int mid_val = arr[sort[mid_idx]];                                        \
                                                                               \
      if (mid_val comparison_op key) {                                         \
        min_idx = mid_idx + 1;                                                 \
      } else {                                                                 \
        max_idx = mid_idx;                                                     \
      }                                                                        \
    }                                                                          \
                                                                               \
    return min_idx;                                                            \
  }

_BINSEARCH(_leftbinsearch, <)
_BINSEARCH(_rightbinsearch, <=)

_ABINSEARCH(_aleftbinsearch, <)
_ABINSEARCH(_arightbinsearch, <=)

/**
 * @implements binsearch
 */
size_t binsearch(int *arr, long key, size_t size, bool align_left) {
  if (align_left) {
    return _leftbinsearch(arr, key, size);
  } else {
    return _rightbinsearch(arr, key, size);
  }
}

/**
 * @implements abinsearch
 */
size_t abinsearch(int *arr, long key, size_t *sort, size_t size,
                  bool align_left) {
  if (align_left) {
    return _aleftbinsearch(arr, key, sort, size);
  } else {
    return _arightbinsearch(arr, key, sort, size);
  }
}
