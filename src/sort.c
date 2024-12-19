/**
 * @file sort.c
 * @implements sort.h
 */

#include <limits.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "sort.h"

/**
 * The size needed for the quicksort stack.
 *
 * In the worst case, quicksort would require a depth of log2(n), where n is the
 * number of elements to sort and is at most 2^(#bits in size_t).
 */
#define _QUICKSORT_STACK_DEPTH sizeof(size_t) * CHAR_BIT

/**
 * The cutoff for switching from quicksort to insertion sort.
 *
 * [QUOTE] As with mergesort, it pays to switch to insertion sort for tiny
 * arrays. The optimum value of the cutoff is system-dependent, but any value
 * between 5 and 15 is likely to work well in most situations.
 *
 * Reference:
 * https://algs4.cs.princeton.edu/23quicksort/
 */
#define _QUICKSORT_INSERTION_CUTOFF 15

/**
 * Swap two variables of a given type.
 */
#define _SWAP(a, b, type)                                                      \
  do {                                                                         \
    type _t = a;                                                               \
    a = b;                                                                     \
    b = _t;                                                                    \
  } while (0)

/**
 * Get the most significant bit.
 *
 * This is essentially how many times we can divide the unum by 2, equal to
 * the floor of the base-2 logarithm of the unum.
 *
 * Reference:
 * https://github.com/numpy/numpy/blob/7c0e2e4224c6feb04a2ac4aa851f49a2c2f6189f/numpy/_core/src/common/npy_sort.h.src#L12
 */
static inline int _get_msb(size_t unum) {
  int depth_limit = 0;
  while (unum >>= 1) {
    depth_limit++;
  }
  return depth_limit;
}

/**
 * @implements quicksort
 */
int quicksort(int *arr, size_t size) {
  if (size < 2) {
    return 0; // Nothing to sort
  }

  int pivot;
  int *pleft = arr;
  int *pright = arr + size - 1;
  int *pmid, *pi, *pj, *pk;

  // Stack for storing the partition pointers to avoid deep recursion; each
  // partition requires two pointers, so the stack size is doubled
  int *stack[_QUICKSORT_STACK_DEPTH * 2];
  int **stack_ptr = stack;

  // Depth stack for storing the depths of the partition
  int depth[_QUICKSORT_STACK_DEPTH];
  int *depth_ptr = depth;
  int cdepth = _get_msb(size) * 2;

  while (true) {
    // TODO: Switch to heap sort if depth limit is reached

    while ((pright - pleft) > _QUICKSORT_INSERTION_CUTOFF) {
      // Quicksort partitioning; this is a median-of-three partitioning, where
      // the pivot is the median of the first, middle, and last elements; see
      // https://algs4.cs.princeton.edu/23quicksort/ for reference
      pmid = pleft + ((pright - pleft) >> 1);
      if (*pmid < *pleft)
        _SWAP(*pmid, *pleft, int);
      if (*pright < *pmid)
        _SWAP(*pright, *pmid, int);
      if (*pmid < *pleft)
        _SWAP(*pmid, *pleft, int);

      pivot = *pmid;
      pi = pleft;
      pj = pright - 1;
      _SWAP(*pmid, *pj, int);

      // Main partitioning loop
      while (true) {
        do {
          ++pi; // Move i pointer right until >= pivot
        } while (*pi < pivot);
        do {
          --pj; // Move j pointer left until <= pivot
        } while (*pj > pivot);
        if (pi >= pj) {
          break;
        }
        _SWAP(*pi, *pj, int);
      }
      pk = pright - 1;
      _SWAP(*pi, *pk, int); // Restore pivot to its final place

      // Push the larger partition onto the stack for later processing
      if (pi - pleft < pright - pi) {
        *stack_ptr++ = pi + 1;
        *stack_ptr++ = pright;
        pright = pi - 1;
      } else {
        *stack_ptr++ = pleft;
        *stack_ptr++ = pi - 1;
        pleft = pi + 1;
      }
      *depth_ptr++ = --cdepth; // Decrement depth for this branch
    }

    // The problem size has dropped below the cutoff, so we switch to insertion
    // sort for the remaining elements
    int current;
    for (pi = pleft + 1; pi <= pright; pi++) {
      current = *pi;
      pivot = current;
      pj = pi;
      pk = pi - 1;
      while (pj > pleft && *pk > pivot) {
        *pj-- = *pk--;
      }
      *pj = current;
    }

    // Pop the stack
    if (stack_ptr == stack) {
      break; // Stack is empty, so we are done
    }
    pright = *(--stack_ptr);
    pleft = *(--stack_ptr);
    cdepth = *(--depth_ptr);
  }

  return 0;
}

/**
 * @implements aquicksort
 */
int aquicksort(int *arr, size_t *tosort, size_t size) {
  if (size < 2) {
    return 0; // Nothing to sort
  }

  int pivot;
  int *v = arr;
  size_t *pleft = tosort;
  size_t *pright = tosort + size - 1;
  size_t *pmid, *pi, *pj, *pk;

  // Stack for storing the partition pointers to avoid deep recursion; each
  // partition requires two pointers, so the stack size is doubled
  size_t *stack[_QUICKSORT_STACK_DEPTH * 2];
  size_t **stack_ptr = stack;

  // Depth stack for storing the depths of the partition
  int depth[_QUICKSORT_STACK_DEPTH];
  int *depth_ptr = depth;
  int cdepth = _get_msb(size) * 2;

  while (true) {
    // TODO: Switch to arg heap sort if depth limit is reached

    while ((pright - pleft) > _QUICKSORT_INSERTION_CUTOFF) {
      // Quicksort partitioning; this is a median-of-three partitioning, where
      // the pivot is the median of the first, middle, and last elements; see
      // https://algs4.cs.princeton.edu/23quicksort/ for reference
      pmid = pleft + ((pright - pleft) >> 1);
      if (v[*pmid] < v[*pleft])
        _SWAP(*pmid, *pleft, size_t);
      if (v[*pright] < v[*pmid])
        _SWAP(*pright, *pmid, size_t);
      if (v[*pmid] < v[*pleft])
        _SWAP(*pmid, *pleft, size_t);

      pivot = v[*pmid];
      pi = pleft;
      pj = pright - 1;
      _SWAP(*pmid, *pj, size_t);

      // Main partitioning loop
      while (true) {
        do {
          ++pi; // Move i pointer right until >= pivot
        } while (v[*pi] < pivot);
        do {
          --pj; // Move j pointer left until <= pivot
        } while (v[*pj] > pivot);
        if (pi >= pj) {
          break;
        }
        _SWAP(*pi, *pj, size_t);
      }
      pk = pright - 1;
      _SWAP(*pi, *pk, size_t); // Restore pivot to its final place

      // Push the larger partition onto the stack for later processing
      if (pi - pleft < pright - pi) {
        *stack_ptr++ = pi + 1;
        *stack_ptr++ = pright;
        pright = pi - 1;
      } else {
        *stack_ptr++ = pleft;
        *stack_ptr++ = pi - 1;
        pleft = pi + 1;
      }
      *depth_ptr++ = --cdepth; // Decrement depth for this branch
    }

    // The problem size has dropped below the cutoff, so we switch to insertion
    // sort for the remaining elements
    size_t current;
    for (pi = pleft + 1; pi <= pright; pi++) {
      current = *pi;
      pivot = v[current];
      pj = pi;
      pk = pi - 1;
      while (pj > pleft && v[*pk] > pivot) {
        *pj-- = *pk--;
      }
      *pj = current;
    }

    // Pop the stack
    if (stack_ptr == stack) {
      break; // Stack is empty, so we are done
    }
    pright = *(--stack_ptr);
    pleft = *(--stack_ptr);
    cdepth = *(--depth_ptr);
  }

  return 0;
}

/**
 * Subroutine for merge when the left half is smaller.
 *
 * This subroutine copies the left half to a temporary buffer and merges from
 * left to right in the original data array. Given the left half is smaller,
 * this is more memory-efficient, and it is more likely that the last few items
 * in the right half are already in the correct place (though not guaranteed).
 */
int _merge_left(int *arr, size_t lsize, size_t rsize) {
  // Copy the left half to the temporary buffer; this is necessary because we
  // will be overwriting the original data array with the result; it is also
  // sufficient to copy only the left half because the unprocessed data in the
  // right half will never be overwritten (left-to-right merge)
  int *temp = malloc(lsize * sizeof(int));
  if (temp == NULL) {
    return -1;
  }
  memcpy(temp, arr, lsize * sizeof(int));

  int *pleft = arr;
  int *pmid = arr + lsize;
  int *pright = arr + lsize + rsize;
  int *ptempterm = temp + lsize;

  int *pi = temp;  // Left half (start)
  int *pj = pmid;  // Right half (start)
  int *pk = pleft; // Merged array (start)

  // Merge the two halves; put the smaller element in the merged array from left
  // to right
  while (pi < ptempterm && pj < pright) {
    if (*pi <= *pj) {
      *pk++ = *pi++;
    } else {
      *pk++ = *pj++;
    }
  }

  // If there are any remaining elements in the left half, copy them over; note
  // that if there are anything left in the right half, they are already in the
  // correct place so we do not need to do anything
  while (pi < ptempterm) {
    *pk++ = *pi++;
  }

  free(temp);
  return 0;
}

/**
 * Subroutine for merge when the right half is smaller.
 *
 * This subroutine copies the right half to a temporary buffer and merges from
 * right to left in the original data array. Given the right half is smaller,
 * this is more memory-efficient, and it is more likely that the first few items
 * in the left half are already in the correct place (though not guaranteed).
 */
int _merge_right(int *arr, size_t lsize, size_t rsize) {
  // Copy the right half to the temporary buffer; this is necessary because we
  // will be overwriting the original data array with the result; it is also
  // sufficient to copy only the right half because the unprocessed data in
  // the left half will never be overwritten (right-to-left merge)
  int *temp = malloc(rsize * sizeof(int));
  if (temp == NULL) {
    return -1;
  }
  memcpy(temp, arr + lsize, rsize * sizeof(int));

  int *pleft = arr - 1;
  int *pmid = arr + lsize - 1;
  int *pright = arr + lsize + rsize - 1;
  int *ptempterm = temp - 1;

  int *pi = temp + rsize - 1; // Left half (start)
  int *pj = pmid;             // Right half (start)
  int *pk = pright;           // Merged array (start)

  // Merge the two halves; put the larger element in the merged array from right
  // to left
  while (pi > ptempterm && pj > pleft) {
    if (*pi > *pj) {
      *pk-- = *pi--;
    } else {
      *pk-- = *pj--;
    }
  }

  // If there are any remaining elements in the left half, copy them over; note
  // that if there are anything left in the right half, they are already in the
  // correct place so we do not need to do anything
  while (pi > ptempterm) {
    *pk-- = *pi--;
  }

  free(temp);
  return 0;
}

/**
 * @implements merge
 */
int merge(int *arr, size_t lsize, size_t rsize) {
  if (lsize == 0 || rsize == 0) {
    return 0; // Nothing to merge
  }

  // Decide a potentially better subroutine based on the size of the two halves
  if (lsize < rsize) {
    return _merge_left(arr, lsize, rsize);
  }
  return _merge_right(arr, lsize, rsize);
}

/**
 * Subroutine for amerge when the left half is smaller.
 *
 * This subroutine copies the left half to a temporary buffer and merges from
 * left to right in the original indices array. Given the left half is smaller,
 * this is more memory-efficient, and it is more likely that the last few items
 * in the right half are already in the correct place (though not guaranteed).
 */
int _amerge_left(int *arr, size_t *tosort, size_t lsize, size_t rsize) {
  // Copy the left half to the temporary buffer; this is necessary because we
  // will be overwriting the original indices array with the result; it is also
  // sufficient to copy only the left half because the unprocessed indices in
  // the right half will never be overwritten (left-to-right merge)
  size_t *temp = malloc(lsize * sizeof(size_t));
  if (temp == NULL) {
    return -1;
  }
  memcpy(temp, tosort, lsize * sizeof(size_t));

  size_t *pleft = tosort;
  size_t *pmid = tosort + lsize;
  size_t *pright = tosort + lsize + rsize;
  size_t *ptempterm = temp + lsize;

  size_t *pi = temp;  // Left half (start)
  size_t *pj = pmid;  // Right half (start)
  size_t *pk = pleft; // Merged array (start)

  // Merge the two halves; put the smaller element in the merged array from left
  // to right
  while (pi < ptempterm && pj < pright) {
    if (arr[*pi] <= arr[*pj]) {
      *pk++ = *pi++;
    } else {
      *pk++ = *pj++;
    }
  }

  // If there are any remaining elements in the left half, copy them over; note
  // that if there are anything left in the right half, they are already in the
  // correct place so we do not need to do anything
  while (pi < ptempterm) {
    *pk++ = *pi++;
  }

  free(temp);
  return 0;
}

/**
 * Subroutine for amerge when the right half is smaller.
 *
 * This subroutine copies the right half to a temporary buffer and merges from
 * right to left in the original indices array. Given the right half is smaller,
 * this is more memory-efficient, and it is more likely that the first few items
 * in the left half are already in the correct place (though not guaranteed).
 */
int _amerge_right(int *arr, size_t *tosort, size_t lsize, size_t rsize) {
  // Copy the right half to the temporary buffer; this is necessary because we
  // will be overwriting the original indices array with the result; it is also
  // sufficient to copy only the right half because the unprocessed indices in
  // the left half will never be overwritten (right-to-left merge)
  size_t *temp = malloc(rsize * sizeof(size_t));
  if (temp == NULL) {
    return -1;
  }
  memcpy(temp, tosort + lsize, rsize * sizeof(size_t));

  size_t *pleft = tosort - 1;
  size_t *pmid = tosort + lsize - 1;
  size_t *pright = tosort + lsize + rsize - 1;
  size_t *ptempterm = temp - 1;

  size_t *pi = temp + rsize - 1; // Right half (end)
  size_t *pj = pmid;             // Left half (end)
  size_t *pk = pright;           // Merged array (end)

  // Merge the two halves; put the larger element in the merged array from right
  // to left
  while (pi > ptempterm && pj > pleft) {
    if (arr[*pi] > arr[*pj]) {
      *pk-- = *pi--;
    } else {
      *pk-- = *pj--;
    }
  }

  // If there are any remaining elements in the right half, copy them over; note
  // that if there are anything left in the left half, they are already in the
  // correct place so we do not need to do anything
  while (pi > ptempterm) {
    *pk-- = *pi--;
  }

  free(temp);
  return 0;
}

/**
 * @implements amerge
 */
int amerge(int *arr, size_t *tosort, size_t lsize, size_t rsize) {
  if (lsize == 0 || rsize == 0) {
    return 0; // Nothing to merge
  }

  // Decide a potentially better subroutine based on the size of the two halves
  if (lsize < rsize) {
    return _amerge_left(arr, tosort, lsize, rsize);
  }
  return _amerge_right(arr, tosort, lsize, rsize);
}

/**
 * @implements kmerge
 */
int kmerge(int *arr, size_t k, size_t *sizes, size_t total_size) {
  if (k < 2) {
    return 0; // Nothing to merge
  }

  size_t mid = k / 2;

  // Left half
  size_t *lsizes = sizes;
  size_t lk = mid;
  size_t ltotal = 0;
  for (size_t i = 0; i < mid; i++) {
    ltotal += sizes[i];
  }

  // Right half
  size_t *rsizes = sizes + mid;
  size_t rk = k - mid;
  size_t rtotal = total_size - ltotal;

  // Recursively merge the two halves
  int status;
  status = kmerge(arr, lk, lsizes, ltotal);
  if (status != 0) {
    return status;
  }
  status = kmerge(arr + ltotal, rk, rsizes, rtotal);
  if (status != 0) {
    return status;
  }

  // Merge the two halves
  return merge(arr, ltotal, rtotal);
}

/**
 * @implements akmerge
 */
int akmerge(int *arr, size_t *tosort, size_t k, size_t *sizes,
            size_t total_size) {
  if (k < 2) {
    return 0; // Nothing to merge
  }

  size_t mid = k / 2;

  // Left half
  size_t *lsizes = sizes;
  size_t lk = mid;
  size_t ltotal = 0;
  for (size_t i = 0; i < mid; i++) {
    ltotal += sizes[i];
  }

  // Right half
  size_t *rsizes = sizes + mid;
  size_t rk = k - mid;
  size_t rtotal = total_size - ltotal;

  // Recursively arg merge the two halves
  int status;
  status = akmerge(arr, tosort, lk, lsizes, ltotal);
  if (status != 0) {
    return status;
  }
  status = akmerge(arr, tosort + ltotal, rk, rsizes, rtotal);
  if (status != 0) {
    return status;
  }

  // Arg merge the two halves
  return amerge(arr, tosort, ltotal, rtotal);
}
