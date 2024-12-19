/**
 * @file binsearch.h
 *
 * This header defines binary search functions.
 */

#ifndef BINSEARCH_H__
#define BINSEARCH_H__

#include <stdbool.h>
#include <stddef.h>

/**
 * Binary search.
 *
 * Perform a binary search on the sorted array of values `arr` of size `size`.
 * The key is the value to search for. The `align_left` flag determines if the
 * returned index is aligned left or right. If it is true, the returned index is
 * the first to satisfy `arr[i-1] < key <= arr[i]`. If it is false, the returned
 * index is the last to satisfy `arr[i-1] <= key < arr[i]`. Too small keys will
 * return 0, and too large keys will return `size`.
 */
size_t binsearch(int *arr, long key, size_t size, bool align_left);

/**
 * Arg binary search.
 *
 * Perform a binary search, but the array of values is not necessarily sorted
 * and `sort` is the array of indices that sort the array of values into
 * ascending order, typically the result of argsort. The key is the value to
 * search for, and the size is the number of elements in the array of values
 * (and the array of sorted indices). The `align_left` flag determines if the
 * returned index is aligned left or right. If it is true, the returned index
 * is the first to satisfy `arr[sort[i-1]] < key <= arr[sort[i]]`. If it is
 * false, the returned index is the last to satisfy `arr[sort[i-1]] <= key <
 * arr[sort[i]]`. Too small keys will return 0, and too large keys will return
 * `size`.
 */
size_t abinsearch(int *arr, long key, size_t *sort, size_t size,
                  bool align_left);

#endif // BINSEARCH_H__
