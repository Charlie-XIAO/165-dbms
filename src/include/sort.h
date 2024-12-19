/**
 * @file sort.h
 *
 * This header defines sorting functions.
 */

#ifndef SORT_H__
#define SORT_H__

#include <stddef.h>

/**
 * Sort via quicksort.
 *
 * This function takes an array of integers and the size. The array will be
 * sorted in ascending order in-place. The function returns 0 on success and -1
 * on failure.
 *
 * Note that this sorting is not stable. Moreover, it is implemented as a hybrid
 * sorting algorithm. It uses quicksort for large problems and switches to
 * insertion sort when the problem size drops below a certain cutoff. TODO: It
 * will also switch to heapsort if the quicksort recursion goes too deep.
 */
int quicksort(int *arr, size_t size);

/**
 * Argsort via quicksort.
 *
 * This function takes an array of integers, an array of indices, and the size.
 * The array will not be modified; only the indices will be sorted in ascending
 * order in-place. When there is no offset, `arr` should both be of size `size`
 * and `tosort` should be initialized as 0 to `size-1`. When there is an offset,
 * `arr` should be of size `size+offset` and `tosort` should be initialized as
 * `offset` to `size+offset-1`. See unit tests for examples. The function
 * returns 0 on success and -1 on failure.
 *
 * Note that this sorting is not stable. Moreover, it is implemented as a hybrid
 * sorting algorithm. It uses quicksort for large problems and switches to
 * insertion sort when the problem size drops below a certain cutoff. TODO: It
 * will also switch to heapsort if the quicksort recursion goes too deep.
 */
int aquicksort(int *arr, size_t *tosort, size_t size);

/**
 * Merge two sorted halves of an array.
 *
 * This function takes an array of integers and the sizes of the left and right
 * halves. The two halves must be sorted in ascending order respectively. The
 * array will be modified in-place to store the merged result. `arr` should be
 * of size `lsize+rsize`. See unit tests for examples. The function returns 0 on
 * success and -1 on failure.
 */
int merge(int *arr, size_t lsize, size_t rsize);

/**
 * Arg merge two sorted halves of an array.
 *
 * This function takes an array of integers, an array of indices, and the sizes
 * of the left and right halves. The two halves must be sorted in ascending
 * order respectively. The array will not be modified; only the indices will be
 * modified in-place to store the merged result. `arr` should be of size
 * `lsize+rsize` and `tosort` should contain indices 0 to `lsize-1` for the left
 * half and `lsize` to `lsize+rsize-1` for the right half. See unit tests for
 * examples. The function returns 0 on success and -1 on failure.
 */
int amerge(int *arr, size_t *tosort, size_t lsize, size_t rsize);

/**
 * K-way merge of sorted parts of an array.
 *
 * This function takes an array of integers, the number of parts to merge, the
 * sizes of the parts, and the total size. The parts must be sorted in ascending
 * order respectively. The array will be modified in-place to store the merged
 * result. `arr` should be of size `total_size`. See unit tests for examples.
 * The function returns 0 on success and -1 on failure.
 *
 * Note that this function is implemented by divide and conquer instead of the
 * more common heap-based approach. Both have time complexity O(nlogk), subject
 * to constant factors.
 */
int kmerge(int *arr, size_t k, size_t *sizes, size_t total_size);

/**
 * Arg k-way merge of sorted parts of an array.
 *
 * This function takes an array of integers, an array of indices, the number of
 * parts to merge, the sizes of the parts, and the total size. The parts must be
 * sorted in ascending order respectively. The array will not be modified; only
 * the indices will be modified in-place to store the merged result. `arr`
 * should be of size `total_size` and `tosort` should contain indices 0 to
 * `sizes[0]-1` for the first part, `sizes[0]` to `sizes[0]+sizes[1]-1` for the
 * second part, etc. See unit tests for examples. The function returns 0 on
 * success and -1 on failure.
 *
 * Note that this function is implemented by divide and conquer instead of the
 * more common heap-based approach. Both have time complexity O(nlogk), subject
 * to constant factors.
 */
int akmerge(int *arr, size_t *tosort, size_t k, size_t *sizes,
            size_t total_size);

#endif // SORT_H__
