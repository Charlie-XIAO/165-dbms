#include <assert.h>
#include <limits.h>
#include <stddef.h>

#include "binsearch.h"
#include "testing.h"

/**
 * Test the binsearch function when aligned left.
 */
void test_binsearch_left() {
  size_t size = 12;
  int arr[] = {0, 2, 4, 6, 8, 10, 12, 14, 14, 14, 16, 18};

  // Too small keys should give 0
  assert(binsearch(arr, LONG_MIN, size, true) == 0);
  assert(binsearch(arr, -2, size, true) == 0);
  assert(binsearch(arr, -1, size, true) == 0);

  // Keys in the middle should give the left boundary
  assert(binsearch(arr, 0, size, true) == 0);
  assert(binsearch(arr, 1, size, true) == 1);
  assert(binsearch(arr, 2, size, true) == 1);
  assert(binsearch(arr, 3, size, true) == 2);
  assert(binsearch(arr, 4, size, true) == 2);
  assert(binsearch(arr, 5, size, true) == 3);
  assert(binsearch(arr, 6, size, true) == 3);
  assert(binsearch(arr, 7, size, true) == 4);
  assert(binsearch(arr, 8, size, true) == 4);
  assert(binsearch(arr, 9, size, true) == 5);
  assert(binsearch(arr, 10, size, true) == 5);
  assert(binsearch(arr, 11, size, true) == 6);
  assert(binsearch(arr, 12, size, true) == 6);
  assert(binsearch(arr, 13, size, true) == 7);
  assert(binsearch(arr, 14, size, true) == 7);
  assert(binsearch(arr, 15, size, true) == 10);
  assert(binsearch(arr, 16, size, true) == 10);
  assert(binsearch(arr, 17, size, true) == 11);
  assert(binsearch(arr, 18, size, true) == 11);

  // Too large keys should give size
  assert(binsearch(arr, 19, size, true) == size);
  assert(binsearch(arr, 20, size, true) == size);
  assert(binsearch(arr, LONG_MAX, size, true) == size);
}

/**
 * Test the binsearch function when aligned right.
 */
void test_binsearch_right() {
  size_t size = 12;
  int arr[] = {0, 2, 4, 6, 8, 10, 12, 14, 14, 14, 16, 18};

  // Too small keys should give 0
  assert(binsearch(arr, LONG_MIN, size, false) == 0);
  assert(binsearch(arr, -2, size, false) == 0);
  assert(binsearch(arr, -1, size, false) == 0);

  // Keys in the middle should give the right boundary
  assert(binsearch(arr, 0, size, false) == 1);
  assert(binsearch(arr, 1, size, false) == 1);
  assert(binsearch(arr, 2, size, false) == 2);
  assert(binsearch(arr, 3, size, false) == 2);
  assert(binsearch(arr, 4, size, false) == 3);
  assert(binsearch(arr, 5, size, false) == 3);
  assert(binsearch(arr, 6, size, false) == 4);
  assert(binsearch(arr, 7, size, false) == 4);
  assert(binsearch(arr, 8, size, false) == 5);
  assert(binsearch(arr, 9, size, false) == 5);
  assert(binsearch(arr, 10, size, false) == 6);
  assert(binsearch(arr, 11, size, false) == 6);
  assert(binsearch(arr, 12, size, false) == 7);
  assert(binsearch(arr, 13, size, false) == 7);
  assert(binsearch(arr, 14, size, false) == 10);
  assert(binsearch(arr, 15, size, false) == 10);
  assert(binsearch(arr, 16, size, false) == 11);
  assert(binsearch(arr, 17, size, false) == 11);
  assert(binsearch(arr, 18, size, false) == 12);

  // Too large keys should give size
  assert(binsearch(arr, 19, size, false) == size);
  assert(binsearch(arr, 20, size, false) == size);
  assert(binsearch(arr, LONG_MAX, size, false) == size);
}

/**
 * Test the abinsearch function when aligned left.
 */
void test_abinsearch_left() {
  size_t size = 12;
  int arr[] = {14, 6, 14, 10, 2, 14, 0, 8, 16, 4, 12, 18};
  size_t sort[] = {6, 4, 9, 1, 7, 3, 10, 0, 5, 2, 8, 11};

  // Too small keys should give 0
  assert(abinsearch(arr, LONG_MIN, sort, size, true) == 0);
  assert(abinsearch(arr, -2, sort, size, true) == 0);
  assert(abinsearch(arr, -1, sort, size, true) == 0);

  // Keys in the middle should give the left boundary
  assert(abinsearch(arr, 0, sort, size, true) == 0);
  assert(abinsearch(arr, 1, sort, size, true) == 1);
  assert(abinsearch(arr, 2, sort, size, true) == 1);
  assert(abinsearch(arr, 3, sort, size, true) == 2);
  assert(abinsearch(arr, 4, sort, size, true) == 2);
  assert(abinsearch(arr, 5, sort, size, true) == 3);
  assert(abinsearch(arr, 6, sort, size, true) == 3);
  assert(abinsearch(arr, 7, sort, size, true) == 4);
  assert(abinsearch(arr, 8, sort, size, true) == 4);
  assert(abinsearch(arr, 9, sort, size, true) == 5);
  assert(abinsearch(arr, 10, sort, size, true) == 5);
  assert(abinsearch(arr, 11, sort, size, true) == 6);
  assert(abinsearch(arr, 12, sort, size, true) == 6);
  assert(abinsearch(arr, 13, sort, size, true) == 7);
  assert(abinsearch(arr, 14, sort, size, true) == 7);
  assert(abinsearch(arr, 15, sort, size, true) == 10);
  assert(abinsearch(arr, 16, sort, size, true) == 10);
  assert(abinsearch(arr, 17, sort, size, true) == 11);
  assert(abinsearch(arr, 18, sort, size, true) == 11);

  // Too large keys should give size
  assert(abinsearch(arr, 19, sort, size, true) == size);
  assert(abinsearch(arr, 20, sort, size, true) == size);
  assert(abinsearch(arr, LONG_MAX, sort, size, true) == size);
}

/**
 * Test the abinsearch function when aligned right.
 */
void test_abinsearch_right() {
  size_t size = 12;
  int arr[] = {14, 6, 14, 10, 2, 14, 0, 8, 16, 4, 12, 18};
  size_t sort[] = {6, 4, 9, 1, 7, 3, 10, 0, 5, 2, 8, 11};

  // Too small keys should give 0
  assert(abinsearch(arr, LONG_MIN, sort, size, false) == 0);
  assert(abinsearch(arr, -2, sort, size, false) == 0);
  assert(abinsearch(arr, -1, sort, size, false) == 0);

  // Keys in the middle should give the right boundary
  assert(abinsearch(arr, 0, sort, size, false) == 1);
  assert(abinsearch(arr, 1, sort, size, false) == 1);
  assert(abinsearch(arr, 2, sort, size, false) == 2);
  assert(abinsearch(arr, 3, sort, size, false) == 2);
  assert(abinsearch(arr, 4, sort, size, false) == 3);
  assert(abinsearch(arr, 5, sort, size, false) == 3);
  assert(abinsearch(arr, 6, sort, size, false) == 4);
  assert(abinsearch(arr, 7, sort, size, false) == 4);
  assert(abinsearch(arr, 8, sort, size, false) == 5);
  assert(abinsearch(arr, 9, sort, size, false) == 5);
  assert(abinsearch(arr, 10, sort, size, false) == 6);
  assert(abinsearch(arr, 11, sort, size, false) == 6);
  assert(abinsearch(arr, 12, sort, size, false) == 7);
  assert(abinsearch(arr, 13, sort, size, false) == 7);
  assert(abinsearch(arr, 14, sort, size, false) == 10);
  assert(abinsearch(arr, 15, sort, size, false) == 10);
  assert(abinsearch(arr, 16, sort, size, false) == 11);
  assert(abinsearch(arr, 17, sort, size, false) == 11);
  assert(abinsearch(arr, 18, sort, size, false) == 12);

  // Too large keys should give size
  assert(abinsearch(arr, 19, sort, size, false) == size);
  assert(abinsearch(arr, 20, sort, size, false) == size);
  assert(abinsearch(arr, LONG_MAX, sort, size, false) == size);
}

int main() {
  TEST(binsearch_left);
  TEST(binsearch_right);
  TEST(abinsearch_left);
  TEST(abinsearch_right);
  return 0;
}
