#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "sort.h"
#include "testing.h"

/**
 * Test the quicksort function.
 */
void test_quicksort() {
  srand(0);
  const size_t size = 10000;

  // Generate random values array
  int values[size];
  for (size_t i = 0; i < size; i++) {
    values[i] = rand();
  }

  // Sort the values array and check that they are sorted
  quicksort(values, size);
  for (size_t i = 0; i < size - 1; i++) {
    assert(values[i] <= values[i + 1]);
  }
}

/**
 * Test the aquicksort function.
 */
void test_aquicksort() {
  srand(0);
  const size_t size = 10000;

  // Generate random values array
  int values[size];
  for (size_t i = 0; i < size; i++) {
    values[i] = rand();
  }

  // Generate index array 0 ~ size-1
  size_t index_array[size];
  for (size_t i = 0; i < size; i++) {
    index_array[i] = i;
  }

  // Argsort and check that they are sorted
  aquicksort(values, index_array, size);
  for (size_t i = 0; i < size - 1; i++) {
    assert(values[index_array[i]] <= values[index_array[i + 1]]);
  }

  // Reset the index array
  for (size_t i = 0; i < size; i++) {
    index_array[i] = i;
  }

  // Argsort with an offset; check that the part before the offset is untouched
  // and the part after the offset is sorted
  size_t offset = size / 5;
  aquicksort(values, index_array + offset, size - offset);
  for (size_t i = 0; i < offset; i++) {
    assert(index_array[i] == i);
  }
  for (size_t i = offset; i < size - 1; i++) {
    assert(values[index_array[i]] <= values[index_array[i + 1]]);
  }
}

/**
 * Test the merge function.
 */
void test_merge() {
  srand(0);
  const size_t size = 10000;

  // Generate random values array
  int values[size];
  for (size_t i = 0; i < size; i++) {
    values[i] = rand();
  }

  // Copy the values array and sort as the ground truth
  int values_true[size];
  memcpy(values_true, values, size * sizeof(int));
  quicksort(values_true, size);

  size_t lsize, rsize;
  int actual_values[size];

  // First half is smaller
  memcpy(actual_values, values, size * sizeof(int));
  lsize = size / 5;
  rsize = size - lsize;
  quicksort(values, lsize);
  quicksort(values + lsize, rsize);
  merge(values, lsize, rsize);
  for (size_t i = 0; i < size; i++) {
    assert(values[i] == values_true[i]);
  }

  // Second half is smaller
  memcpy(actual_values, values, size * sizeof(int));
  rsize = size / 5;
  lsize = size - rsize;
  quicksort(values, lsize);
  quicksort(values + lsize, rsize);
  merge(values, lsize, rsize);
  for (size_t i = 0; i < size; i++) {
    assert(values[i] == values_true[i]);
  }

  // Left half is empty
  memcpy(actual_values, values, size * sizeof(int));
  lsize = 0;
  rsize = size;
  quicksort(values, lsize);
  quicksort(values + lsize, rsize);
  merge(values, lsize, rsize);
  for (size_t i = 0; i < size; i++) {
    assert(values[i] == values_true[i]);
  }

  // Right half is empty
  memcpy(actual_values, values, size * sizeof(int));
  lsize = size;
  rsize = 0;
  quicksort(values, lsize);
  quicksort(values + lsize, rsize);
  merge(values, lsize, rsize);
  for (size_t i = 0; i < size; i++) {
    assert(values[i] == values_true[i]);
  }
}

/**
 * Test the amerge function.
 */
void test_amerge() {
  srand(0);
  const size_t size = 10000;

  // Generate random values array
  int values[size];
  for (size_t i = 0; i < size; i++) {
    values[i] = rand();
  }

  // Generate index array 0 ~ size-1
  size_t index_array[size];
  for (size_t i = 0; i < size; i++) {
    index_array[i] = i;
  }

  // Copy the index array and argsort as the ground truth
  size_t index_array_true[size];
  memcpy(index_array_true, index_array, size * sizeof(size_t));
  aquicksort(values, index_array_true, size);

  size_t lsize, rsize;

  // First half is smaller
  lsize = size / 5;
  rsize = size - lsize;
  aquicksort(values, index_array, lsize);
  aquicksort(values, index_array + lsize, rsize);
  amerge(values, index_array, lsize, rsize);
  for (size_t i = 0; i < size; i++) {
    assert(index_array[i] == index_array_true[i]);
  }

  // Reset the index array
  for (size_t i = 0; i < size; i++) {
    index_array[i] = i;
  }

  // Second half is smaller
  rsize = size / 5;
  lsize = size - rsize;
  aquicksort(values, index_array, lsize);
  aquicksort(values, index_array + lsize, rsize);
  amerge(values, index_array, lsize, rsize);
  for (size_t i = 0; i < size; i++) {
    assert(index_array[i] == index_array_true[i]);
  }

  // Reset the index array
  for (size_t i = 0; i < size; i++) {
    index_array[i] = i;
  }

  // Left half is empty
  lsize = 0;
  rsize = size;
  aquicksort(values, index_array, lsize);
  aquicksort(values, index_array + lsize, rsize);
  amerge(values, index_array, lsize, rsize);
  for (size_t i = 0; i < size; i++) {
    assert(index_array[i] == index_array_true[i]);
  }

  // Reset the index array
  for (size_t i = 0; i < size; i++) {
    index_array[i] = i;
  }

  // Right half is empty
  lsize = size;
  rsize = 0;
  aquicksort(values, index_array, lsize);
  aquicksort(values, index_array + lsize, rsize);
  amerge(values, index_array, lsize, rsize);
  for (size_t i = 0; i < size; i++) {
    assert(index_array[i] == index_array_true[i]);
  }
}

/**
 * Test the kmerge function.
 */
void test_kmerge() {
  srand(0);
  const size_t size = 10000;

  // Generate random values array
  int values[size];
  for (size_t i = 0; i < size; i++) {
    values[i] = rand();
  }

  // Copy the values array and sort as the ground truth
  int values_true[size];
  memcpy(values_true, values, size * sizeof(int));
  quicksort(values_true, size);

  size_t k;
  size_t sizes[5];
  int actual_values[size];

  // Test with [1/5, 3/5, 1/5] merge (k=3)
  memcpy(actual_values, values, size * sizeof(int));
  k = 3;
  sizes[0] = size / 5;
  sizes[1] = 3 * size / 5;
  sizes[2] = size - sizes[0] - sizes[1];
  quicksort(values, sizes[0]);
  quicksort(values + sizes[0], sizes[1]);
  quicksort(values + sizes[0] + sizes[1], sizes[2]);
  kmerge(values, k, sizes, size);
  for (size_t i = 0; i < size; i++) {
    assert(values[i] == values_true[i]);
  }

  // Test with [2/5, 1/5, 1/5, 1/5] merge (k=4)
  memcpy(actual_values, values, size * sizeof(int));
  k = 4;
  sizes[0] = 2 * size / 5;
  sizes[1] = size / 5;
  sizes[2] = size / 5;
  sizes[3] = size - sizes[0] - sizes[1] - sizes[2];
  quicksort(values, sizes[0]);
  quicksort(values + sizes[0], sizes[1]);
  quicksort(values + sizes[0] + sizes[1], sizes[2]);
  quicksort(values + sizes[0] + sizes[1] + sizes[2], sizes[3]);
  kmerge(values, k, sizes, size);
  for (size_t i = 0; i < size; i++) {
    assert(values[i] == values_true[i]);
  }

  // Test with [1/5, 1/5, 1/5, 1/5, 1/5] merge (k=5)
  memcpy(actual_values, values, size * sizeof(int));
  k = 5;
  sizes[0] = size / 5;
  sizes[1] = size / 5;
  sizes[2] = size / 5;
  sizes[3] = size / 5;
  sizes[4] = size - sizes[0] - sizes[1] - sizes[2] - sizes[3];
  quicksort(values, sizes[0]);
  quicksort(values + sizes[0], sizes[1]);
  quicksort(values + sizes[0] + sizes[1], sizes[2]);
  quicksort(values + sizes[0] + sizes[1] + sizes[2], sizes[3]);
  quicksort(values + sizes[0] + sizes[1] + sizes[2] + sizes[3], sizes[4]);
  kmerge(values, k, sizes, size);
  for (size_t i = 0; i < size; i++) {
    assert(values[i] == values_true[i]);
  }
}

/**
 * Test the akmerge function.
 */
void test_akmerge() {
  srand(0);
  const size_t size = 10000;

  // Generate random values array
  int values[size];
  for (size_t i = 0; i < size; i++) {
    values[i] = rand();
  }

  // Generate index array 0 ~ size-1
  size_t index_array[size];
  for (size_t i = 0; i < size; i++) {
    index_array[i] = i;
  }

  // Copy the index array and argsort as the ground truth
  size_t index_array_true[size];
  memcpy(index_array_true, index_array, size * sizeof(size_t));
  aquicksort(values, index_array_true, size);

  size_t k;
  size_t sizes[5];

  // Test with [1/5, 3/5, 1/5] merge (k=3)
  k = 3;
  sizes[0] = size / 5;
  sizes[1] = 3 * size / 5;
  sizes[2] = size - sizes[0] - sizes[1];
  aquicksort(values, index_array, sizes[0]);
  aquicksort(values, index_array + sizes[0], sizes[1]);
  aquicksort(values, index_array + sizes[0] + sizes[1], sizes[2]);
  akmerge(values, index_array, k, sizes, size);
  for (size_t i = 0; i < size; i++) {
    assert(index_array[i] == index_array_true[i]);
  }

  // Reset the index array
  for (size_t i = 0; i < size; i++) {
    index_array[i] = i;
  }

  // Test with [2/5, 1/5, 1/5, 1/5] merge (k=4)
  k = 4;
  sizes[0] = 2 * size / 5;
  sizes[1] = size / 5;
  sizes[2] = size / 5;
  sizes[3] = size - sizes[0] - sizes[1] - sizes[2];
  aquicksort(values, index_array, sizes[0]);
  aquicksort(values, index_array + sizes[0], sizes[1]);
  aquicksort(values, index_array + sizes[0] + sizes[1], sizes[2]);
  aquicksort(values, index_array + sizes[0] + sizes[1] + sizes[2], sizes[3]);
  akmerge(values, index_array, k, sizes, size);
  for (size_t i = 0; i < size; i++) {
    assert(index_array[i] == index_array_true[i]);
  }

  // Reset the index array
  for (size_t i = 0; i < size; i++) {
    index_array[i] = i;
  }

  // Test with [1/5, 1/5, 1/5, 1/5, 1/5] merge (k=5)
  k = 5;
  sizes[0] = size / 5;
  sizes[1] = size / 5;
  sizes[2] = size / 5;
  sizes[3] = size / 5;
  sizes[4] = size - sizes[0] - sizes[1] - sizes[2] - sizes[3];
  aquicksort(values, index_array, sizes[0]);
  aquicksort(values, index_array + sizes[0], sizes[1]);
  aquicksort(values, index_array + sizes[0] + sizes[1], sizes[2]);
  aquicksort(values, index_array + sizes[0] + sizes[1] + sizes[2], sizes[3]);
  aquicksort(values, index_array + sizes[0] + sizes[1] + sizes[2] + sizes[3],
             sizes[4]);
  akmerge(values, index_array, k, sizes, size);
  for (size_t i = 0; i < size; i++) {
    assert(index_array[i] == index_array_true[i]);
  }
}

int main() {
  TEST(quicksort);
  TEST(aquicksort);
  TEST(merge);
  TEST(amerge);
  TEST(kmerge);
  TEST(akmerge);
  return 0;
}
