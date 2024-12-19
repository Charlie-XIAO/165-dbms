#include <assert.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>

#include "binsearch.h"
#include "bptree.h"
#include "sort.h"
#include "testing.h"

/**
 * Helper function to find the first leaf node in the B+ tree.
 */
BPlusNode *_find_first_leaf(BPlusTree *tree) {
  BPlusNode *node = tree->root;
  while (node->type != BPLUS_NODE_TYPE_LEAF) {
    node = node->spec.internal.children[0];
  }
  return node;
}

/**
 * Test the bplus_tree_create function.
 */
void test_bplus_tree_create() {
  srand(0);

  size_t size = BPLUS_TREE_ORDER * BPLUS_TREE_ORDER; // Ensure >= 2 levels
  if (size < 10000) {
    size = 10000;
  }

  int *data = malloc(sizeof(int) * size);
  for (size_t i = 0; i < size; i++) {
    data[i] = rand();
  }

  size_t *sorter = malloc(sizeof(size_t) * size);
  for (size_t i = 0; i < size; i++) {
    sorter[i] = i;
  }
  aquicksort(data, sorter, size);

  // Bulk load the B+ tree
  BPlusTree *tree = bplus_tree_create(data, sorter, size);
  assert(tree->size == size);

  // Check data in the leaf nodes while also checking the forward connections in
  // the leaf level
  BPlusNode *node = _find_first_leaf(tree);
  for (size_t i = 0; i < size; i += BPLUS_TREE_ORDER - 1) {
    for (int j = 0; i + j < size && j < BPLUS_TREE_ORDER - 1; j++) {
      assert(node->keys[j] == data[sorter[i + j]]);
      assert(node->spec.leaf.values[j] == sorter[i + j]);
    }
    node = node->spec.leaf.next;
  }
  assert(node == NULL);

  bplus_tree_free(tree);
  free(data);
  free(sorter);
}

/**
 * Test the bplus_tree_insert function.
 */
void test_bplus_tree_insert() {
  srand(0);

  size_t size = BPLUS_TREE_ORDER * BPLUS_TREE_ORDER; // Ensure >= 2 levels
  if (size < 10000) {
    size = 10000;
  }

  int *data = malloc(sizeof(int) * size);
  for (size_t i = 0; i < size; i++) {
    data[i] = rand();
  }

  size_t *sorter = malloc(sizeof(size_t) * size);
  for (size_t i = 0; i < size; i++) {
    sorter[i] = i;
  }
  aquicksort(data, sorter, size);

  // Insert into the B+ tree
  BPlusTree *tree = bplus_tree_create(NULL, NULL, 0);
  for (size_t i = 0; i < size; i++) {
    bplus_tree_insert(tree, data[sorter[i]], sorter[i]);
  }
  assert(tree->size == size);

  // Check data in the leaf nodes while also checking the forward connections in
  // the leaf level
  BPlusNode *node = _find_first_leaf(tree);
  for (size_t i = 0; i < size;) {
    for (int j = 0; i + j < size && j < node->n_keys; j++) {
      assert(node->keys[j] == data[sorter[i + j]]);
      assert(node->spec.leaf.values[j] == sorter[i + j]);
    }
    i += node->n_keys;
    node = node->spec.leaf.next;
  }
  assert(node == NULL);

  bplus_tree_free(tree);
  free(data);
  free(sorter);
}

/**
 * Test the bplus_tree_search_cont function.
 */
void test_bplus_tree_search_cont() {
  srand(0);

  size_t size = BPLUS_TREE_ORDER * BPLUS_TREE_ORDER; // Ensure >= 2 levels
  if (size < 10000) {
    size = 10000;
  }
  size = 20;

  int *data = malloc(sizeof(int) * size);
  for (size_t i = 0; i < size; i++) {
    data[i] = rand() % size; // Many duplicates
  }
  quicksort(data, size);
  BPlusTree *tree = bplus_tree_create(data, NULL, size);

  // Test 10 random keys, then two corner cases
  for (int x = 0; x < 12; x++) {
    int key;
    switch (x) {
    case 10: // Find INT_MIN
      key = INT_MIN;
      break;
    case 11: // Find INT_MAX
      key = INT_MAX;
      break;
    default:
      key = rand() % size;
      break;
    }

    // Check that the search result is consistent with the binary search, both
    // aligned left and right
    assert(bplus_tree_search_cont(tree, key, true) ==
           binsearch(data, key, size, true));
    assert(bplus_tree_search_cont(tree, key, false) ==
           binsearch(data, key, size, false));
  }

  bplus_tree_free(tree);
  free(data);
}

/**
 * Test the bplus_tree_search_range_cont function with small toy data.
 *
 * This includes some corner cases like empty tree, single key, duplicate keys,
 * ranges on borders, etc.
 */
void test_bplus_tree_search_range_cont_toy() {
  BPlusTree *tree = bplus_tree_create(NULL, NULL, 0);
  size_t *values, count;

  // Tree data: []
  values = bplus_tree_search_range_cont(tree, -100, 100, &count);
  assert(count == 0);
  free(values);

  // Tree data: [1]
  bplus_tree_insert(tree, 1, 0);
  values = bplus_tree_search_range_cont(tree, 0, 2, &count);
  assert(count == 1);
  assert(values[0] == 0);
  free(values);
  values = bplus_tree_search_range_cont(tree, 1, 2, &count);
  assert(count == 1);
  assert(values[0] == 0);
  free(values);
  values = bplus_tree_search_range_cont(tree, 0, 1, &count);
  assert(count == 0);
  free(values);
  values = bplus_tree_search_range_cont(tree, 2, 3, &count);
  assert(count == 0);
  free(values);

  // Tree data: [1, 2, 3, 3, 3, 4, 5]
  bplus_tree_insert(tree, 2, 1);
  bplus_tree_insert(tree, 3, 2);
  bplus_tree_insert(tree, 3, 3);
  bplus_tree_insert(tree, 3, 4);
  bplus_tree_insert(tree, 4, 5);
  bplus_tree_insert(tree, 5, 6);
  values = bplus_tree_search_range_cont(tree, 0, 1, &count);
  assert(count == 0);
  free(values);
  values = bplus_tree_search_range_cont(tree, 1, 2, &count);
  assert(count == 1);
  assert(values[0] == 0);
  free(values);
  values = bplus_tree_search_range_cont(tree, 2, 3, &count);
  assert(count == 1);
  assert(values[0] == 1);
  free(values);
  values = bplus_tree_search_range_cont(tree, 3, 4, &count);
  assert(count == 3);
  assert(values[0] == 2);
  assert(values[1] == 3);
  assert(values[2] == 4);
  free(values);
  values = bplus_tree_search_range_cont(tree, 4, 5, &count);
  assert(count == 1);
  assert(values[0] == 5);
  free(values);
  values = bplus_tree_search_range_cont(tree, 5, 6, &count);
  assert(count == 1);
  assert(values[0] == 6);
  free(values);
  values = bplus_tree_search_range_cont(tree, 6, 7, &count);
  assert(count == 0);
  free(values);

  // Same data as before, but try some larger ranges
  values = bplus_tree_search_range_cont(tree, 0, 7, &count);
  assert(count == 7);
  assert(values[0] == 0);
  assert(values[1] == 1);
  assert(values[2] == 2);
  assert(values[3] == 3);
  assert(values[4] == 4);
  assert(values[5] == 5);
  assert(values[6] == 6);
  free(values);
  values = bplus_tree_search_range_cont(tree, -100, 100, &count);
  assert(count == 7);
  assert(values[0] == 0);
  assert(values[1] == 1);
  assert(values[2] == 2);
  assert(values[3] == 3);
  assert(values[4] == 4);
  assert(values[5] == 5);
  assert(values[6] == 6);
  free(values);
  values = bplus_tree_search_range_cont(tree, -100, 3, &count);
  assert(count == 2);
  assert(values[0] == 0);
  assert(values[1] == 1);
  free(values);
  values = bplus_tree_search_range_cont(tree, -100, 4, &count);
  assert(count == 5);
  assert(values[0] == 0);
  assert(values[1] == 1);
  assert(values[2] == 2);
  assert(values[3] == 3);
  assert(values[4] == 4);
  free(values);
  values = bplus_tree_search_range_cont(tree, 3, 100, &count);
  assert(count == 5);
  assert(values[0] == 2);
  assert(values[1] == 3);
  assert(values[2] == 4);
  assert(values[3] == 5);
  assert(values[4] == 6);
  free(values);

  bplus_tree_free(tree);
}

/**
 * Test the bplus_tree_search_range_cont function with large random data and
 * ranges.
 */
void test_bplus_tree_search_range_cont() {
  srand(0);

  size_t size = BPLUS_TREE_ORDER * BPLUS_TREE_ORDER; // Ensure >= 2 levels
  if (size < 10000) {
    size = 10000;
  }
  size = 1000;

  int *data = malloc(sizeof(int) * size);
  for (size_t i = 0; i < size; i++) {
    data[i] = rand();
  }
  quicksort(data, size);
  BPlusTree *tree = bplus_tree_create(data, NULL, size);

  // Test 10 random ranges, then three infinite ranges
  size_t *expected_values = malloc(sizeof(size_t) * size);
  for (int x = 0; x < 13; x++) {
    long lower, upper;
    switch (x) {
    case 10: // Lower bound is INT_MIN
      lower = INT_MIN;
      upper = rand();
      break;
    case 11: // Upper bound is INT_MAX
      lower = rand();
      upper = INT_MAX;
      break;
    case 12: // Both bounds are INT_MIN and INT_MAX
      lower = INT_MIN;
      upper = INT_MAX;
      break;
    default:
      lower = rand();
      upper = rand();
      if (lower > upper) {
        long tmp = lower;
        lower = upper;
        upper = tmp;
      }
      break;
    }

    // Brute force range search
    size_t expected_count = 0;
    for (size_t i = 0; i < size; i++) {
      if (data[i] >= lower && data[i] < upper) {
        expected_values[expected_count++] = i;
      }
    }

    // B+ tree range search and check that the results are consistent with the
    // brute force range search
    size_t result_count;
    size_t *result_values =
        bplus_tree_search_range_cont(tree, lower, upper, &result_count);
    assert(result_count == expected_count);
    for (size_t i = 0; i < result_count; i++) {
      assert(result_values[i] == expected_values[i]);
    }
    free(result_values);
  }

  bplus_tree_free(tree);
  free(data);
  free(expected_values);
}

/**
 * Test the bplus_tree_search_range function with small toy data.
 *
 * This includes some corner cases like empty tree, single key, duplicate keys,
 * ranges on borders, etc.
 */
void test_bplus_tree_search_range_toy() {
  BPlusTree *tree = bplus_tree_create(NULL, NULL, 0);
  size_t values[10];
  size_t count;

  // Tree data: []
  count = bplus_tree_search_range(tree, -100, 100, values);
  assert(count == 0);

  // Tree data: [1]
  bplus_tree_insert(tree, 1, 10);
  count = bplus_tree_search_range(tree, 0, 2, values);
  assert(count == 1);
  assert(values[0] == 10);
  count = bplus_tree_search_range(tree, 1, 2, values);
  assert(count == 1);
  assert(values[0] == 10);
  count = bplus_tree_search_range(tree, 0, 1, values);
  assert(count == 0);
  count = bplus_tree_search_range(tree, 2, 3, values);
  assert(count == 0);

  // Tree data: [1, 2, 3, 3, 3, 4, 5]
  bplus_tree_insert(tree, 2, 20);
  bplus_tree_insert(tree, 3, 30);
  bplus_tree_insert(tree, 3, 31);
  bplus_tree_insert(tree, 3, 32);
  bplus_tree_insert(tree, 4, 40);
  bplus_tree_insert(tree, 5, 50);
  count = bplus_tree_search_range(tree, 0, 1, values);
  assert(count == 0);
  count = bplus_tree_search_range(tree, 1, 2, values);
  assert(count == 1);
  assert(values[0] == 10);
  count = bplus_tree_search_range(tree, 2, 3, values);
  assert(count == 1);
  assert(values[0] == 20);
  count = bplus_tree_search_range(tree, 3, 4, values);
  assert(count == 3);
  assert(values[0] == 30);
  assert(values[1] == 31);
  assert(values[2] == 32);
  count = bplus_tree_search_range(tree, 4, 5, values);
  assert(count == 1);
  assert(values[0] == 40);
  count = bplus_tree_search_range(tree, 5, 6, values);
  assert(count == 1);
  assert(values[0] == 50);
  count = bplus_tree_search_range(tree, 6, 7, values);
  assert(count == 0);

  // Same data as before, but try some larger ranges
  count = bplus_tree_search_range(tree, 0, 7, values);
  assert(count == 7);
  assert(values[0] == 10);
  assert(values[1] == 20);
  assert(values[2] == 30);
  assert(values[3] == 31);
  assert(values[4] == 32);
  assert(values[5] == 40);
  assert(values[6] == 50);
  count = bplus_tree_search_range(tree, -100, 100, values);
  assert(count == 7);
  assert(values[0] == 10);
  assert(values[1] == 20);
  assert(values[2] == 30);
  assert(values[3] == 31);
  assert(values[4] == 32);
  assert(values[5] == 40);
  assert(values[6] == 50);
  count = bplus_tree_search_range(tree, -100, 3, values);
  assert(count == 2);
  assert(values[0] == 10);
  assert(values[1] == 20);
  count = bplus_tree_search_range(tree, -100, 4, values);
  assert(count == 5);
  assert(values[0] == 10);
  assert(values[1] == 20);
  assert(values[2] == 30);
  assert(values[3] == 31);
  assert(values[4] == 32);
  count = bplus_tree_search_range(tree, 3, 100, values);
  assert(count == 5);
  assert(values[0] == 30);
  assert(values[1] == 31);
  assert(values[2] == 32);
  assert(values[3] == 40);
  assert(values[4] == 50);

  bplus_tree_free(tree);
}

/**
 * Test the bplus_tree_search_range function with large random data and ranges.
 */
void test_bplus_tree_search_range() {
  srand(0);

  size_t size = BPLUS_TREE_ORDER * BPLUS_TREE_ORDER; // Ensure >= 2 levels
  if (size < 10000) {
    size = 10000;
  }

  int *data = malloc(sizeof(int) * size);
  for (size_t i = 0; i < size; i++) {
    data[i] = rand();
  }

  size_t *sorter = malloc(sizeof(size_t) * size);
  for (size_t i = 0; i < size; i++) {
    sorter[i] = i;
  }
  aquicksort(data, sorter, size);
  BPlusTree *tree = bplus_tree_create(data, sorter, size);

  // Test 10 random ranges, then three infinite ranges
  size_t *expected_values = malloc(sizeof(size_t) * size);
  size_t *result_values = malloc(sizeof(size_t) * size);
  for (int x = 0; x < 13; x++) {
    long lower, upper;
    switch (x) {
    case 10: // Lower bound is INT_MIN
      lower = INT_MIN;
      upper = rand();
      break;
    case 11: // Upper bound is INT_MAX
      lower = rand();
      upper = INT_MAX;
      break;
    case 12: // Both bounds are LONG_MIN and INT_MAX
      lower = LONG_MIN;
      upper = INT_MAX;
      break;
    default:
      lower = rand();
      upper = rand();
      if (lower > upper) {
        long tmp = lower;
        lower = upper;
        upper = tmp;
      }
      break;
    }

    // Brute force range search
    size_t expected_count = 0;
    for (size_t i = 0; i < size; i++) {
      if (data[sorter[i]] >= lower && data[sorter[i]] < upper) {
        expected_values[expected_count++] = sorter[i];
      }
    }

    // B+ tree range search and check that the results are consistent with the
    // brute force range search
    size_t result_count =
        bplus_tree_search_range(tree, lower, upper, result_values);
    assert(result_count == expected_count);
    for (size_t i = 0; i < result_count; i++) {
      assert(result_values[i] == expected_values[i]);
    }
  }

  bplus_tree_free(tree);
  free(data);
  free(sorter);
  free(expected_values);
  free(result_values);
}

int main() {
  TEST(bplus_tree_create);
  TEST(bplus_tree_insert);
  TEST(bplus_tree_search_cont);
  TEST(bplus_tree_search_range_cont_toy);
  TEST(bplus_tree_search_range_cont);
  TEST(bplus_tree_search_range_toy);
  TEST(bplus_tree_search_range);
  return 0;
}
