/**
 * @file bptree.h
 *
 * This header contains the implementation of a B+ tree, which in particular is
 * has keys as integer data and values as indices.
 */

#ifndef BPTREE_H__
#define BPTREE_H__

#include <stdbool.h>
#include <stddef.h>

#include "consts.h"

/**
 * The types of a B+ tree node.
 */
typedef enum BPlusNodeType {
  BPLUS_NODE_TYPE_INTERNAL,
  BPLUS_NODE_TYPE_LEAF,
} BPlusNodeType;

/**
 * The B+ tree node structure.
 *
 * Both internal and leaf nodes contain the type, the number of keys, and the
 * array of keys. Internal nodes further contain an array of children, and leaf
 * nodes further contain an array of values and a pointer to the next leaf node.
 *
 * There is always the same number of values as the number of keys for leaf
 * nodes, and there is always one more child than the number of keys for
 * internal nodes. The maximum number of children is fixed, which is the order
 * of the B+ tree. The maximum number of keys and values is thus one less than
 * the order of the B+ tree.
 */
typedef struct BPlusNode {
  enum BPlusNodeType type;
  int n_keys;
  int keys[BPLUS_TREE_ORDER - 1];
  union {
    struct {
      struct BPlusNode *children[BPLUS_TREE_ORDER];
    } internal;
    struct {
      size_t values[BPLUS_TREE_ORDER - 1];
      struct BPlusNode *next;
    } leaf;
  } spec;
} BPlusNode;

/**
 * The B+ tree structure.
 *
 * The tree contains a pointer to the root node, the number of levels (which
 * includes the root node but not the leaf level), and the number of values in
 * the tree (i.e., not counting the internal nodes).
 */
typedef struct BPlusTree {
  BPlusNode *root;
  int n_levels;
  size_t size;
} BPlusTree;

/**
 * Create a B+ tree by bulk loading sorted data.
 *
 * If the sorter is not provided, then data must be sorted itself. Otherwise,
 * the data can be unsorted and the sorter gives the order of the data, commonly
 * obtained by argsorting the data. The size is the number of elements in the
 * data. This function returns the created B+ tree on success or NULL on
 * failure.
 */
BPlusTree *bplus_tree_create(int *data, size_t *sorter, size_t size);

/**
 * Insert a key-value pair into the B+ tree.
 *
 * This function returns 0 on success and -1 on failure.
 */
int bplus_tree_insert(BPlusTree *tree, int key, size_t value);

/**
 * Perform a point search on the B+ tree, assuming contiguous values.
 *
 * This function returns the value of the target key in the target node, with
 * the same semantic as in binary search. If the key is larger (including
 * equal if aligned right) than all keys in the tree, this function will return
 * the size of the tree. Note that this function does not work with B+ trees
 * in general, but only with the assumption that the values (indices) are
 * contiguous.
 */
size_t bplus_tree_search_cont(BPlusTree *tree, int key, bool align_left);

/**
 * Perform a range search on the B+ tree, assuming contiguous values.
 *
 * This function is particularly used when the values (i.e., indices) are
 * contiguous, so that with by performing two point searches for the lower and
 * upper bounds, we can immediately materialize the range without linearly
 * scanning through the leaf level. This function will set the count to the
 * number of qualifying values (indices) and return the array of values
 * (indices). Even if the count is 0, the function will return a non-NULL
 * pointer as given by `malloc(0)`. The returned value being NULL always means
 * some error occurred during the search.
 */
size_t *bplus_tree_search_range_cont(BPlusTree *tree, long lower, long upper,
                                     size_t *count);

/**
 * Perform a range search on the B+ tree.
 *
 * This function does not assume contiguous values (indices). It will perform
 * one point search for the lower bound and then linearly scan through the leaf
 * level until reaching the upper bound or the end of the tree. This funciton
 * will return the number of qualifying values (indices) and set them in the
 * provided values pointer. The values pointer must thus be pre-allocated with
 * enough capacity.
 */
size_t bplus_tree_search_range(BPlusTree *tree, long lower, long upper,
                               size_t *values);

/**
 * Free a B+ tree.
 *
 * This function recursively frees all nodes in the B+ tree and then the tree
 * itself.
 */
void bplus_tree_free(BPlusTree *tree);

/**
 * [PRIVATE] Helper function to print a B+ tree node.
 *
 * The indent is the number of spaces to indent the output.
 */
void __print_bplus_node(BPlusNode *node, int indent);

/**
 * [PRIVATE] Helper function to print a B+ tree.
 */
void __print_bplus_tree(BPlusTree *tree);

#endif // BPTREE_H__
