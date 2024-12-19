/**
 * @file bptree.c
 * @implements bptree.h
 */

#include <assert.h>
#include <math.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "binsearch.h"
#include "bptree.h"

#include <stdio.h>

/**
 * The access stack for internal nodes in a B+ tree.
 *
 * This structure is used to keep track of the access path to the leaf node
 * since we do not have parent pointers. `s` should always point to the bottom
 * of the stack and `sptr` should dynamically point to the top of the stack.
 * The number of elements can be obtained by the difference between the two
 * pointers.
 */
typedef struct _BPlusNodeAccessStack {
  BPlusNode **s;
  BPlusNode **sptr;
} _BPlusNodeAccessStack;

/**
 * Helper function to create an empty internal node.
 */
static inline BPlusNode *_create_internal_node() {
  BPlusNode *node = malloc(sizeof(BPlusNode)); // XXX: not checking for NULL
  if (node == NULL) {
    return NULL;
  }
  node->type = BPLUS_NODE_TYPE_INTERNAL;
  node->n_keys = 0;
  return node;
}

/**
 * Helper function to create an empty leaf node.
 */
static inline BPlusNode *_create_leaf_node() {
  BPlusNode *node = malloc(sizeof(BPlusNode)); // XXX: not checking for NULL
  node->type = BPLUS_NODE_TYPE_LEAF;
  node->n_keys = 0;
  node->spec.leaf.next = NULL;
  return node;
}

/**
 * Helper function to push a key up the B+ tree, append-only.
 *
 * This function takes an access stack of internal nodes, with the deepest node
 * being the root of the B+ tree. It will first check if the top node has free
 * space, in which case the key will be appended directly. Otherwise, it will
 * split the node at the median key and recursively push the median key up the
 * tree. The splitted node will be popped from the access stack before recursing
 * and the newly created node during splitting will be pushed onto the stack
 * after recursing so that the stack is up-to-date. The invariant is that, after
 * the recursion is finished, the given key will be the last key in the top
 * node of the stack.
 */
void _push_key_append_only(_BPlusNodeAccessStack *stack, int key) {
  // Assume a non-empty stack, peek the top node
  BPlusNode *node = *(stack->sptr - 1);
  if (node->n_keys < BPLUS_TREE_ORDER - 1) {
    // Node has free space, so we directly insert the key and return
    node->keys[node->n_keys++] = key;
    return;
  }

  // Node is full, find the split point (the key that will be promoted)
  int split_ind = BPLUS_TREE_ORDER / 2;
  int split_key = node->keys[split_ind];

  // Create a new internal node and move the keys and children to the right of
  // the split point to the new node, then insert the given new key
  BPlusNode *new_node = _create_internal_node();
  // The number of keys to copy is the number of keys to the right of the split
  // point; total number of keys is (BPLUS_TREE_ORDER - 1), number of keys up to
  // the split point (inclusive) is (split_ind + 1)
  int n_copy = BPLUS_TREE_ORDER - split_ind - 2;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#pragma GCC diagnostic ignored "-Wstringop-overflow"
  memcpy(new_node->keys, node->keys + split_ind + 1, sizeof(int) * n_copy);
  memcpy(new_node->spec.internal.children,
         node->spec.internal.children + split_ind + 1,
         sizeof(BPlusNode *) * (n_copy + 1));
#pragma GCC diagnostic pop
  new_node->n_keys = n_copy;
  new_node->keys[new_node->n_keys++] = key;
  node->n_keys = split_ind;

  // Pop the stack since the original node (i.e., current stack top) will no
  // longer be on the access path; later the new node will be pushed onto stack
  // (but not now because deeper levels in the stack may be changed as well)
  --stack->sptr;

  if (stack->sptr == stack->s) {
    // There are no more nodes in the access stack, so we need to create a new
    // root node and link to the current node and the new node; the new root
    // will be pushed to the empty stack
    BPlusNode *root = _create_internal_node();
    root->keys[root->n_keys++] = split_key;
    root->spec.internal.children[0] = node;
    root->spec.internal.children[1] = new_node;
    *(stack->sptr++) = root;
  } else {
    // There are more nodes in the access stack; we can recursively insert the
    // split key into the parent node
    _push_key_append_only(stack, split_key);
    BPlusNode *parent = *(stack->sptr - 1);
    parent->spec.internal.children[parent->n_keys] = new_node;
  }

  // Push the new node onto stack
  (*stack->sptr++) = new_node;
}

/**
 * @implements bplus_tree_create
 */
BPlusTree *bplus_tree_create(int *data, size_t *sorter, size_t size) {
  // Create the empty root node with leftmost child being the first leaf node;
  // note the cases of sorter being NULL and non-NULL only differ in using index
  // `i` or `sorter[i]`
  size_t i = 0;
  BPlusNode *leaf, *internal;
  leaf = _create_leaf_node();
  if (sorter == NULL) {
    for (; i < BPLUS_TREE_ORDER - 1 && i < size; i++) {
      leaf->keys[i] = data[i];
      leaf->spec.leaf.values[i] = i;
      leaf->n_keys++;
    }
  } else {
    for (; i < BPLUS_TREE_ORDER - 1 && i < size; i++) {
      leaf->keys[i] = data[sorter[i]];
      leaf->spec.leaf.values[i] = sorter[i];
      leaf->n_keys++;
    }
  }
  internal = _create_internal_node();
  internal->spec.internal.children[0] = leaf;

  if (i == 0) {
    // No data to insert, return the empty tree
    BPlusTree *tree = malloc(sizeof(BPlusTree));
    if (tree == NULL) {
      return NULL;
    }
    tree->root = internal;
    tree->size = 0;
    tree->n_levels = 1;
    return tree;
  }

  // Create the access stack for the internal nodes and push the root node
  // initially since that is the only node in our access path for now
  int stack_size = (int)ceil(log(size) / log(BPLUS_TREE_ORDER)) * 2;
  stack_size = stack_size < 2 ? 2 : stack_size;
  BPlusNode **stack_array = malloc(sizeof(BPlusNode *) * stack_size);
  if (stack_array == NULL) {
    return NULL;
  }
  _BPlusNodeAccessStack stack = {.s = stack_array, .sptr = stack_array};
  *(stack.sptr++) = internal;

  // Bulk load the remaining data into the tree; note the cases of sorter being
  // NULL and non-NULL only differ in using index `i` or `sorter[i]`
  BPlusNode *new_leaf;
  if (sorter == NULL) {
    while (i < size) {
      new_leaf = _create_leaf_node();
      leaf->spec.leaf.next = new_leaf;
      leaf = new_leaf;
      for (int j = 0; j < BPLUS_TREE_ORDER - 1 && i < size; i++, j++) {
        leaf->keys[j] = data[i];
        leaf->spec.leaf.values[j] = i;
        leaf->n_keys++;
      }
      _push_key_append_only(&stack, leaf->keys[0]);
      internal = *(stack.sptr - 1);
      internal->spec.internal.children[internal->n_keys] = leaf;
    }
  } else {
    while (i < size) {
      new_leaf = _create_leaf_node();
      leaf->spec.leaf.next = new_leaf;
      leaf = new_leaf;
      for (int j = 0; j < BPLUS_TREE_ORDER - 1 && i < size; i++, j++) {
        leaf->keys[j] = data[sorter[i]];
        leaf->spec.leaf.values[j] = sorter[i];
        leaf->n_keys++;
      }
      _push_key_append_only(&stack, leaf->keys[0]);
      internal = *(stack.sptr - 1);
      internal->spec.internal.children[internal->n_keys] = leaf;
    }
  }

  BPlusTree *tree = malloc(sizeof(BPlusTree));
  if (tree == NULL) {
    free(stack.s);
    return NULL;
  }
  tree->root = *stack.s;
  tree->size = size;
  tree->n_levels = stack.sptr - stack.s;
  free(stack.s);
  return tree;
}

/**
 * Helper function to push a key up the B+ tree, allowing insertions.
 *
 * This function takes an access stack of internal nodes, with the deepest node
 * being the root of the B+ tree. It will first check if the top node has free
 * space, in which case the key will be inserted in a sorted manner directly.
 * Otherwise, it will split the node at the median key and recursively push the
 * median key up the tree. The splitted node will be popped from the access
 * stack before recursing and the newly created node during splitting will be
 * pushed onto the stack after recursing so that the stack is up-to-date. The
 * invariant is that, after the recursion is finished, the given key will be at
 * the returned slot index in the top node of the stack; special case is -1
 * where the key is promoted to some higher level.
 */
int _push_key(_BPlusNodeAccessStack *stack, int key) {
  // Assume a non-empty stack, peek the top node
  BPlusNode *node = *(stack->sptr - 1);
  if (node->n_keys < BPLUS_TREE_ORDER - 1) {
    // Node has free space, so we directly insert the key and return the index
    // of the inserted key
    int ind = binsearch(node->keys, key, node->n_keys, false);
    memcpy(node->keys + ind + 1, node->keys + ind,
           sizeof(int) * (node->n_keys - ind));
    memcpy(node->spec.internal.children + ind + 2,
           node->spec.internal.children + ind + 1,
           sizeof(BPlusNode *) * (node->n_keys - ind));
    node->keys[ind] = key;
    node->n_keys++;
    return ind;
  }

  // Node is full, find the split point (the key that will be promoted)
  int split_ind = BPLUS_TREE_ORDER / 2;
  int ind = binsearch(node->keys, key, node->n_keys, false);

  // Create a new internal node to hold the moved keys and children
  int split_key;
  int slot;
  BPlusNode *slot_node;
  BPlusNode *new_node = _create_internal_node();
  if (ind < split_ind) {
    // Copy the keys and children to the right of the split point to the new
    // node, because the insertion point is to the left of the split point
    memcpy(new_node->keys, node->keys + split_ind,
           sizeof(int) * (BPLUS_TREE_ORDER - split_ind - 1));
    memcpy(new_node->spec.internal.children,
           node->spec.internal.children + split_ind,
           sizeof(BPlusNode *) * (BPLUS_TREE_ORDER - split_ind));
    // Copy the keys and children to the right of the insertion point but to the
    // left of the split point one position to the right; note this excludes the
    // split point itself because it will be promoted anyways
    memcpy(node->keys + ind + 1, node->keys + ind,
           sizeof(int) * (split_ind - ind - 1));
    memcpy(node->spec.internal.children + ind + 2,
           node->spec.internal.children + ind + 1,
           sizeof(BPlusNode *) * (split_ind - ind - 1));
    // Insert the key at the insertion point; the split key is the key to the
    // left of the split point because we are inserting to the left and things
    // are pushed one position to the right; the reserved slot will then be the
    // insertion point, i.e., right child of the key
    split_key = node->keys[split_ind - 1];
    node->keys[ind] = key;
    slot = ind;
    slot_node = node;
  } else if (ind == split_ind) {
    // The insertion point is at the split point, so the keys to the right of
    // the split point are copied to the new node, and the children are copied
    // in the same way but with one position offset to the right of the new node
    // because its first child will be the reserved slot
    memcpy(new_node->keys, node->keys + split_ind,
           sizeof(int) * (BPLUS_TREE_ORDER - split_ind - 1));
    memcpy(new_node->spec.internal.children + 1,
           node->spec.internal.children + split_ind + 1,
           sizeof(BPlusNode *) * (BPLUS_TREE_ORDER - split_ind - 1));
    // There is no need to insert the key because it is promoted; the split key
    // is just the key itself; the reserved slot is the first child of the new
    // node so we need to give slot=-1 so that slot+1=0
    split_key = key;
    slot = -1;
    slot_node = new_node;
  } else {
    // Copy the keys and children to the right of the split point but to the
    // left of the insertion point to the new node
    memcpy(new_node->keys, node->keys + split_ind + 1,
           sizeof(int) * (ind - split_ind - 1));
    memcpy(new_node->spec.internal.children,
           node->spec.internal.children + split_ind + 1,
           sizeof(BPlusNode *) * (ind - split_ind));
    // Copy the keys and children to the right of the insertion point to one
    // position from the previous step
    memcpy(new_node->keys + ind - split_ind, node->keys + ind,
           sizeof(int) * (BPLUS_TREE_ORDER - ind - 1));
    memcpy(new_node->spec.internal.children + ind - split_ind + 1,
           node->spec.internal.children + ind + 1,
           sizeof(BPlusNode *) * (BPLUS_TREE_ORDER - ind - 1));
    // Insert the key at the insertion point (mapped to the new node); the split
    // key is the key at the split point because we are inserting to the right
    // and things to the left are unchanged; the reserved slot will then be the
    // insertion point, i.e., right child of the key
    split_key = node->keys[split_ind];
    new_node->keys[ind - split_ind - 1] = key;
    slot = ind - split_ind - 1;
    slot_node = new_node;
  }
  node->n_keys = split_ind;
  new_node->n_keys = BPLUS_TREE_ORDER - split_ind - 1;

  // Pop the stack since the original node (i.e., current stack top) will no
  // longer be on the access path; later the new node will be pushed onto stack
  // (but not now because deeper levels in the stack may be changed as well)
  --stack->sptr;

  if (stack->sptr == stack->s) {
    // There are no more nodes in the access stack, so we need to create a new
    // root node and link to the current node and the new node; the new root
    // will be pushed to the empty stack
    BPlusNode *root = _create_internal_node();
    root->keys[root->n_keys++] = split_key;
    root->spec.internal.children[0] = node;
    root->spec.internal.children[1] = new_node;
    *(stack->sptr++) = root;
  } else {
    // There are more nodes in the access stack; we can recursively insert the
    // split key into the parent node
    int slot = _push_key(stack, split_key);
    BPlusNode *parent = *(stack->sptr - 1);
    parent->spec.internal.children[slot + 1] = new_node;
  }

  // Push the new node onto stack
  (*stack->sptr++) = slot_node;
  return slot;
}

/**
 * @implements bplus_tree_insert
 */
int bplus_tree_insert(BPlusTree *tree, int key, size_t value) {
  // Initialize the access stack; we need one more level than the tree depth
  // because we if all nodes on the access path are full, we need to split the
  // root node and create a new root node which increments depth by one
  BPlusNode **stack_array = malloc(sizeof(BPlusNode *) * (tree->n_levels + 1));
  if (stack_array == NULL) {
    return -1;
  }
  _BPlusNodeAccessStack stack = {.s = stack_array, .sptr = stack_array};

  // Starting from the root, binary search until reaching a leaf node
  BPlusNode *node = tree->root;
  while (node->type == BPLUS_NODE_TYPE_INTERNAL) {
    *(stack.sptr++) = node;
    int ind = binsearch(node->keys, key, node->n_keys, false);
    node = node->spec.internal.children[ind];
  }

  // We are now at the leaf node; if there is space, insert the key directly
  // in a sorted manner
  if (node->n_keys < BPLUS_TREE_ORDER - 1) {
    int ind = binsearch(node->keys, key, node->n_keys, false);
    memcpy(node->keys + ind + 1, node->keys + ind,
           sizeof(int) * (node->n_keys - ind));
    memcpy(node->spec.leaf.values + ind + 1, node->spec.leaf.values + ind,
           sizeof(size_t) * (node->n_keys - ind));
    node->keys[ind] = key;
    node->spec.leaf.values[ind] = value;
    node->n_keys++;
    free(stack.s);
    tree->size++;
    return 0;
  }

  // The leaf node is full, so we need to split the node and push the median
  // key up the tree; see the splitting strategy in _push_key, with only slight
  // differences that (1) we care about values (which exactly correspond to the
  // keys) so there is no need to care about splitting children, and (2) the
  // split key is also copied to the new node (because the splitting point in
  // leaf node is COPIED when promoted instead of MOVED)
  int split_ind = BPLUS_TREE_ORDER / 2;
  int ind = binsearch(node->keys, key, node->n_keys, false);
  BPlusNode *new_node = _create_leaf_node();
  if (ind < split_ind) {
    memcpy(new_node->keys, node->keys + split_ind - 1,
           sizeof(int) * (BPLUS_TREE_ORDER - split_ind));
    memcpy(new_node->spec.leaf.values, node->spec.leaf.values + split_ind - 1,
           sizeof(size_t) * (BPLUS_TREE_ORDER - split_ind));
    memcpy(node->keys + ind + 1, node->keys + ind,
           sizeof(int) * (split_ind - ind - 1));
    memcpy(node->spec.leaf.values + ind + 1, node->spec.leaf.values + ind,
           sizeof(size_t) * (split_ind - ind - 1));
    node->keys[ind] = key;
    node->spec.leaf.values[ind] = value;
  } else if (ind == split_ind) {
    memcpy(new_node->keys + 1, node->keys + split_ind,
           sizeof(int) * (BPLUS_TREE_ORDER - split_ind - 1));
    memcpy(new_node->spec.leaf.values + 1, node->spec.leaf.values + split_ind,
           sizeof(size_t) * (BPLUS_TREE_ORDER - split_ind - 1));
    new_node->keys[0] = key;
    new_node->spec.leaf.values[0] = value;
  } else {
    memcpy(new_node->keys, node->keys + split_ind,
           sizeof(int) * (ind - split_ind));
    memcpy(new_node->spec.leaf.values, node->spec.leaf.values + split_ind,
           sizeof(size_t) * (ind - split_ind));
    memcpy(new_node->keys + ind - split_ind + 1, node->keys + ind,
           sizeof(int) * (BPLUS_TREE_ORDER - ind - 1));
    memcpy(new_node->spec.leaf.values + ind - split_ind + 1,
           node->spec.leaf.values + ind,
           sizeof(size_t) * (BPLUS_TREE_ORDER - ind - 1));
    new_node->keys[ind - split_ind] = key;
    new_node->spec.leaf.values[ind - split_ind] = value;
  }
  node->n_keys = split_ind;
  new_node->n_keys = BPLUS_TREE_ORDER - split_ind;

  // Rewire the leaf nodes
  BPlusNode *next_node = node->spec.leaf.next;
  new_node->spec.leaf.next = next_node;
  node->spec.leaf.next = new_node;

  // The split key is the first key in the new node; promote it up the tree
  int slot = _push_key(&stack, new_node->keys[0]);
  BPlusNode *parent = *(stack.sptr - 1);
  parent->spec.internal.children[slot + 1] = new_node;

  // Note that the tree root may have changed because of node splitting, but we
  // can guarantee that the root is the deepest node in the stack
  tree->root = *stack.s;
  tree->n_levels = stack.sptr - stack.s;
  tree->size++;
  free(stack.s);
  return 0;
}

/**
 * B+ tree point search helper.
 *
 * This function sets the target node that it lands on and returns the index of
 * the target key in the target node. The target node being NULL means that the
 * key is larger (including equal if aligned right) than all keys in the tree.
 */
int _bplus_tree_search_helper(BPlusTree *tree, int key, bool align_left,
                              BPlusNode **target) {
  // Starting from the root, binary search until reaching a leaf node
  int ind;
  BPlusNode *node = tree->root;
  while (node->type == BPLUS_NODE_TYPE_INTERNAL) {
    ind = binsearch(node->keys, key, node->n_keys, align_left);
    node = node->spec.internal.children[ind];
  }

  // Binary search the leaf node
  ind = binsearch(node->keys, key, node->n_keys, align_left);
  if (ind == node->n_keys) {
    *target = node->spec.leaf.next;
    return 0;
  }
  *target = node;
  return ind;
}

/**
 * @implements bplus_tree_search_cont
 */
size_t bplus_tree_search_cont(BPlusTree *tree, int key, bool align_left) {
  BPlusNode *node;
  int ind = _bplus_tree_search_helper(tree, key, align_left, &node);
  return node == NULL ? tree->size : node->spec.leaf.values[ind];
}

/**
 * @implements bplus_tree_search_range_cont
 */
size_t *bplus_tree_search_range_cont(BPlusTree *tree, long lower, long upper,
                                     size_t *count) {
  if (lower >= upper) {
    *count = 0;
    return malloc(0);
  }

  // Search for the lower bound aligned left, because we want to avoid missing
  // duplicates
  size_t lower_value = bplus_tree_search_cont(tree, lower, true);
  if (lower_value == tree->size) {
    // This means that the lower bound is larger than all keys in the tree; we
    // can return with nothing found
    *count = 0;
    return malloc(0);
  }

  // Search for the upper bound aligned left, because the upper bound is
  // exclusive
  size_t upper_value = bplus_tree_search_cont(tree, upper, true);

  // Values (indices) are assumed contiguous, so we can directly materialize the
  // range [lower_value, upper_value)
  *count = upper_value - lower_value;
  size_t *values = malloc(sizeof(size_t) * *count);
  if (values == NULL) {
    return NULL;
  }
  for (size_t i = 0; i < *count; i++) {
    values[i] = lower_value + i;
  }
  return values;
}

/**
 * @implements bplus_tree_search_range
 */
size_t bplus_tree_search_range(BPlusTree *tree, long lower, long upper,
                               size_t *values) {
  if (lower >= upper) {
    return 0;
  }

  // Search for the lower bound aligned left, because we want to avoid missing
  // duplicates
  BPlusNode *node;
  int ind = _bplus_tree_search_helper(tree, lower, true, &node);
  if (node == NULL) {
    // This means that the lower bound is larger than all keys in the tree; we
    // can return with nothing found
    return 0;
  }

  // Starting from the landed leaf node and key index, we linearly scan the leaf
  // level of the B+ tree until we reach the upper bound or the end of the tree
  size_t count = 0;
  while (node != NULL) {
    for (; ind < node->n_keys && node->keys[ind] < upper; ind++) {
      values[count++] = node->spec.leaf.values[ind];
    }
    if (ind == node->n_keys) {
      // Hitting the end of the current leaf node, move to the first key of the
      // next leaf node
      node = node->spec.leaf.next;
      ind = 0;
    } else {
      // We reached here not because hitting the end of the current leaf node,
      // but because hitting the upper bound, so we should stop here
      break;
    }
  }
  return count;
}

/**
 * Helper function to recursively free a B+ tree node.
 */
void _free_node(BPlusNode *node) {
  if (node->type == BPLUS_NODE_TYPE_INTERNAL) {
    for (int i = 0; i < node->n_keys + 1; i++) {
      _free_node(node->spec.internal.children[i]);
    }
  }
  free(node);
}

/**
 * @implements bplus_tree_free
 */
void bplus_tree_free(BPlusTree *tree) {
  if (tree == NULL) {
    return;
  }
  _free_node(tree->root);
  free(tree);
}

/**
 * @implements __print_bplus_node
 */
void __print_bplus_node(BPlusNode *node, int indent) {
  if (node->type == BPLUS_NODE_TYPE_INTERNAL) {
    for (int i = 0; i < node->n_keys; i++) {
      __print_bplus_node(node->spec.internal.children[i], indent + 4);
      printf("%*s%d <%p>\n", indent, "", node->keys[i], (void *)node);
    }
    __print_bplus_node(node->spec.internal.children[node->n_keys], indent + 4);
  } else {
    printf("%*s[ ", indent, "");
    for (int i = 0; i < node->n_keys && i < 5; i++) {
      printf("%d (%zu) ", node->keys[i], node->spec.leaf.values[i]);
    }
    if (node->n_keys > 5) {
      printf("... ");
    }
    printf("] (length=%d)\n", node->n_keys);
    printf("%*s\033[90m<%p -> %p>\033[0m\n", indent, "", (void *)node,
           (void *)node->spec.leaf.next);
  }
}

/**
 * @implements __print_bplus_tree
 */
void __print_bplus_tree(BPlusTree *tree) {
  printf("Depth: %d\n", tree->n_levels);
  __print_bplus_node(tree->root, 0);
}
