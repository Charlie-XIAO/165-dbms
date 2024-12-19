#include "hash_table.h"
#include <stdlib.h>

// The naive hash function.
static int hash(keyType key, int size) { return key % size; }

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if
// the parameter passed to the method is not null, if malloc fails, etc).
int allocate(hashtable **ht, int size) {
  // Allocate memory for the hashtable struct and let ht point to its address
  *ht = (hashtable *)malloc(sizeof(hashtable));
  if (*ht == NULL) {
    return -1;
  }
  (*ht)->size = size;

  // Allocate memory for the array of hashnode pointers
  (*ht)->array = (hashnode **)malloc(sizeof(hashnode *) * size);
  if ((*ht)->array == NULL) {
    free(*ht); // Remember to free previously allocated memory
    return -1;
  }

  // Initialize the array of hashnode pointers as NULL
  for (int i = 0; i < size; i++) {
    (*ht)->array[i] = NULL;
  }

  return 0;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is
// called and fails).
int put(hashtable *ht, keyType key, valType value) {
  if (ht == NULL) {
    return -1;
  }
  int hash_key = hash(key, ht->size);

  // Allocate memory for a new hashnode that stores the key-value pair
  hashnode *node = (hashnode *)malloc(sizeof(hashnode));
  if (node == NULL) {
    return -1;
  }
  node->key = key;
  node->value = value;

  // Insert the new hashnode to the front of the linked list at the hash index;
  // note that if we want to insert at the end of the linked list we would need
  // to traverse which is not efficient, thus the choice to insert at the front
  node->next = ht->array[hash_key];
  ht->array[hash_key] = node;

  return 0;
}

// This method retrieves entries with a matching key and stores the
// corresponding values in the values array. The size of the values array is
// given by the parameter num_values. If there are more matching entries than
// num_values, they are not stored in the values array to avoid a buffer
// overflow. The function returns the number of matching entries using the
// num_results pointer. If the value of num_results is greater than num_values,
// the caller can invoke this function again (with a larger buffer) to get
// values that it missed during the first call. This method returns an error
// code, 0 for success and -1 otherwise (e.g., if the hashtable is not
// allocated).
int get(hashtable *ht, keyType key, valType *values, int num_values,
        int *num_results) {
  if (ht == NULL) {
    return -1;
  }
  if (num_results == NULL) {
    return -1;
  }
  int hash_key = hash(key, ht->size);

  // Traverse the linked list at the hash index to find at most `num_values`
  // matching key-value pairs
  int count = 0;
  hashnode *current = ht->array[hash_key];
  while (current != NULL) {
    if (count >= num_values) {
      break; // Stop traversing if we have stored enough matches
    }
    if (current->key == key) {
      values[count] = current->value;
      count++;
    }
    current = current->next;
  }

  // Continue traversing the linked list in case there are more potential
  // matches
  while (current != NULL) {
    if (current->key == key) {
      count++;
    }
    current = current->next;
  }

  // Store the total number of matches
  *num_results = count;

  return 0;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the
// hashtable is not allocated).
int erase(hashtable *ht, keyType key) {
  if (ht == NULL) {
    return -1;
  }
  int hash_key = hash(key, ht->size);

  // First make sure that the first node in the linked list at the hash index
  // does not match the key to be deleted
  hashnode *current = ht->array[hash_key];
  while (current != NULL && current->key == key) {
    ht->array[hash_key] = current->next;
    free(current);
    current = ht->array[hash_key];
  }

  // Now we are certain that the first node in the linked list at the hash index
  // does not need to be removed; we can proceed to traverse through the linked
  // list and remove all matching keys
  hashnode *prev = ht->array[hash_key];
  if (prev == NULL) {
    return 0; // Everything has been removed so we are done
  }
  current = prev->next;
  while (current != NULL) {
    if (current->key == key) {
      prev->next = current->next;
      free(current);
      current = prev->next;
    } else {
      prev = current;
      current = prev->next;
    }
  }

  return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable *ht) {
  if (ht == NULL) {
    return 0;
  }

  // Traverse each linked list to free all nodes in within
  for (int i = 0; i < ht->size; i++) {
    hashnode *current = ht->array[i];
    // Free each node in the linked list
    while (current != NULL) {
      hashnode *next = current->next;
      free(current);
      current = next;
    }
  }

  free(ht->array);
  free(ht);

  return 0;
}
