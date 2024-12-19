#ifndef CS165_HASH_TABLE_H__
#define CS165_HASH_TABLE_H__

typedef int keyType;
typedef int valType;

typedef struct hashnode {
  keyType key;
  valType value;
  struct hashnode *next;
} hashnode;

// Hash table structure, which is essentially an array of linked lists.
typedef struct hashtable {
  int size;
  hashnode **array;
} hashtable;

int allocate(hashtable **ht, int size);
int put(hashtable *ht, keyType key, valType value);
int get(hashtable *ht, keyType key, valType *values, int num_values,
        int *num_results);
int erase(hashtable *ht, keyType key);
int deallocate(hashtable *ht);

#endif
