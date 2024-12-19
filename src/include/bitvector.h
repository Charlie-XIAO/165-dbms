/**
 * @file bitvector.h
 *
 * This header defines the bit vector structure, partially referring to:
 * https://c-faq.com/misc/bitsets.html
 */

#ifndef BITVECTOR_H__
#define BITVECTOR_H__

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>

#define _BITMASK(b) (1 << ((b) % CHAR_BIT))
#define _BITSLOT(b) ((b) / CHAR_BIT)
#define _BITNSLOTS(nb) ((nb + CHAR_BIT - 1) / CHAR_BIT)

typedef struct BitVector {
  unsigned char *data;
  size_t length;
} BitVector;

/**
 * Initialize a bit vector with the specified length and all false.
 *
 * This returns NULL if the creation failed, otherwise returns the pointer to
 * the create bit vector.
 */
static inline BitVector *bitvector_create(size_t length) {
  BitVector *bv = malloc(sizeof(BitVector));
  if (bv == NULL) {
    return NULL;
  }
  bv->length = length;
  bv->data = (unsigned char *)calloc(_BITNSLOTS(length), sizeof(unsigned char));
  if (bv->data == NULL) {
    free(bv);
    return NULL;
  }
  return bv;
}

/**
 * Free a bit vector.
 */
static inline void bitvector_free(BitVector *bv) {
  if (bv != NULL) {
    free(bv->data);
    free(bv);
  }
}

/**
 * Set the specified bit to true.
 */
static inline void bitvector_set(BitVector *bv, size_t bit) {
  if (bit < bv->length) {
    bv->data[_BITSLOT(bit)] |= _BITMASK(bit);
  }
}
/**
 * Unset the specified bit to false.
 */
static inline void bitvector_unset(BitVector *bv, size_t bit) {
  if (bit < bv->length) {
    bv->data[_BITSLOT(bit)] &= ~_BITMASK(bit);
  }
}

/**
 * Test if the specified bit is true.
 */
static inline bool bitvector_test(const BitVector *bv, size_t bit) {
  if (bit < bv->length) {
    return bv->data[_BITSLOT(bit)] & _BITMASK(bit);
  }
  return false;
}

#endif /* BITVECTOR_H__ */
