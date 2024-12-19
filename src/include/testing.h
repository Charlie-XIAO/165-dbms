/**
 * @file testing.h
 *
 * This header contains testing utilities.
 */

#ifndef TESTING_H__
#define TESTING_H__

#include <stdio.h>

/**
 * Convenience macro to invoke a test function.
 *
 * The test function should be named `test_<func_name>`.
 */
#define TEST(func_name)                                                        \
  do {                                                                         \
    printf("\x1b[33m\xE2\x80\xA6 %s \x1b[0m", #func_name);                     \
    fflush(stdout);                                                            \
    test_##func_name();                                                        \
    printf("\r\x1b[2K\x1b[32m\xE2\x9C\x93 %s\x1b[0m\n", #func_name);           \
  } while (0)

#endif // TESTING_H__
