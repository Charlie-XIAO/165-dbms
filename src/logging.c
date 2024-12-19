/**
 * @file logging.c
 */

#include <stdarg.h>

#include "consts.h"
#include "logging.h"

/**
 * @implements log_file
 */
void log_file(FILE *out, const char *format, ...) {
#if LOG
  va_list v;
  va_start(v, format);
  vfprintf(out, format, v);
  va_end(v);
#else
  (void)out;
  (void)format;
#endif
}

/**
 * @implements printf_error
 */
void printf_error(const char *format, ...) {
  va_list v;
  va_start(v, format);
  fprintf(stderr, ANSI_COLOR_RED);
  vfprintf(stderr, format, v);
  fprintf(stderr, ANSI_COLOR_RESET);
  va_end(v);
}

/**
 * @implements printf_info
 */
void printf_info(const char *format, ...) {
  va_list v;
  va_start(v, format);
  fprintf(stdout, ANSI_COLOR_GREEN);
  vfprintf(stdout, format, v);
  fprintf(stdout, ANSI_COLOR_RESET);
  fflush(stdout);
  va_end(v);
}
