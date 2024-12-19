/**
 * @file logging.h
 *
 * This header defines the logging utilities.
 */

#ifndef LOGGING_H__
#define LOGGING_H__

#include <stdio.h>

/**
 * Log to the given file pointer, enabled only when `LOG` is true.
 *
 * The first argument is the file pointer to log to. The rest of the arguments
 * are the same as `printf`.
 */
void log_file(FILE *out, const char *format, ...);

/**
 * Print to stderr in red.
 *
 * The arguments are the same as `printf`.
 */
void printf_error(const char *format, ...);

/**
 * Print to stdout in green.
 *
 * The arguments are the same as `printf`.
 */
void printf_info(const char *format, ...);

#endif /* LOGGING_H__ */
