/**
 * @file io.h
 *
 * This header defines various IO functionalities.
 */

#ifndef IO_H__
#define IO_H__

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

/**
 * A simple CSV struct.
 *
 * This struct contains the file size, the mmap'ed file data, the header row,
 * and the number of columns. It also stores an offset to indicate the offset
 * that the next read should start from.
 *
 * Warning: This is not a general-purpose CSV parser and should be used with
 * specific patterns.
 */
typedef struct CSV {
  size_t size;
  char *data;
  char *header;
  size_t n_cols;
  size_t offset;
} CSV;

/**
 * The status codes for the CSV parsing.
 */
typedef enum CSVParseStatus {
  // The parsing is successful and has no more rows to parse.
  CSV_PARSE_STATUS_EOF,
  // The parsing is successful and has more rows to parse.
  CSV_PARSE_STATUS_CONTINUE,
  // The parsing encounters an error.
  CSV_PARSE_STATUS_ERROR,
} CSVParseStatus;

/**
 * Load a CSV file from the specified path.
 *
 * This function will mmap the file to memory and parse the first row to get the
 * number of columns and the header string. It returns a pointer to the CSV
 * struct that contains basic information on success, or NULL on failure.
 */
CSV *load_csv(char *path);

/**
 * Close the CSV file.
 *
 * This function will munmap the file data and free the memory allocated for the
 * CSV struct.
 */
void close_csv(CSV *csv);

/**
 * Parse the next row of the CSV file.
 *
 * This function will store the parsed integers in the buffer and returns the
 * parsing status code.
 */
CSVParseStatus parse_next_row(CSV *csv, int *buffer);

/**
 * Get the catalog file for database persistence.
 *
 * If `write` is true: this function opens the catalog file in binary write
 * mode. This will truncate whatever is in the file if it already exists. On
 * success, it returns the file pointer and sets the status to 0, otherwise it
 * returns NULL and sets the status to -1.
 *
 * If `write` is false: this function opens the catalog file in binary read
 * mode. On error it returns NULL and sets the status to -1. The following are
 * several cases of success:
 *
 * - If the catalog file exists and is not empty, it returns the file pointer
 *   and sets the status to 0.
 * - If the catalog file exists but is empty, it returns NULL and sets the
 *   status to 1.
 * - If the catalog file does not exist, it creates an empty file, then returns
 *   NULL and sets the status to 1.
 *
 * In any case where a file pointer is returned, the caller is responsible for
 * closing the file after using it.
 */
FILE *get_catalog_file(bool write, int *status);

/**
 * Clear the database persistence directory.
 *
 * This function removes all column files in the database persistence directory.
 * It returns 0 on success or -1 on failure.
 */
int clear_db_persistence_dir();

/**
 * Map a column file to memory.
 *
 * This function creates a new file for the column data if it does not already
 * exist. The capacity represents the number of intergers that can be held in
 * the memory mapping. It returns a pointer to the mmap'ed memory on success, or
 * NULL on failure. The file descriptor of the column file is stored in the
 * given pointer on success.
 */
int *mmap_column_file(char *table_name, char *column_name, size_t capacity,
                      int *column_fd);

/**
 * Remap a column file with a new capacity.
 *
 * This function truncates the file to the new size and remaps the memory from
 * the old capacity to the new capacity. The capacities represent the number of
 * integers that can be held in the memory mapping. It returns a pointer to the
 * new mmap'ed memory on success, or NULL on failure.
 */
int *mremap_column_file(int *old_data, size_t old_capacity, size_t new_capacity,
                        int column_fd);

/**
 * Unmap a column file from memory.
 *
 * This function truncates the file to the new size, synchronizes the mmap'ed
 * memory to the file, and unmaps the memory. The capacity represents the number
 * of integers that can be held in the memory mapping. It returns 0 on success
 * or -1 on failure.
 */
int munmap_column_file(int *data, size_t capacity, int column_fd);

#endif /* IO_H__ */
