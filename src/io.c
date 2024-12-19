/**
 * @file io.c
 * @implements io.h
 */

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "consts.h"
#include "io.h"

/**
 * @implements load_csv
 */
CSV *load_csv(char *path) {
  int fd = open(path, O_RDONLY);
  if (fd < 0) {
    return NULL;
  }

  // Get the file size
  struct stat sb;
  if (fstat(fd, &sb) < 0) {
    close(fd);
    NULL;
  }

  // Map the file to memory
  char *data = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (data == MAP_FAILED) {
    close(fd);
    return NULL;
  }

  // Parse the first row to get the number of columns; start with 1 because even
  // if there's no comma there is at least one column
  size_t n_cols = 1;
  char *end = strchr(data, '\n');
  if (end == NULL) {
    munmap(data, sb.st_size);
    close(fd);
    return NULL;
  }
  for (char *ptr = data; ptr < end; ptr++) {
    if (*ptr == ',') {
      n_cols++;
    }
  }

  // Load the header row
  size_t offset = (end - data) + 1;
  char *header = malloc(offset);
  if (header == NULL) {
    munmap(data, sb.st_size);
    close(fd);
    return NULL;
  }
  strncpy(header, data, offset - 1);
  header[offset - 1] = '\0';

  // Allocate memory for the CSV struct
  CSV *csv = malloc(sizeof(CSV));
  if (csv == NULL) {
    munmap(data, sb.st_size);
    close(fd);
    return NULL;
  }
  csv->size = sb.st_size;
  csv->data = data;
  csv->header = header;
  csv->n_cols = n_cols;
  csv->offset = offset;

  return csv;
}

/**
 * @implements close_csv
 */
void close_csv(CSV *csv) {
  munmap(csv->data, csv->size);
  free(csv->header);
  free(csv);
}

/**
 * @implements parse_next_row
 */
CSVParseStatus parse_next_row(CSV *csv, int *buffer) {
  char *ptr = csv->data + csv->offset;

  // Jump to the first non-empty row since the last read
  while (*ptr == '\n') {
    ptr++;
  }

  // Check if we have reached the end of the file
  if (ptr >= csv->data + csv->size) {
    return CSV_PARSE_STATUS_EOF;
  }

  // Find the end of the row
  size_t current = 0;
  while (csv->offset < csv->size) {
    char *endptr;
    errno = 0; // Reset errno before calling strtol so we can check for status
    long value = strtol(ptr, &endptr, 10);

    // Check if the parsing was successful
    if (errno != 0 || ptr == endptr) {
      return CSV_PARSE_STATUS_ERROR;
    }

    // Set the value in the buffer and move the pointer to after the integer
    buffer[current++] = (int)value; // XXX: This is unsafe conversion
    ptr = endptr;

    // The character after the last element in a row should be a newline or a
    // null-terminator
    if (current == csv->n_cols) {
      if (*ptr != '\n' && *ptr != '\0') {
        return CSV_PARSE_STATUS_ERROR;
      }
      // Move the offset to the start of the next row and break
      csv->offset = (ptr - csv->data) + 1;
      break;
    }

    // This is not the last element in the row, so we expect a comma
    if (*ptr != ',') {
      return CSV_PARSE_STATUS_ERROR;
    }
    ptr++; // Skip the comma
  }

  return CSV_PARSE_STATUS_CONTINUE;
}

/**
 * @implements get_catalog_file
 */
FILE *get_catalog_file(bool write, int *status) {
  FILE *catalog;
  char catalog_path[sizeof(DB_PERSIST_DIR) + sizeof(DB_PERSIST_CATALOG_FILE) +
                    2];
  sprintf(catalog_path, "%s/%s", DB_PERSIST_DIR, DB_PERSIST_CATALOG_FILE);

  // write=true is simpler to handle; we just open the file in binary mode
  if (write) {
    catalog = fopen(catalog_path, "wb");
    if (catalog == NULL) {
      *status = -1;
      return NULL;
    }
    *status = 0;
    return catalog;
  }

  // Create the database persistence directory if it does not already exist
  int mkdir_status = mkdir(DB_PERSIST_DIR, 0755); // drwxr-xr-x
  if (mkdir_status < 0 && errno != EEXIST) {
    *status = -1;
    return NULL;
  }

  // Try to open the catalog file in binary mode; if opening fails it is likely
  // because the file does not exist yet, in which case we try to create it by
  // opening in write mode and closing immediately
  catalog = fopen(catalog_path, "rb");
  if (catalog == NULL) {
    catalog = fopen(catalog_path, "wb");
    if (catalog == NULL) {
      *status = -1;
      return NULL;
    }
    fclose(catalog);
    *status = 1;
    return NULL;
  }

  // The catalog file exists and is openable; we check if it is empty by seeking
  // to the end and checking the position, then rewinding back to the start
  fseek(catalog, 0, SEEK_END);
  if (ftell(catalog) == 0) {
    *status = 1;
    fclose(catalog);
    return NULL;
  }
  rewind(catalog);

  // The catalog file exists and is not empty, so we return the file pointer
  *status = 0;
  return catalog;
}

/**
 * @implements clear_db_persistence_dir
 */
int clear_db_persistence_dir() {
  struct dirent *entry;
  DIR *dp = opendir(DB_PERSIST_DIR);
  if (dp == NULL) {
    return -1;
  }

  // Iterate over all files in the directory and remove them, except for the
  // catalog file because regardless of what it will be overwritten by the
  // updated information on system shutdown
  bool success = true;
  char file_path[DEFAULT_BUFFER_SIZE];
  while ((entry = readdir(dp)) != NULL) {
    // Skip current directory, parent directory, and the catalog file
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
        strcmp(entry->d_name, DB_PERSIST_CATALOG_FILE) == 0) {
      continue;
    }

    // Construct the file path and remove the file
    snprintf(file_path, sizeof(file_path), "%s/%s", DB_PERSIST_DIR,
             entry->d_name);
    if (remove(file_path) < 0) {
      // Set the flag instead of returning error code immediately because we try
      // to clean up as much as possible
      success = false;
    }
  }
  closedir(dp);

  return success ? 0 : -1;
}

/**
 * @implements mmap_column_file
 */
int *mmap_column_file(char *table_name, char *column_name, size_t capacity,
                      int *column_fd) {
  char path[sizeof(DB_PERSIST_DIR) + MAX_SIZE_NAME * 2 + 3];
  sprintf(path, "%s/%s.%s", DB_PERSIST_DIR, table_name, column_name);

  // Open the file and create if it does not already exist; give read and write
  // permissions to the owner so that we can run xxd to inspect persisted data
  int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    return NULL;
  }

  // Truncate the file to the desired size; this is necessary to ensure that we
  // can mmap that many bytes to memory if the file is newly created
  size_t size = capacity * sizeof(int);
  if (ftruncate(fd, size) < 0) {
    close(fd);
    return NULL;
  }

  // Map the file to memory
  int *data = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (data == MAP_FAILED) {
    close(fd);
    return NULL;
  }

  *column_fd = fd;
  return data;
}

/**
 * @implements mremap_column_file
 */
int *mremap_column_file(int *old_data, size_t old_capacity, size_t new_capacity,
                        int column_fd) {
  size_t new_size = new_capacity * sizeof(int);

  // Truncate the file to the new size
  if (ftruncate(column_fd, new_size) < 0) {
    return NULL;
  }

  // Remap the memory to the new capacity
  int *new_data =
      mremap(old_data, old_capacity * sizeof(int), new_size, MREMAP_MAYMOVE);
  if (new_data == MAP_FAILED) {
    return NULL;
  }
  return new_data;
}

/**
 * @implements munmap_column_file
 */
int munmap_column_file(int *data, size_t capacity, int column_fd) {
  size_t size = capacity * sizeof(int);

  // Truncate the file to the new size
  if (ftruncate(column_fd, size) < 0) {
    return -1;
  }

  // Sync the memory to the file
  if (msync(data, size, MS_SYNC) < 0) {
    return -1;
  }

  // Unmap the memory
  if (munmap(data, capacity * sizeof(int)) < 0) {
    return -1;
  }

  close(column_fd);
  return 0;
}
