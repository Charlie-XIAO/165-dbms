/**
 * @file consts.h
 *
 * This header defines shared constants.
 */

#ifndef CONSTS_H__
#define CONSTS_H__

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

#define SCAN_CALLBACK_SELECT_FLAG 0x01
#define SCAN_CALLBACK_MIN_FLAG 0x02
#define SCAN_CALLBACK_MAX_FLAG 0x04
#define SCAN_CALLBACK_SUM_FLAG 0x08

#ifndef LOG
/**
 * Whether to enable logging.
 *
 * By default logging is disabled.
 */
#define LOG 0
#endif

#ifndef SOCK_PATH
/**
 * The path to the Unix socket used for server-client communication.
 *
 * On Windows we want this to be written to a Docker container-only path.
 */
#define SOCK_PATH "cs165_unix_socket"
#endif

#ifndef DB_PERSIST_DIR
/**
 * The database persistence directory.
 */
#define DB_PERSIST_DIR ".cs165_db"
#endif

#ifndef DB_PERSIST_CATALOG_FILE
/**
 * The file name if the database catalog within the persistence directory.
 */
#define DB_PERSIST_CATALOG_FILE "__catalog__"
#endif

/**
 * A default buffer size with no specific purpose.
 */
#define DEFAULT_BUFFER_SIZE 1024

/**
 * The default buffer size for socket communication.
 */
#define DEFAULT_SOCKET_BUFFER_SIZE 4096

/**
 * The size of the thread task queue.
 */
#define THREAD_TASK_QUEUE_SIZE 1024

/**
 * The size limit of the name of an object in the database.
 */
#define MAX_SIZE_NAME 64

/**
 * The size limit of the name of a handle in the client context.
 */
#define HANDLE_MAX_SIZE 64

/**
 * The maximum number of handles that can be printed with a single print
 * command.
 */
#define MAX_PRINT_HANDLES 64

/**
 * The number of tables that a database can hold initially.
 */
#define INIT_NUM_TABLES_IN_DB 16

/**
 * The factor by which to expand the database capacity when it is full.
 */
#define EXPAND_FACTOR_DB 2

/**
 * The number of rows that a table can hold initially.
 */
#define INIT_NUM_ROWS_IN_TABLE 1024

/**
 * The factor by which to expand the table capacity when it is full.
 */
#define EXPAND_FACTOR_TABLE 2

/**
 * The factor by which to shrink the table capacity when it is underutilized.
 */
#define SHRINK_FACTOR_TABLE 2

/**
 * The number of handles of a type that a client context can hold initially.
 */
#define INIT_NUM_HANDLES_IN_CLIENT_CONTEXT 1

/**
 * The factor by which to expand the handle capacity of a type when it is full.
 */
#define EXPAND_FACTOR_CLIENT_CONTEXT 2

/**
 * The number of each type of operator that a batch context can hold initially.
 */
#define INIT_NUM_OPS_IN_BATCH_CONTEXT 1

/**
 * The factor by which to expand the batch context capacity for a specific type
 * of operator when it is full.
 */
#define EXPAND_FACTOR_BATCH_CONTEXT 2

/**
 * The number of elements to allocate initially for the result of a join, if
 * the maximum possible size (cartesion product of input sizes) is larger than
 * this number.
 */
#define INIT_NUM_ELEMS_IN_JOIN_RESULT 1024

/**
 * The factor by which to expand the join result buffer when it is full.
 */
#define EXPAND_FACTOR_JOIN_RESULT 2

/**
 * The number of elements per batch when loading multiple rows of data. The
 * elements are integers (4B), so per batch is 4KB which is the typical page
 * size.
 */
#define NUM_ELEMS_PER_LOAD_BATCH 1024

/**
 * The number of pages per scan task, if the scan is parallelized.
 */
#define NUM_PAGES_PER_SCAN_TASK 32

/**
 * The order of the B+ tree.
 *
 * A node holds `order-1` keys, then either `order` children (internal nodes) or
 * `order-1` values (leaf nodes). Assume that keys (int) are 4 bytes, values
 * (size_t) are 8 bytes, and child pointers are 8 bytes. Also assume that page
 * size is 4096 bytes, order 320 fits in a page. However, this may not be the
 * ideal order in practice depending on various factors, and may need to be
 * tuned by experimentation.
 */
#define BPLUS_TREE_ORDER 320

/**
 * The threshold for the hash join algorithm to choose naive-hash or grace-hash.
 *
 * If both input arrays to join have sizes less than this threshold, the hash
 * join algorithm will use the naive-hash algorithm. Otherwise, it will use the
 * grace-hash algorithm.
 */
#define NAIVE_GRACE_JOIN_THRESHOLD 100000

#endif /* CONSTS_H__ */
