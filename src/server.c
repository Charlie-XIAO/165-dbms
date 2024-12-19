/**
 * @file server.c
 *
 * This file provides a basic Unix socket implementation for a server used in an
 * interactive client-server database.
 *
 * For more information on Unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 */

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "cmdload.h"
#include "comm.h"
#include "consts.h"
#include "db_operator.h"
#include "io.h"
#include "join.h"
#include "logging.h"
#include "message.h"
#include "parse.h"
#include "scan.h"
#include "sysinfo.h"
#include "thread_pool.h"

/**
 * The context of a load command.
 *
 * The load command is a multi-step process that requires multiple messages to
 * be sent between the client and the server. This context is used to keep track
 * of the state of the load command. The query is the load DbOperator, which is
 * NULL if the load command is not started or is terminated, otherwise malloc'ed
 * and updated in the process. The error is the error message if the load query
 * fails, otherwise NULL. XXX: The error message is currently always non-
 * malloc'ed, i.e., no need to free.
 */
typedef struct LoadCommandContext {
  DbOperator *query;
  char *error;
  size_t n_cumu_rows;
} LoadCommandContext;

/**
 * Processing status codes of the server.
 */
typedef enum ServerProcessCode {
  // Processing is successful.
  SERVER_PROCESS_CODE_OK,
  // Processing is successful and a server shutdown is requested.
  SERVER_PROCESS_CODE_OK_TERMINATE_SHUTDOWN,
  // Processing failed but the processing loop should continue.
  SERVER_PROCESS_CODE_ERROR_NONBREAKING,
  // Processing failed because of failure to send message header.
  SERVER_PROCESS_CODE_ERROR_SEND_HEADER,
  // Processing failed because of failure to send message payload.
  SERVER_PROCESS_CODE_ERROR_SEND_PAYLOAD,
  // Processing failed because of failure to receive message payload.
  SERVER_PROCESS_CODE_ERROR_RECV_PAYLOAD,
} ServerProcessCode;

/**
 * Process a MESSAGE_STATUS_C_REQUEST_PROCESS_COMMAND message.
 *
 * This is the ordinary routine to process a query from the client. The client
 * sends a query string to the server, which is then parsed into a DbOperator
 * query. The query is then executed and the result is sent back to the client.
 */
ServerProcessCode mproc_request_process_command(Message *recv_message,
                                                ClientContext *client_context,
                                                BatchContext *batch_context,
                                                int client_socket) {
  char recv_buffer[recv_message->length + 1];

  // Receive the second message from the client that contains the payload
  int length = recv_all(client_socket, recv_buffer, recv_message->length);
  if (length < 0) {
    return SERVER_PROCESS_CODE_ERROR_RECV_PAYLOAD;
  }
  recv_message->payload = recv_buffer;
  recv_message->payload[recv_message->length] = '\0';

  // Initialize the send message
  Message send_message;
  memset(&send_message, 0, sizeof(Message));
  send_message.status = MESSAGE_STATUS_OK;
  send_message.length = 0;
  send_message.payload = NULL;
  send_message.is_malloced = false;

  if (strncmp(recv_message->payload, "shutdown", 8) == 0) {
    // Process the special shutdown command
    return SERVER_PROCESS_CODE_OK_TERMINATE_SHUTDOWN;
  } else if (strncmp(recv_message->payload, "single_core()", 13) == 0) {
    // Switch to single-core mode if not already in single-core mode, otherwise
    // this is an error
    if (!__multi_threaded__) {
      send_message.status = MESSAGE_STATUS_EXECUTION_ERROR;
      send_message.payload = "Already in single-core mode.";
      send_message.length = strlen(send_message.payload);
    } else {
      __multi_threaded__ = false;
    }
  } else if (strncmp(recv_message->payload, "single_core_execute()", 21) == 0) {
    // Switch to multi-threaded mode if not already in multi-threaded mode,
    if (__multi_threaded__) {
      send_message.status = MESSAGE_STATUS_EXECUTION_ERROR;
      send_message.payload = "Not in single-core mode.";
      send_message.length = strlen(send_message.payload);
    } else {
      __multi_threaded__ = true;
    }
  } else {
    // Ordinary query processing routine: convert the query string into a
    // request for a database operator and execute the operator; in this process
    // the message will be updated
    DbOperator *query =
        parse_command(recv_message->payload, &send_message, client_socket,
                      client_context, batch_context);
    execute_db_operator(query, &send_message);
  }

  // Send a first message that contains the header to the client
  if (send_all(client_socket, &send_message, sizeof(Message)) == -1) {
    return SERVER_PROCESS_CODE_ERROR_SEND_HEADER;
  }

  // We do not send a second message if the payload is empty
  if (send_message.length == 0) {
    if (send_message.is_malloced) {
      free(send_message.payload);
    }
    return SERVER_PROCESS_CODE_OK;
  }

  // Send a second message that contains the actual payload to the client; note
  // that the payload buffer here is dynamically allocated because it could be
  // very large (e.g., tabular printout of a large table) that exceeds the stack
  // size
  char *result = malloc(send_message.length); // XXX: Not null-terminated
  strncpy(result, send_message.payload, send_message.length);
  if (send_message.is_malloced) {
    free(send_message.payload); // Free malloced payload after copied
  }
  if (send_all(client_socket, result, send_message.length) == -1) {
    free(result);
    return SERVER_PROCESS_CODE_ERROR_SEND_PAYLOAD;
  }
  free(result);
  return SERVER_PROCESS_CODE_OK;
}

/**
 * Process a MESSAGE_STATUS_C_SENDING_CSV_N_COLS message.
 *
 * This is the first step of the load command. The client sends the number of
 * columns in the CSV file to the server. The server then initializes the load
 * query with the number of columns.
 */
ServerProcessCode mproc_sending_csv_n_cols(Message *recv_message,
                                           LoadCommandContext *load_context,
                                           ClientContext *client_context,
                                           int client_socket) {
  assert(load_context->query == NULL && "Previous load is not terminated");
  log_file(stdout, "QUERY: `load(...)` [preparsed-by-client]\n");

  // Receive the second message from the client that contains the payload, which
  // is the number of columns in the CSV file
  char recv_buffer[recv_message->length + 1];
  int length = recv_all(client_socket, recv_buffer, recv_message->length);
  if (length < 0) {
    return SERVER_PROCESS_CODE_ERROR_RECV_PAYLOAD;
  }
  size_t n_cols;
  memcpy(&n_cols, recv_buffer, sizeof(size_t));

  // Initialize the load query
  load_context->query = malloc(sizeof(DbOperator));
  load_context->query->type = OPERATOR_TYPE_LOAD;
  load_context->query->client_fd = client_socket;
  load_context->query->context = client_context;
  load_context->query->fields.load.n_cols = n_cols;
  load_context->error = NULL;
  load_context->n_cumu_rows = 0;
  return SERVER_PROCESS_CODE_OK;
}

/**
 * Process a MESSAGE_STATUS_C_SENDING_CSV_HEADER message.
 *
 * This is the second step of the load command. The client sends the header of
 * the CSV file to the server. The server then validates the header and updates
 * the load query with the table and columns.
 */
ServerProcessCode mproc_sending_csv_header(Message *recv_message,
                                           LoadCommandContext *load_context,
                                           int client_socket) {
  assert(load_context->query != NULL && "Load is not started");

  // Receive the second message from the client that contains the payload, which
  // is the header string (i.e., first row) of the CSV file
  char recv_buffer[recv_message->length + 1];
  int length = recv_all(client_socket, recv_buffer, recv_message->length);
  if (length < 0) {
    return SERVER_PROCESS_CODE_ERROR_RECV_PAYLOAD;
  }
  recv_buffer[recv_message->length] = '\0';

  // Parse the header string to validate the header
  size_t n_cols = load_context->query->fields.load.n_cols;
  Table *table;
  DbSchemaStatus status = cmdload_validate_header(recv_buffer, n_cols, &table);
  if (status != DB_SCHEMA_STATUS_OK) {
    load_context->error = format_status(status);
    return SERVER_PROCESS_CODE_ERROR_NONBREAKING;
  }

  // Update load query information
  load_context->query->fields.load.table = table;
  load_context->query->fields.load.n_rows = 0; // Initialize
  return SERVER_PROCESS_CODE_OK;
}

/**
 * Process a MESSAGE_STATUS_C_SENDING_CSV_ROWS message.
 *
 * This is the third step of the load command. The client sends a batch of rows
 * from the CSV file to the server. The server then updates the load query with
 * the data and number of rows. This step can happen multiple times until all
 * rows are sent, and this routine is called for each batch of rows.
 */
ServerProcessCode mproc_sending_csv_rows(Message *recv_message,
                                         LoadCommandContext *load_context,
                                         int client_socket) {
  assert(load_context->query != NULL && "Load is not started");

  // Receive the second message from the client that contains the payload, which
  // is a batch of rows from the CSV file
  char *recv_buffer = malloc(recv_message->length * sizeof(char));
  int length = recv_all(client_socket, recv_buffer, recv_message->length);
  if (length < 0) {
    return SERVER_PROCESS_CODE_ERROR_RECV_PAYLOAD;
  }
  int *data = malloc(recv_message->length);
  if (data == NULL) {
    load_context->error = "Failed to allocate internal memory.";
    return SERVER_PROCESS_CODE_ERROR_NONBREAKING;
  }
  memcpy(data, recv_buffer, recv_message->length);
  free(recv_buffer);

  // Update load query information
  load_context->query->fields.load.data = data;
  load_context->query->fields.load.n_rows =
      recv_message->length /
      (load_context->query->fields.load.n_cols * sizeof(int));
  load_context->n_cumu_rows += load_context->query->fields.load.n_rows;

  // Initialize the send message
  Message send_message;
  memset(&send_message, 0, sizeof(Message));
  send_message.status = MESSAGE_STATUS_OK;
  send_message.length = 0;
  send_message.payload = NULL;
  send_message.is_malloced = false;

  // Execute the load query
  execute_db_operator(load_context->query, &send_message);
  if (send_message.status != MESSAGE_STATUS_OK) {
    free(data);
    // The payloads for the errors that may happen in executing a load query are
    // always non-malloc'ed so it is safe to put them directly here; XXX: if
    // this changes, then either we need to keep track of whether the error is
    // malloc'ed, or we need to make all errors malloc'ed
    load_context->error = send_message.payload;
    return SERVER_PROCESS_CODE_ERROR_NONBREAKING;
  }
  free(data);
  return SERVER_PROCESS_CODE_OK;
}

/**
 * Process a MESSAGE_STATUS_C_SENDING_CSV_FINISHED message.
 *
 * This is the final step of the load command. The client sends a message to
 * indicate that all rows from the CSV file have been sent to the server. The
 * server then finalizes the load query and sends a message back to the client
 * to indicate the success or failure of the load command.
 */
ServerProcessCode mproc_sending_csv_finished(LoadCommandContext *load_context,
                                             int client_socket) {
  assert(load_context->query != NULL && "Load is not started");

  // Conclude the load query
  DbSchemaStatus conclude_status = cmdload_conclude(
      load_context->query->fields.load.table, load_context->n_cumu_rows);
  if (conclude_status != DB_SCHEMA_STATUS_OK) {
    load_context->error = format_status(conclude_status);
  }

  // We will not receive a second message because there is no payload in this
  // case; we can now free the load query and reset it to NULL
  free(load_context->query);
  load_context->query = NULL;

  // Initialize the send message
  Message send_message;
  memset(&send_message, 0, sizeof(Message));

  // If the load query was successful, send a message confirming the success and
  // it does not need to further send its payload
  if (load_context->error == NULL) {
    send_message.status = MESSAGE_STATUS_OK;
    send_message.length = 0;
    send_message.payload = NULL;
    send_message.is_malloced = false;
    if (send_all(client_socket, &send_message, sizeof(Message)) == -1) {
      return SERVER_PROCESS_CODE_ERROR_SEND_HEADER;
    }
    return SERVER_PROCESS_CODE_OK;
  }

  // The load query is not successful, report the failure to the client
  send_message.status = MESSAGE_STATUS_EXECUTION_ERROR;
  send_message.payload = load_context->error;
  send_message.length = strlen(send_message.payload);
  send_message.is_malloced = false;
  if (send_all(client_socket, &send_message, sizeof(Message)) == -1) {
    return SERVER_PROCESS_CODE_ERROR_SEND_HEADER;
  }
  if (send_all(client_socket, send_message.payload, send_message.length) ==
      -1) {
    return SERVER_PROCESS_CODE_ERROR_SEND_PAYLOAD;
  }
  return SERVER_PROCESS_CODE_OK;
}

/**
 * Listen to messages from client continually and execute queries.
 *
 * This function returns whether a shutdown is requested while handling the
 * current client connection.
 */
bool handle_client(int client_socket) {
  printf_info("Established connection with client socket %d.\n", client_socket);

  bool shutdown_requested = false;
  ClientContext *client_context = init_client_context();
  LoadCommandContext load_context = {.query = NULL};
  BatchContext batch_context;
  reset_batch_context(&batch_context);

  Message recv_message;
  while (true) {
    // Receive a first message that contains the header
    int length = recv_all(client_socket, &recv_message, sizeof(Message));
    if (length < 0) {
      printf_error("Failed to receive header from client at socket %d.\n",
                   client_socket);
      shutdown_requested = true; // FATAL
      break;
    } else if (length == 0) {
      break; // No more message to receive from client, elegantly exit loop
    }

    // Process based on the status of the received message
    ServerProcessCode sp_code;
    switch (recv_message.status) {
    case MESSAGE_STATUS_C_REQUEST_PROCESS_COMMAND:
      sp_code = mproc_request_process_command(&recv_message, client_context,
                                              &batch_context, client_socket);
      break;
    case MESSAGE_STATUS_C_SENDING_CSV_N_COLS:
      sp_code = mproc_sending_csv_n_cols(&recv_message, &load_context,
                                         client_context, client_socket);
      break;
    case MESSAGE_STATUS_C_SENDING_CSV_HEADER:
      sp_code =
          mproc_sending_csv_header(&recv_message, &load_context, client_socket);
      break;
    case MESSAGE_STATUS_C_SENDING_CSV_ROWS:
      sp_code =
          mproc_sending_csv_rows(&recv_message, &load_context, client_socket);
      break;
    case MESSAGE_STATUS_C_SENDING_CSV_FINISHED:
      sp_code = mproc_sending_csv_finished(&load_context, client_socket);
      break;
    default:
      assert(0 && "Unreachable code.");
    }

    // Handle the server process code
    switch (sp_code) {
    case SERVER_PROCESS_CODE_OK:
    case SERVER_PROCESS_CODE_ERROR_NONBREAKING:
      // Both cases we do not terminate the server processing loop; moreover,
      // non-breaking errors should be handled in their respective processing
      // routines and not here
      break;
    case SERVER_PROCESS_CODE_OK_TERMINATE_SHUTDOWN:
      shutdown_requested = true;
      break;
    case SERVER_PROCESS_CODE_ERROR_SEND_HEADER:
      printf_error("Failed to send header to client at socket %d.\n",
                   client_socket);
      shutdown_requested = true; // FATAL
      break;
    case SERVER_PROCESS_CODE_ERROR_SEND_PAYLOAD:
      printf_error("Failed to send payload to client at socket %d.\n",
                   client_socket);
      shutdown_requested = true; // FATAL
      break;
    case SERVER_PROCESS_CODE_ERROR_RECV_PAYLOAD:
      printf_error("Failed to receive payload from client at socket %d.\n",
                   client_socket);
      shutdown_requested = true; // FATAL
      break;
    }

    if (shutdown_requested) {
      break;
    }
  }

  printf_info("Client connection closed at socket %d.\n", client_socket);
  free_client_context(client_context);
  close(client_socket);
  return shutdown_requested;
}

/**
 * The worker function for the thread pool.
 *
 * This function continuously consumes tasks from the task queue and executes
 * them. The worker thread will terminate when it receives a termination task.
 */
void *thread_worker(void *arg) {
  (void)arg; // XXX: Unused but needed for the function signature

  DbSchemaStatus status;
  while (true) {
    // Dequeue a task from the queue; this is blocking until there is a task to
    // dequeue or we are done (i.e., THREAD_TASK_TYPE_TERMINATE)
    ThreadTask task = thread_pool_dequeue_task(__thread_pool__);
    if (task.type == THREAD_TASK_TYPE_TERMINATE) {
      break;
    }

    // Call subroutines to execute the shared scan task based on the task type
    switch (task.type) {
    case THREAD_TASK_TYPE_SHARED_SCAN:
      shared_scan_subroutine(task.data);
      free(task.data);
      break;
    case THREAD_TASK_TYPE_HASH_JOIN:
      status = hash_join_subroutine(task.data);
      if (status != DB_SCHEMA_STATUS_OK) {
        printf_error("Failed to execute hash join task %d: %s\n", task.id,
                     format_status(status));
      }
      break;
    default: // Including THREAD_TASK_TYPE_TERMINATE, which should have been
             // handled above
      assert(0 && "Unreachable code.");
    }
    log_file(stdout, "  [%lu] Finished task %d\n", pthread_self(), task.id);

    // Mark the task as completed
    thread_pool_mark_task_completion(__thread_pool__);
  }

  log_file(stdout, "  [%lu] Thread exiting\n", pthread_self());
  return NULL;
}

/**
 * Set up the connection on the server side using Unix sockets.
 *
 * This function returns a valid server socket file descriptor on success,
 * otherwise -1 on failure.
 */
int setup_server() {
  printf_info("Setting up the server...\n");

  // Create a new socket
  int server_socket;
  if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    printf_error("Failed to create socket.\n");
    return -1;
  }

  // Set up a UNIX domain socket address structure with AF_UNIX family and
  // unlink any existing socket file at the specified path
  struct sockaddr_un local;
  local.sun_family = AF_UNIX;
  strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
  unlink(local.sun_path);

  // Bind the socket to the address
  size_t len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
  if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
    printf_error("Failed to bind to socket.\n");
    return -1;
  }

  // Listen for incoming connections
  if (listen(server_socket, 5) == -1) {
    printf_error("Failed to listen on socket.\n");
    return -1;
  }

  return server_socket;
}

int main(int argc, char *argv[]) {
  init_sysinfo();
  printf("System information:\n");
  printf("  __n_processors__  %d\n", __n_processors__);
  printf("  __page_size__     %d\n", __page_size__);
  printf("  __avg_load_1__    %.2f\n", __avg_load_1__);
  printf("  __avg_load_5__    %.2f\n", __avg_load_5__);
  printf("  __avg_load_15__   %.2f\n", __avg_load_15__);

  // By default, the number of workers is the number of processors, minus a
  // weighted average of system loads over the past few minutes
  int n_jobs = __n_processors__ - 1 - // One for the main thread
               0.7 * __avg_load_1__ - // Highest weight for 1-minute
               0.2 * __avg_load_5__ - 0.1 * __avg_load_15__;
  if (n_jobs < __n_processors__ / 4)
    n_jobs = __n_processors__ / 4; // At minimum use 1/4 of all processors
  if (n_jobs < 1)
    n_jobs = 1;

  // Parse the command line arguments
  int opt;
  while ((opt = getopt(argc, argv, "j:")) != -1) {
    switch (opt) {
    case 'j':
      n_jobs = atoi(optarg);
      if (n_jobs < 0 || n_jobs > __n_processors__ - 1) {
        printf_error("Invalid number of workers: %d\n; must be between [0, %d]",
                     n_jobs, __n_processors__ - 1);
        return 1;
      }
      break;
    default:
      printf_error("Usage: %s [-j jobs]\n", argv[0]);
      return 1;
    }
  }

  // Launch the system
  if (system_launch() < 0) {
    printf_error("System failed to launch.\n");
    return 1;
  }
  printf_info("System successfully launched.\n");

  // Set up the thread pool
  if (n_jobs > 0) {
    __thread_pool__ = malloc(sizeof(ThreadPool));
    if (__thread_pool__ == NULL) {
      printf_error("Failed to set up the thread pool.\n");
      return 1;
    }
    thread_pool_init(__thread_pool__, n_jobs, thread_worker);
    printf_info("Thread pool successfully set up with %d workers.\n", n_jobs);
  }

  // Set up the server
  int server_socket = setup_server();
  if (server_socket < 0) {
    return 1;
  }
  printf_info("Waiting for client connection at socket %d...\n", server_socket);

  // Prepare the remote socket
  struct sockaddr_un remote;
  socklen_t t = sizeof(remote);
  int client_socket = 0;

  // Wait for incoming client connections and handle them
  while (true) {
    if ((client_socket =
             accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
      printf_error("Failed to accept a new connection.\n");
      return 1;
    }

    // TODO: Handle multiple concurrent clients
    // - Maximum number of concurrent client connections to allow?
    // - What aspects of siloes or isolation are maintained in the design?
    if (handle_client(client_socket)) {
      break; // Shutdown requested
    }
  }

  close(server_socket);

  // Clean up the thread pool
  if (n_jobs > 0) {
    thread_pool_shutdown(__thread_pool__);
    free(__thread_pool__);
  }

  // Shutdown the system
  if (system_shutdown() < 0) {
    printf_error("System failed to shutdown (exiting forcefully).\n");
    return 1;
  }
  printf_info("System successfully shut down (gracefully).\n");
  return 0;
}
