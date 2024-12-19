/**
 * @file client.c
 *
 * This file provides a basic Unix socket implementation for a client used in an
 * interactive client-server database.
 *
 * For more information on Unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 */

// This line at the top is necessary for compilation on the lab machine and many
// other Unix machines. Please look up _XOPEN_SOURCE for more details.
#define _XOPEN_SOURCE

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "comm.h"
#include "consts.h"
#include "io.h"
#include "logging.h"
#include "message.h"

/**
 * Processing status codes of the client.
 */
typedef enum ClientProcessCode {
  // Processing is successful.
  CLIENT_PROCESS_CODE_OK,
  // Processing is successful, and the processing loop should terminate.
  CLIENT_PROCESS_CODE_OK_TERMINATE,
  // Processing failed but the processing loop should continue.
  CLIENT_PROCESS_CODE_ERROR_NONBREAKING,
  // Processing failed because of failure to send message header.
  CLIENT_PROCESS_CODE_ERROR_SEND_HEADER,
  // Processing failed because of failure to send message payload.
  CLIENT_PROCESS_CODE_ERROR_SEND_PAYLOAD,
  // Processing failed because of failure to receive message header.
  CLIENT_PROCESS_CODE_ERROR_RECV_HEADER,
  // Processing failed because of failure to receive message payload.
  CLIENT_PROCESS_CODE_ERROR_RECV_PAYLOAD,
} ClientProcessCode;

/**
 * Connect to the server using a Unix socket.
 *
 * This function returns a valid client socket file descriptor on success, or -1
 * on failure.
 */
int connect_client() {
  int client_socket;
  if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    printf_error("Failed to create socket.\n");
    return -1;
  }

  // Set up a UNIX domain socket address structure
  struct sockaddr_un remote;
  remote.sun_family = AF_UNIX;
  strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
  size_t length = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
  if (connect(client_socket, (struct sockaddr *)&remote, length) == -1) {
    printf_error("Client failed to establish connection.\n");
    return -1;
  }
  return client_socket;
}

/**
 * Process a load query.
 *
 * This is a special routine that processes load queries made by the client, in
 * particular, those starting in `load(`. This is because the loaded files are
 * on the client side, so we cannot wait until the server parses the query.
 * Instead, load queries are parsed on the client side with the steps specified
 * in the comments down below. The procecessing status code is returned.
 */
ClientProcessCode process_load_command(char *command, int client_socket) {
  char *tokenizer, *to_free;
  tokenizer = to_free = malloc((strlen(command + 5) + 1) * sizeof(char));
  strcpy(tokenizer, command + 5);

  // Check that the load command is well-formed, i.e., it has a closing
  // parenthesis, and the file name is within quotes; note the last character
  // may or may not be a newline character
  size_t command_length = strlen(tokenizer);
  if (tokenizer[command_length - 1] == '\n') {
    tokenizer[command_length--] = '\0';
  }
  if (tokenizer[0] != '"' || tokenizer[command_length - 2] != '"' ||
      tokenizer[command_length - 1] != ')') {
    printf_error("Invalid command.\n", tokenizer);
    free(to_free);
    return CLIENT_PROCESS_CODE_ERROR_NONBREAKING;
  }
  tokenizer[command_length - 2] = '\0';
  tokenizer++;

  // Load the CSV file
  CSV *csv = load_csv(tokenizer);
  if (csv == NULL) {
    printf_error("Failed to load CSV file: `%s`.\n", tokenizer);
    free(to_free);
    return CLIENT_PROCESS_CODE_ERROR_NONBREAKING;
  }
  free(to_free);

  Message send_message;
  memset(&send_message, 0, sizeof(Message));

  // Step I: Send the number of columns to the server
  send_message.status = MESSAGE_STATUS_C_SENDING_CSV_N_COLS;
  send_message.length = sizeof(size_t);
  if (send_all(client_socket, &send_message, sizeof(Message)) == -1) {
    close_csv(csv);
    return CLIENT_PROCESS_CODE_ERROR_SEND_HEADER;
  }
  if (send_all(client_socket, &csv->n_cols, sizeof(size_t)) == -1) {
    close_csv(csv);
    return CLIENT_PROCESS_CODE_ERROR_SEND_PAYLOAD;
  }

  // Step II: Send the CSV header string to the server
  send_message.status = MESSAGE_STATUS_C_SENDING_CSV_HEADER;
  send_message.length = strlen(csv->header) + 1;
  if (send_all(client_socket, &send_message, sizeof(Message)) == -1) {
    close_csv(csv);
    return CLIENT_PROCESS_CODE_ERROR_SEND_HEADER;
  }
  if (send_all(client_socket, csv->header, send_message.length) == -1) {
    close_csv(csv);
    return CLIENT_PROCESS_CODE_ERROR_SEND_PAYLOAD;
  }

  // Step III: Parse and send CSV rows (flattened) to the server in batches
  send_message.status = MESSAGE_STATUS_C_SENDING_CSV_ROWS;
  size_t row_stride = csv->n_cols * sizeof(int);
  int batch_size = NUM_ELEMS_PER_LOAD_BATCH / csv->n_cols;
  int csv_rows_buffer[NUM_ELEMS_PER_LOAD_BATCH];
  CSVParseStatus parse_status;
  while (true) {
    size_t current_length = 0;
    for (int i = 0; i < batch_size; i++) {
      parse_status = parse_next_row(csv, csv_rows_buffer + i * csv->n_cols);
      if (parse_status != CSV_PARSE_STATUS_CONTINUE) {
        break; // EOF or error
      }
      current_length += row_stride;
    }

    // This means we have nothing to send in the current batch, either because
    // we have hit EOF or an error occurred; both cases we should break out of
    // the loop and move to next step
    if (current_length == 0) {
      break;
    }

    // Send the current batch of CSV rows
    send_message.length = current_length;
    if (send_all(client_socket, &send_message, sizeof(Message)) == -1) {
      close_csv(csv);
      return CLIENT_PROCESS_CODE_ERROR_SEND_HEADER;
    }
    if (send_all(client_socket, csv_rows_buffer, send_message.length) == -1) {
      close_csv(csv);
      return CLIENT_PROCESS_CODE_ERROR_SEND_PAYLOAD;
    }

    if (parse_status != CSV_PARSE_STATUS_CONTINUE) {
      break; // EOF or error, no more batches will be sent
    }
  }

  // Step IV: Send a final message to indicate end of load; only the header is
  // sent since there is no payload
  send_message.length = 0;
  send_message.status = MESSAGE_STATUS_C_SENDING_CSV_FINISHED;
  close_csv(csv);
  if (send_all(client_socket, &send_message, sizeof(Message)) == -1) {
    return CLIENT_PROCESS_CODE_ERROR_SEND_HEADER;
  }

  // Step V: Receive the feedback from the server
  Message recv_message;
  int length = recv_all(client_socket, &recv_message, sizeof(Message));
  if (length < 0) {
    return CLIENT_PROCESS_CODE_ERROR_RECV_HEADER;
  } else if (length == 0) {
    return CLIENT_PROCESS_CODE_OK_TERMINATE; // Server closed the connection
  }
  if (recv_message.length != 0) {
    // We need to receive a second message that contains the actual payload,
    // and this means the processing on the server side has failed
    char payload[recv_message.length + 1];
    length = recv_all(client_socket, payload, recv_message.length);
    if (length <= 0) {
      return CLIENT_PROCESS_CODE_ERROR_RECV_PAYLOAD;
    }
    payload[recv_message.length] = '\0';
    printf_error("%s\n", payload);
    return CLIENT_PROCESS_CODE_ERROR_NONBREAKING;
  }

  // No payload means the that the processing is successful on the server side
  // and we do not need to receive a second message
  return CLIENT_PROCESS_CODE_OK;
}

/**
 * Process a general client query.
 *
 * This follows the ordinary processing routine. First the client sends the
 * header and the payload to the server, where the payload is the query command
 * read from the input. Next the client receives the feedback from the server
 * and displays message if necessary. Except for a few special cases, all client
 * queries should be processed via this routine. The processing status code is
 * returned.
 */
ClientProcessCode process_command(char *command, int client_socket) {
  // Do not process empty input
  size_t command_length = strlen(command);
  if (command[command_length - 1] == '\n') {
    command[command_length--] = '\0';
  }
  if (command_length == 0) {
    return CLIENT_PROCESS_CODE_OK;
  }

  // Send the message header then the actual payload
  Message send_message;
  memset(&send_message, 0, sizeof(Message));
  send_message.length = command_length;
  send_message.is_malloced = false;
  send_message.payload = command;
  send_message.status = MESSAGE_STATUS_C_REQUEST_PROCESS_COMMAND;
  if (send_all(client_socket, &send_message, sizeof(Message)) == -1) {
    return CLIENT_PROCESS_CODE_ERROR_SEND_HEADER;
  }
  if (send_all(client_socket, send_message.payload, send_message.length) ==
      -1) {
    return CLIENT_PROCESS_CODE_ERROR_SEND_PAYLOAD;
  }

  // Receive a first message from the server that contains the header
  Message recv_message;
  int length = recv_all(client_socket, &recv_message, sizeof(Message));
  if (length < 0) {
    return CLIENT_PROCESS_CODE_ERROR_RECV_HEADER;
  } else if (length == 0) {
    return CLIENT_PROCESS_CODE_OK_TERMINATE; // Server closed the connection
  }

  // We do not receive a second message if the header has specified an empty
  // payload; however, the status of a message with payload length 0 still
  // carries meaning so we print their interpretations
  if (recv_message.length == 0) {
    switch (recv_message.status) {
    case MESSAGE_STATUS_INVALID_COMMAND:
      printf_error("Invalid command.\n");
      break;
    case MESSAGE_STATUS_UNKNOWN_EXECUTION_ERROR:
      printf_error("Unknown error encountered during execution.\n");
      break;
    default:
      break;
    }
    return CLIENT_PROCESS_CODE_OK;
  }

  // The header has specified non-empty payload so we need to receive a second
  // message that contains the actual payload; note that we dynamically allocate
  // this payload buffer because it could be very large (e.g., tabular printout
  // of a large table) that exceeds the stack size
  char *payload = malloc(recv_message.length + 1);
  length = recv_all(client_socket, payload, recv_message.length);
  if (length <= 0) {
    free(payload);
    return CLIENT_PROCESS_CODE_ERROR_RECV_PAYLOAD;
  } else {
    payload[recv_message.length] = '\0';
    switch (recv_message.status) {
    case MESSAGE_STATUS_OK:
      printf("%s\n", payload);
      break;
    default:
      printf_error("%s\n", payload);
      break;
    }
  }
  free(payload);
  return CLIENT_PROCESS_CODE_OK;
}

int main() {
  int client_socket = connect_client();
  if (client_socket < 0) {
    return 1;
  }

  // Output an interactive marker at the start of each command if the input is
  // from a file descriptor associated with a terminal; otherwise do not output
  // the marker, e.g., if the input is piped in from a file
  char *prefix = isatty(fileno(stdin)) ? "\x1b[1;32mclient>\x1b[0m " : "";

  char read_buffer[DEFAULT_BUFFER_SIZE];
  char *read_ptr; // Used the check if read was successful
  while (true) {
    printf("%s", prefix);
    read_ptr = fgets(read_buffer, DEFAULT_BUFFER_SIZE, stdin);
    if (read_ptr == NULL || feof(stdin)) {
      break;
    }

    // Process the command
    ClientProcessCode cp_code;
    if (strncmp(read_buffer, "load(", 5) == 0) {
      cp_code = process_load_command(read_buffer, client_socket);
    } else {
      cp_code = process_command(read_buffer, client_socket);
    }

    // Check the process code and take appropriate action
    switch (cp_code) {
    case CLIENT_PROCESS_CODE_OK:
    case CLIENT_PROCESS_CODE_ERROR_NONBREAKING:
      // Both cases we do not terminate the client processing loop; moreover,
      // non-breaking errors should be handled in their respective processing
      // routines and not here
      break;
    case CLIENT_PROCESS_CODE_OK_TERMINATE:
      close(client_socket);
      return 0;
    case CLIENT_PROCESS_CODE_ERROR_SEND_HEADER:
      printf_error("Failed to send message header.\n");
      close(client_socket);
      return 1;
    case CLIENT_PROCESS_CODE_ERROR_SEND_PAYLOAD:
      printf_error("Failed to send message payload.\n");
      close(client_socket);
      return 1;
    case CLIENT_PROCESS_CODE_ERROR_RECV_HEADER:
      printf_error("Failed to receive message header.\n");
      close(client_socket);
      return 1;
    case CLIENT_PROCESS_CODE_ERROR_RECV_PAYLOAD:
      printf_error("Failed to receive message payload.\n");
      close(client_socket);
      return 1;
    }
  }

  return 0;
}
