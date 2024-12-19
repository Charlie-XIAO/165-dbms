/**
 * @file message.h
 *
 * This header defines the message interface used for communication between
 * client and server.
 */

#ifndef MESSAGE_H__
#define MESSAGE_H__

#include <stdbool.h>

/**
 * A message status that indicates the status of the previous request.
 */
typedef enum MessageStatus {
  /**
   * Server successfully processed the client request.
   *
   * The corresponding payload should be the execution result of the request
   * that needs to be displayed on the client side, if any. If there is nothing
   * to display, the payload should be left empty.
   */
  MESSAGE_STATUS_OK,
  /**
   * Server received an invalid command from the client.
   *
   * The corresponding payload is empty and should be ignored.
   */
  MESSAGE_STATUS_INVALID_COMMAND,
  /**
   * Server received a valid command but cannot parse into a valid DbOperator.
   *
   * The corresponding payload is the error message.
   */
  MESSAGE_STATUS_PARSE_ERROR,
  /**
   * Server failed to batch the command as requested.
   *
   * The corresponding payload is the error message.
   */
  MESSAGE_STATUS_BATCH_ERROR,
  /**
   * Server failed to execute the client request.
   *
   * In this case, though the command is parsed into a valid DbOperator, the
   * server failed to execute that DbOperator. The corresponding payload is the
   * error message.
   */
  MESSAGE_STATUS_EXECUTION_ERROR,
  /**
   * Server failed to execute the client request and the reason is unknown.
   *
   * In this case, though the command is parsed into a valid DbOperator, the
   * server failed to execute that DbOperator and the reason is unknown. The
   * corresponding payload is empty and should be ignored.
   */
  MESSAGE_STATUS_UNKNOWN_EXECUTION_ERROR,
  /**
   * Client requested the server to process the command.
   *
   * The corresponding payload is the command to be processed.
   */
  MESSAGE_STATUS_C_REQUEST_PROCESS_COMMAND,
  /**
   * Client sent the number of columns in the CSV to the server.
   *
   * The corresponding payload is the number of columns (size_t).
   */
  MESSAGE_STATUS_C_SENDING_CSV_N_COLS,
  /**
   * Client sent the CSV header to the server.
   *
   * The corresponding payload is the string of the header row in the loaded
   * CSV file.
   */
  MESSAGE_STATUS_C_SENDING_CSV_HEADER,
  /**
   * Client sent a batch of CSV rows to the server.
   *
   * The corresponding payload is a 2D array of integers flattened into 1D, with
   * the first dimension corresponding to the number of rows in the batch,
   * computed as `length / (n_cols * sizeof(int))`, and the second dimension
   * corresponding to the number of columns in the CSV that should have been
   * sent in a previous MESSAGE_STATUS_C_SENDING_CSV_N_COLS message.
   */
  MESSAGE_STATUS_C_SENDING_CSV_ROWS,
  /**
   * Client finished sending the CSV file to the server.
   *
   * The corresponding payload is empty.
   */
  MESSAGE_STATUS_C_SENDING_CSV_FINISHED,
} MessageStatus;

/**
 * A single message to be sent between client and server.
 *
 * This struct contains the status of the message, the length of the payload,
 * the payload itself, and a flag indicating whether the payload is malloc'ed.
 * Note that the flag is not telling the receiver to free the payload - the
 * payload is always copied to a buffer and sent over network to the receiver;
 * it is telling the sender whether it should be freed after copying to the
 * buffer.
 */
typedef struct Message {
  MessageStatus status;
  int length;
  char *payload;
  bool is_malloced;
} Message;

#endif /* MESSAGE_H__ */
