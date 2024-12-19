/**
 * @file comm.c
 * @implements comm.h
 */

#include <sys/socket.h>

#include "comm.h"
#include "consts.h"

/**
 * @implements recv_all
 */
ssize_t recv_all(int socket, void *buf, size_t length) {
  size_t total_received = 0;

  // Receive data in chunks, with the last chunk possibly being smaller
  ssize_t current_received;
  while (total_received < length) {
    size_t remaining = length - total_received;
    size_t to_read = remaining > DEFAULT_SOCKET_BUFFER_SIZE
                         ? DEFAULT_SOCKET_BUFFER_SIZE
                         : remaining;
    current_received = recv(socket, (char *)buf + total_received, to_read, 0);
    if (current_received <= 0) {
      return current_received; // Either error or connection closed
    }
    total_received += current_received;
  }

  return total_received;
}

/**
 * @implements send_all
 */
ssize_t send_all(int socket, void *buf, size_t length) {
  size_t total_sent = 0;

  // Send data in chunks, with the last chunk possibly being smaller
  ssize_t current_sent;
  while (total_sent < length) {
    size_t remaining = length - total_sent;
    size_t to_send = remaining > DEFAULT_SOCKET_BUFFER_SIZE
                         ? DEFAULT_SOCKET_BUFFER_SIZE
                         : remaining;
    current_sent = send(socket, (char *)buf + total_sent, to_send, 0);
    if (current_sent < 0) {
      return current_sent; // Error
    }
    total_sent += current_sent;
  }

  return total_sent;
}
