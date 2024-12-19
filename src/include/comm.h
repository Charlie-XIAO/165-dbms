/**
 * @file comm.h
 *
 * This header defines utilities for socket communication.
 */

#ifndef COMM_H__
#define COMM_H__

#include "sys/types.h"

/**
 * Receive a certain amount of data from a socket.
 *
 * This function will receive data in chunks until the specified amount of data
 * is received. The received data will be stored in the payload buffer. This
 * function returns the number of bytes received if all data are received
 * successfully, or 0 if the connection is closed midway. Otherwise it will
 * return -1 to indicate an error.
 */
ssize_t recv_all(int socket, void *buf, size_t length);

/**
 * Send a certain amount of data to a socket.
 *
 * This function will send data in chunks until the specified amount of data is
 * sent. The data to be sent is stored in the payload buffer. This function
 * returns the number of bytes sent if all data are sent successfully, or -1 to
 * indicate an error.
 */
ssize_t send_all(int socket, void *buf, size_t length);

#endif /* COMM_H__ */
