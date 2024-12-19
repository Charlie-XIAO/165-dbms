/**
 * @file thread_pool.h
 *
 * This header defines the thread pool and task queue structures and operations
 * for multi-threaded execution. It also provides global variables for thread
 * pools and task queues used in the system so that they can be accessed from
 * different parts of the system.
 */

#ifndef THREAD_POOL_H__
#define THREAD_POOL_H__

#include <pthread.h>
#include <stdbool.h>

#include "consts.h"

/**
 * The type of a thread task.
 *
 * Specially, `THREAD_TASK_TYPE_TERMINATE` is used to signal the worker threads
 * to terminate.
 */
typedef enum ThreadTaskType {
  THREAD_TASK_TYPE_TERMINATE,
  THREAD_TASK_TYPE_SHARED_SCAN,
  THREAD_TASK_TYPE_HASH_JOIN,
} ThreadTaskType;

/**
 * A thread task structure.
 *
 * This is a thin wrapper around the actual task data that is to be processed by
 * the worker threads (which can essentially be any data type as long as it can
 * be handled by the corresponding worker function). Task type indicates how the
 * task data is meant to be handled. The ID servers as a unique identifier that
 * is mainly for debugging and logging purposes.
 */
typedef struct ThreadTask {
  int id;
  ThreadTaskType type;
  void *data;
} ThreadTask;

/**
 * A thread task queue structure.
 *
 * This structure contains a circular buffer of thread tasks, which is used to
 * store tasks for the worker threads to consume. The queue is thread-safe and
 * supports blocking operations for both enqueue and dequeue operations. The
 * mutex is used to protect the queue from concurrent access. The mutexes
 * respectively signal when the queue is not empty, the queue is not full, and
 * some task is completed.
 */
typedef struct ThreadTaskQueue {
  ThreadTask tasks[THREAD_TASK_QUEUE_SIZE];
  int front;
  int rear;
  int count;
  int n_completed;
  pthread_mutex_t mutex;
  pthread_cond_t cond_non_empty;
  pthread_cond_t cond_non_full;
  pthread_cond_t cond_completed;
} ThreadTaskQueue;

/**
 * A thread pool structure.
 *
 * This structure contains the task queue, the worker threads, and the number of
 * workers in the thread pool. It also contains a flag indicating whether the
 * thread pool has initialized a shutdown, which is useful when signaling
 * workers to exit.
 */
typedef struct ThreadPool {
  ThreadTaskQueue queue;
  pthread_t *workers;
  int n_workers;
  bool shutdown_inited;
} ThreadPool;

/**
 * Generate a next task ID.
 *
 * This function is essentially a counter that increments every time being
 * called, going from 0 to INT_MAX and then wrapping around to 0 again. It
 * always returns a positive integer.
 */
int next_task_id();

/**
 * Initialize a thread pool.
 *
 * This function initializes a thread pool with the specified number of worker
 * threads and a task queue.
 */
void thread_pool_init(ThreadPool *pool, int n_workers,
                      void *(*worker_func)(void *));

/**
 * Shutdown a thread pool.
 *
 * This function will mark the thread pool as shutdown initialized and signal
 * all worker threads. It will then join all worker threads and destroy the task
 * queue.
 */
void thread_pool_shutdown(ThreadPool *pool);

/**
 * Enqueue a task into the task queue of a thread pool.
 *
 * This function enqueues a task into the task queue. If the queue is full, it
 * will block until a consumer dequeues a task.
 */
void thread_pool_enqueue_task(ThreadPool *pool, ThreadTask *task);

/**
 * Dequeue a task from the task queue of a thread pool.
 *
 * This function dequeues a task from the task queue. If the queue is empty, it
 * will block until a producer enqueues a task. If the thread pool has
 * initialized a shutdown, it will return a termination task.
 */
ThreadTask thread_pool_dequeue_task(ThreadPool *pool);

/**
 * Reset the completion status of a thread task queue.
 *
 * This essentially resets the number of completed tasks in the task queue to
 * zero, which is useful when we need to wait for a certain number of tasks to
 * complete.
 */
void thread_pool_reset_queue_completion(ThreadPool *pool);

/**
 * Mark a single task completion in a thread task queue.
 *
 * This function increments the number of completed tasks in the task queue and
 * signals that a task is completed, so that the checker thread can be waked up.
 */
void thread_pool_mark_task_completion(ThreadPool *pool);

/**
 * Wait for a certain number of tasks to complete in a thread task queue.
 *
 * This function will block until the number of completed tasks in the task
 * queue reaches the specified number of tasks. Note that this function must be
 * called after a completion status reset, and that no irrelevant tasks should
 * be enqueued after the reset.
 */
void thread_pool_wait_queue_completion(ThreadPool *pool, int n_tasks);

/**
 * A global flag indicating whether the system is in multi-threaded mode.
 *
 * This flag is turned on by default, and can be toggled by the client with the
 * `single_core()` and `single_core_execute()` commands. The system will run
 * operations in multi-threaded mode when possible if this flag is on.
 *
 * Note that this flag has nothing to do with whether a specific thread pool or
 * task queue is initialized or not. In particular, if we are in multi-threaded
 * mode and the thread pool or task queue is not initialized, it is an error.
 */
extern bool __multi_threaded__;

/**
 * The global thread pool.
 */
extern ThreadPool *__thread_pool__;

#endif /* THREAD_POOL_H__ */
