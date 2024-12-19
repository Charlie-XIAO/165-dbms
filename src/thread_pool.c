/**
 * @file thread_pool.c
 * @implements thread_pool.h
 */

#include <limits.h>
#include <stdlib.h>

#include "thread_pool.h"

static int global_task_id = 0;

/**
 * @implements next_task_id
 */
int next_task_id() {
  global_task_id = (global_task_id + 1) % INT_MAX;
  return global_task_id;
}

/**
 * @implements thread_pool_init
 */
void thread_pool_init(ThreadPool *pool, int n_workers,
                      void *(*worker_func)(void *)) {
  // Initialize the task queue
  pool->queue.front = 0;
  pool->queue.rear = -1;
  pool->queue.count = 0;
  pool->queue.n_completed = 0;
  pthread_mutex_init(&pool->queue.mutex, NULL);
  pthread_cond_init(&pool->queue.cond_non_empty, NULL);
  pthread_cond_init(&pool->queue.cond_non_full, NULL);
  pthread_cond_init(&pool->queue.cond_completed, NULL);

  // Initialize the thread pool
  pool->shutdown_inited = false;
  pool->n_workers = n_workers;
  pool->workers = malloc(n_workers * sizeof(pthread_t));
  for (int i = 0; i < n_workers; i++) {
    pthread_create(&pool->workers[i], NULL, worker_func, NULL);
  }
}

/**
 * @implements thread_pool_shutdown
 */
void thread_pool_shutdown(ThreadPool *pool) {
  // Wake up all workers; otherwise workers may be waiting for being signaled
  // because of an empty task queue at some point, not noticing that the thread
  // pool is shutting down
  pthread_mutex_lock(&pool->queue.mutex);
  pool->shutdown_inited = true;
  pthread_cond_broadcast(&pool->queue.cond_non_empty);
  pthread_mutex_unlock(&pool->queue.mutex);

  for (int i = 0; i < pool->n_workers; i++) {
    pthread_join(pool->workers[i], NULL);
  }
  free(pool->workers);

  // Destroy the task queue
  pthread_mutex_destroy(&pool->queue.mutex);
  pthread_cond_destroy(&pool->queue.cond_non_empty);
  pthread_cond_destroy(&pool->queue.cond_non_full);
  pthread_cond_destroy(&pool->queue.cond_completed);
}

/**
 * @implements thread_pool_enqueue_task
 */
void thread_pool_enqueue_task(ThreadPool *pool, ThreadTask *task) {
  pthread_mutex_lock(&pool->queue.mutex);

  // The task queue is full, wait for a consumer to dequeue a task
  while (pool->queue.count >= THREAD_TASK_QUEUE_SIZE) {
    pthread_cond_wait(&pool->queue.cond_non_full, &pool->queue.mutex);
  }

  // Enqueue a task
  pool->queue.rear = (pool->queue.rear + 1) % THREAD_TASK_QUEUE_SIZE;
  pool->queue.tasks[pool->queue.rear] = *task;
  pool->queue.count++;

  // Signal that the task queue is not empty because we have just added a task
  pthread_cond_signal(&pool->queue.cond_non_empty);
  pthread_mutex_unlock(&pool->queue.mutex);
}

/**
 * @implements thread_pool_dequeue_task
 */
ThreadTask thread_pool_dequeue_task(ThreadPool *pool) {
  pthread_mutex_lock(&pool->queue.mutex);

  // If the thread pool has initialized a shutdown, we should no longer consume
  // any tasks from the queue
  if (pool->shutdown_inited) {
    pthread_mutex_unlock(&pool->queue.mutex);
    return (ThreadTask){
        .id = next_task_id(), .type = THREAD_TASK_TYPE_TERMINATE, .data = NULL};
  }

  // The task queue is empty, wait for a producer to enqueue a task; note that
  // the thread pool could have initialized a shutdown while we are waiting,
  // so we need to re-check the shutdown flag every time we are waked up
  while (pool->queue.count <= 0 && !pool->shutdown_inited) {
    pthread_cond_wait(&pool->queue.cond_non_empty, &pool->queue.mutex);
  }

  // Re-check the shutdown flag after being waked up
  if (pool->shutdown_inited) {
    pthread_mutex_unlock(&pool->queue.mutex);
    return (ThreadTask){
        .id = next_task_id(), .type = THREAD_TASK_TYPE_TERMINATE, .data = NULL};
  }

  // Dequeue a task
  ThreadTask task = pool->queue.tasks[pool->queue.front];
  pool->queue.front = (pool->queue.front + 1) % THREAD_TASK_QUEUE_SIZE;
  pool->queue.count--;

  // Signal that the task queue is not full because we have just removed a task
  pthread_cond_signal(&pool->queue.cond_non_full);
  pthread_mutex_unlock(&pool->queue.mutex);

  return task;
}

/**
 * @implements thread_pool_reset_queue_completion
 */
void thread_pool_reset_queue_completion(ThreadPool *pool) {
  pthread_mutex_lock(&pool->queue.mutex);
  pool->queue.n_completed = 0;
  pthread_mutex_unlock(&pool->queue.mutex);
}

/**
 * @implements thread_pool_mark_task_completion
 */
void thread_pool_mark_task_completion(ThreadPool *pool) {
  pthread_mutex_lock(&pool->queue.mutex);
  pool->queue.n_completed++;
  pthread_cond_signal(&pool->queue.cond_completed);
  pthread_mutex_unlock(&pool->queue.mutex);
}

/**
 * @implements thread_pool_wait_queue_completion
 */
void thread_pool_wait_queue_completion(ThreadPool *pool, int n_tasks) {
  pthread_mutex_lock(&pool->queue.mutex);
  while (pool->queue.n_completed < n_tasks) {
    pthread_cond_wait(&pool->queue.cond_completed, &pool->queue.mutex);
  }
  pthread_mutex_unlock(&pool->queue.mutex);
}

// Initialize global variables

bool __multi_threaded__ = true;
ThreadPool *__thread_pool__ = NULL;
