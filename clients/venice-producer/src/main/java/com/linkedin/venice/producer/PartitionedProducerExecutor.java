package com.linkedin.venice.producer;

import java.util.concurrent.TimeUnit;


/**
 * Executor abstraction for partition-based parallel processing in Venice Producer.
 *
 * <p>This interface enables partition-based workers that eliminate head-of-line blocking
 * while maintaining per-key ordering. The key insight is that ordering only matters
 * within the same partition (same key maps to same partition), so different partitions
 * can run in parallel.</p>
 *
 * <p>Execution flow:</p>
 * <pre>
 * asyncPut(key) -> partition = hash(key) % numWorkers
 *                          |
 *              Worker[partition % W]
 *                1. preprocess(key,val)  <- Same thread does both
 *                2. veniceWriter.put()   <- Non-blocking
 *                          |
 *              Kafka callback -> complete userFuture
 * </pre>
 *
 * <p>Execution modes (both pools optional):</p>
 * <ul>
 *   <li>Workers=0, Callback=0: Fully inline (preprocess + dispatch on caller thread, callback on Kafka thread)</li>
 *   <li>Workers=0, Callback>0: Inline preprocessing, callback on dedicated threads</li>
 *   <li>Workers>0, Callback=0: Default - parallel workers, callback on Kafka thread</li>
 *   <li>Workers>0, Callback>0: Full async - parallel workers + callback isolation</li>
 * </ul>
 */
public interface PartitionedProducerExecutor {
  /**
   * Submit work for a specific partition.
   * If workers enabled: routes to worker[partition % workerCount].
   * If workers disabled: executes inline on caller thread.
   *
   * @param partition the partition number used for routing to the appropriate worker
   * @param task the work to execute (preprocessing + dispatch)
   */
  void submit(int partition, Runnable task);

  /**
   * Execute callback (for user future completion).
   * If callback executor enabled: hands off to callback pool.
   * If callback executor disabled: runs inline on caller (Kafka) thread.
   *
   * @param callback the callback to execute
   */
  void executeCallback(Runnable callback);

  /**
   * @return whether worker threads are enabled (workerCount > 0)
   */
  boolean isWorkersEnabled();

  /**
   * @return whether callback executor is enabled (callbackThreadCount > 0)
   */
  boolean isCallbackExecutorEnabled();

  /**
   * Get queue depth for specific worker (for metrics).
   *
   * @param workerIndex the worker index
   * @return queue size for the specified worker, or 0 if workers disabled
   */
  int getWorkerQueueSize(int workerIndex);

  /**
   * Get total queue depth across all workers.
   *
   * @return sum of all worker queue depths, or 0 if workers disabled
   */
  int getTotalWorkerQueueSize();

  /**
   * Get callback executor queue depth.
   *
   * @return callback queue size, or 0 if callback executor disabled
   */
  int getCallbackQueueSize();

  /**
   * @return number of workers, or 0 if workers disabled
   */
  int getWorkerCount();

  /**
   * Initiates an orderly shutdown in which previously submitted tasks are executed,
   * but no new tasks will be accepted.
   */
  void shutdown();

  /**
   * Blocks until all tasks have completed execution after a shutdown request,
   * or the timeout occurs, or the current thread is interrupted, whichever happens first.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return true if all executors terminated, false if timeout elapsed
   * @throws InterruptedException if interrupted while waiting
   */
  boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
}
