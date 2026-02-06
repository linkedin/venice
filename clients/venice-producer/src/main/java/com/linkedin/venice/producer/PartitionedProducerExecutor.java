package com.linkedin.venice.producer;

import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Executor for partition-based parallel processing in Venice Producer.
 *
 * <p>This class enables partition-based workers that eliminate head-of-line blocking
 * while maintaining per-key ordering. The key insight is that ordering only matters
 * within the same partition (same key maps to same partition), so different partitions
 * can run in parallel.</p>
 *
 * <p>Key design principles:</p>
 * <ul>
 *   <li>Each worker is a single-threaded executor handling a subset of partitions</li>
 *   <li>Tasks for the same partition always go to the same worker, preserving order</li>
 *   <li>Different partitions can be processed in parallel by different workers</li>
 *   <li>Blocking policy provides backpressure when queues are full (caller blocks until space available)</li>
 * </ul>
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
 *   <li>workerCount=0, callbackThreadCount=0: Fully inline (preprocess + dispatch on caller thread,
 *       callback on Kafka thread)</li>
 *   <li>workerCount=0, callbackThreadCount>0: Inline preprocessing, callback on dedicated threads</li>
 *   <li>workerCount>0, callbackThreadCount=0: Default - parallel workers, callback on Kafka thread</li>
 *   <li>workerCount>0, callbackThreadCount>0: Full async - parallel workers + callback isolation</li>
 * </ul>
 */
public class PartitionedProducerExecutor {
  private static final Logger LOGGER = LogManager.getLogger(PartitionedProducerExecutor.class);

  /**
   * A rejection handler that blocks the submitting thread until queue space is available.
   * Unlike CallerRunsPolicy (which runs the task on the caller thread), this ensures
   * tasks always execute on the worker thread, preserving thread affinity guarantees.
   *
   * <p>Handles shutdown gracefully by checking executor state and throwing
   * RejectedExecutionException if the executor is shutting down.</p>
   */
  private static class BlockingRejectionHandler implements RejectedExecutionHandler {
    private static final long OFFER_TIMEOUT_MS = 100;
    private final String poolName;

    BlockingRejectionHandler(String poolName) {
      this.poolName = poolName;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      BlockingQueue<Runnable> queue = executor.getQueue();
      LOGGER.warn("Queue full for {}, blocking caller. Queue size: {}", poolName, queue.size());
      try {
        while (!executor.isShutdown()) {
          if (queue.offer(r, OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            return;
          }
        }
        throw new RejectedExecutionException("Executor has been shutdown");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException("Interrupted while waiting for queue space", e);
      }
    }
  }

  private final ThreadPoolExecutor[] workers; // null if workerCount=0
  private final ThreadPoolExecutor callbackExecutor; // null if callbackThreadCount=0
  private final int workerCount;
  private final boolean workersEnabled;
  private final boolean callbackExecutorEnabled;

  /**
   * Creates a new partitioned producer executor.
   *
   * @param workerCount number of partition workers (0 to disable and execute inline)
   * @param workerQueueCapacity queue capacity per worker for backpressure
   * @param callbackThreadCount number of callback threads (0 to disable and run on Kafka thread)
   * @param callbackQueueCapacity queue capacity for callback executor
   * @param storeName store name for naming threads and metrics
   * @param metricsRepository metrics repository for registering thread pool stats (may be null)
   */
  public PartitionedProducerExecutor(
      int workerCount,
      int workerQueueCapacity,
      int callbackThreadCount,
      int callbackQueueCapacity,
      String storeName,
      MetricsRepository metricsRepository) {

    this.workersEnabled = workerCount > 0;
    this.callbackExecutorEnabled = callbackThreadCount > 0;
    this.workerCount = workersEnabled ? workerCount : 0;

    // Worker threads (OPTIONAL - null if disabled)
    if (workersEnabled) {
      this.workers = new ThreadPoolExecutor[workerCount];
      for (int i = 0; i < workerCount; i++) {
        String workerName = "venice-producer-worker-" + storeName + "-" + i;
        this.workers[i] = new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(workerQueueCapacity),
            new DaemonThreadFactory(workerName),
            new BlockingRejectionHandler(workerName));

        if (metricsRepository != null) {
          new ThreadPoolStats(metricsRepository, workers[i], storeName + "_producer_worker_" + i);
        }
      }
      LOGGER.info(
          "Created {} partition workers for store {} with queue capacity {}",
          workerCount,
          storeName,
          workerQueueCapacity);
    } else {
      this.workers = null;
      LOGGER.info("Workers disabled for store {}, tasks will execute inline on caller thread", storeName);
    }

    // Callback executor (OPTIONAL - null if disabled)
    if (callbackExecutorEnabled) {
      String callbackPoolName = "venice-producer-callback-" + storeName;
      this.callbackExecutor = new ThreadPoolExecutor(
          callbackThreadCount,
          callbackThreadCount,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(callbackQueueCapacity),
          new DaemonThreadFactory(callbackPoolName),
          new BlockingRejectionHandler(callbackPoolName));

      if (metricsRepository != null) {
        new ThreadPoolStats(metricsRepository, callbackExecutor, storeName + "_producer_callback_pool");
      }
      LOGGER.info(
          "Created callback executor for store {} with {} threads and queue capacity {}",
          storeName,
          callbackThreadCount,
          callbackQueueCapacity);
    } else {
      this.callbackExecutor = null;
      LOGGER.info("Callback executor disabled for store {}, callbacks will run on Kafka thread", storeName);
    }
  }

  /**
   * Submit work for a specific partition.
   * If workers enabled: routes to worker[partition % workerCount].
   * If workers disabled: executes inline on caller thread.
   *
   * @param partition the partition number used for routing to the appropriate worker
   * @param task the work to execute (preprocessing + dispatch)
   */
  public void submit(int partition, Runnable task) {
    if (!workersEnabled) {
      // INLINE execution on caller thread
      task.run();
      return;
    }
    // Use bitwise AND to handle Integer.MIN_VALUE correctly (Math.abs would return negative)
    int workerIndex = (partition & Integer.MAX_VALUE) % workerCount;
    try {
      workers[workerIndex].execute(task);
    } catch (RejectedExecutionException e) {
      // Fallback: execute inline to ensure task completes (e.g., during shutdown)
      LOGGER.warn("Worker executor rejected task for partition {}, executing inline", partition, e);
      task.run();
    }
  }

  /**
   * Execute callback (for user future completion).
   * If callback executor enabled: hands off to callback pool.
   * If callback executor disabled: runs inline on caller (Kafka) thread.
   *
   * @param callback the callback to execute
   */
  public void executeCallback(Runnable callback) {
    if (!callbackExecutorEnabled) {
      // INLINE execution on Kafka callback thread
      callback.run();
      return;
    }

    try {
      callbackExecutor.execute(callback);
    } catch (RejectedExecutionException e) {
      // Fail fast rather than risk blocking Kafka I/O thread with user callback code
      LOGGER.error("Callback executor rejected task during shutdown", e);
      throw e;
    }
  }

  /**
   * @return whether worker threads are enabled (workerCount > 0)
   */
  public boolean isWorkersEnabled() {
    return workersEnabled;
  }

  /**
   * @return whether callback executor is enabled (callbackThreadCount > 0)
   */
  public boolean isCallbackExecutorEnabled() {
    return callbackExecutorEnabled;
  }

  /**
   * Get queue depth for specific worker (for metrics).
   *
   * @param workerIndex the worker index
   * @return queue size for the specified worker, or 0 if workers disabled
   */
  public int getWorkerQueueSize(int workerIndex) {
    if (!workersEnabled) {
      return 0;
    }
    // Use bitwise AND to handle Integer.MIN_VALUE correctly (Math.abs would return negative)
    return workers[(workerIndex & Integer.MAX_VALUE) % workerCount].getQueue().size();
  }

  /**
   * Get total queue depth across all workers.
   *
   * @return sum of all worker queue depths, or 0 if workers disabled
   */
  public int getTotalWorkerQueueSize() {
    if (!workersEnabled) {
      return 0;
    }
    int total = 0;
    for (ThreadPoolExecutor worker: workers) {
      total += worker.getQueue().size();
    }
    return total;
  }

  /**
   * Get callback executor queue depth.
   *
   * @return callback queue size, or 0 if callback executor disabled
   */
  public int getCallbackQueueSize() {
    if (!callbackExecutorEnabled) {
      return 0;
    }
    return callbackExecutor.getQueue().size();
  }

  /**
   * @return number of workers, or 0 if workers disabled
   */
  public int getWorkerCount() {
    return workerCount;
  }

  /**
   * Initiates an orderly shutdown in which previously submitted tasks are executed,
   * but no new tasks will be accepted.
   */
  public void shutdown() {
    if (workers != null) {
      for (ThreadPoolExecutor worker: workers) {
        worker.shutdown();
      }
    }
    if (callbackExecutor != null) {
      callbackExecutor.shutdown();
    }
  }

  /**
   * Attempts to stop all actively executing tasks and halts the processing of waiting tasks.
   * This method should be called after {@link #shutdown()} and {@link #awaitTermination(long, TimeUnit)}
   * if tasks did not complete within the timeout.
   */
  public void shutdownNow() {
    if (workers != null) {
      for (ThreadPoolExecutor worker: workers) {
        worker.shutdownNow();
      }
    }
    if (callbackExecutor != null) {
      callbackExecutor.shutdownNow();
    }
  }

  /**
   * Blocks until all tasks have completed execution after a shutdown request,
   * or the timeout occurs, or the current thread is interrupted, whichever happens first.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return true if all executors terminated, false if timeout elapsed
   * @throws InterruptedException if interrupted while waiting
   */
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);

    // Wait for all workers in parallel
    boolean workersTerminated = true;
    if (workers != null) {
      List<CompletableFuture<Boolean>> futures = new ArrayList<>(workers.length);
      for (ThreadPoolExecutor worker: workers) {
        futures.add(CompletableFuture.supplyAsync(() -> {
          try {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
              return false;
            }
            return worker.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
          }
        }));
      }

      // Wait for all futures and combine results
      for (CompletableFuture<Boolean> future: futures) {
        try {
          workersTerminated &= future.join();
        } catch (Exception e) {
          workersTerminated = false;
        }
      }
    }

    // Wait for callback executor (single executor, no parallelization needed)
    boolean callbackTerminated = true;
    if (callbackExecutor != null) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        return false;
      }
      callbackTerminated = callbackExecutor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
    }

    return workersTerminated && callbackTerminated;
  }
}
