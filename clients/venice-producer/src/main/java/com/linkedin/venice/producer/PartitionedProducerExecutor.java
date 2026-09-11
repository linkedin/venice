package com.linkedin.venice.producer;

import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.PartitionStripedExecutor;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.BlockingQueue;
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
 * <p>The partition-worker mechanism is the shared, producer-agnostic {@link PartitionStripedExecutor}; this
 * class adds only producer-specific policy: the optional callback pool, caller-runs fallback, and inline mode.</p>
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

  private final PartitionStripedExecutor workers; // null if workerCount=0
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
      this.workers = new PartitionStripedExecutor(
          workerCount,
          workerQueueCapacity,
          "venice-producer-worker-" + storeName,
          metricsRepository == null
              ? null
              : (worker, i) -> new ThreadPoolStats(metricsRepository, worker, storeName + "_producer_worker_" + i));
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
   * If workers enabled: routes to the stripe owning the partition (blocking if its queue is full).
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
    try {
      workers.submit(partition, task);
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
    return workers.getStripeQueueSize(workers.stripeFor(workerIndex));
  }

  /**
   * Get total queue depth across all workers.
   *
   * @return sum of all worker queue depths, or 0 if workers disabled
   */
  public int getTotalWorkerQueueSize() {
    return workersEnabled ? workers.getTotalQueueSize() : 0;
  }

  /**
   * Get callback executor queue depth.
   *
   * @return callback queue size, or 0 if callback executor disabled
   */
  public int getCallbackQueueSize() {
    return callbackExecutorEnabled ? callbackExecutor.getQueue().size() : 0;
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
      workers.shutdown();
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
      workers.shutdownNow();
    }
    if (callbackExecutor != null) {
      callbackExecutor.shutdownNow();
    }
  }

  /**
   * Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs.
   * <p>An interrupt does not abandon the drain (that would drop still-queued writes); it is absorbed, each wait
   * continues against the original deadline, and it is re-thrown afterwards so the caller owns interrupt restoration.</p>
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return true if all executors terminated, false if timeout elapsed
   * @throws InterruptedException if interrupted (thrown only after draining or the original deadline lapses)
   */
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    boolean interrupted = false;

    boolean workersTerminated = workers == null;
    while (!workersTerminated) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        break;
      }
      try {
        workersTerminated = workers.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }

    boolean callbackTerminated = callbackExecutor == null;
    while (!callbackTerminated) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        break;
      }
      try {
        callbackTerminated = callbackExecutor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }

    if (interrupted) {
      throw new InterruptedException();
    }
    return workersTerminated && callbackTerminated;
  }

  /**
   * Blocks the caller until the callback-pool queue has space, throwing once the pool is shutting down.
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
}
