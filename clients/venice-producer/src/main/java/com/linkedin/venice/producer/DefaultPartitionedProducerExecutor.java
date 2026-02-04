package com.linkedin.venice.producer;

import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Default implementation of {@link PartitionedProducerExecutor} that uses partition-based
 * workers for parallel processing while maintaining per-key ordering.
 *
 * <p>Key design principles:</p>
 * <ul>
 *   <li>Each worker is a single-threaded executor handling a subset of partitions</li>
 *   <li>Tasks for the same partition always go to the same worker, preserving order</li>
 *   <li>Different partitions can be processed in parallel by different workers</li>
 *   <li>CallerRunsPolicy provides backpressure when queues are full</li>
 * </ul>
 *
 * <p>Both worker pool and callback executor are optional:</p>
 * <ul>
 *   <li>workerCount=0: Tasks execute inline on caller thread</li>
 *   <li>callbackThreadCount=0: Callbacks execute on Kafka callback thread</li>
 * </ul>
 */
public class DefaultPartitionedProducerExecutor implements PartitionedProducerExecutor {
  private static final Logger LOGGER = LogManager.getLogger(DefaultPartitionedProducerExecutor.class);

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
  public DefaultPartitionedProducerExecutor(
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
        this.workers[i] = new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(workerQueueCapacity),
            new DaemonThreadFactory("venice-producer-worker-" + storeName + "-" + i),
            new ThreadPoolExecutor.CallerRunsPolicy());

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
      this.callbackExecutor = new ThreadPoolExecutor(
          callbackThreadCount,
          callbackThreadCount,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(callbackQueueCapacity),
          new DaemonThreadFactory("venice-producer-callback-" + storeName),
          new ThreadPoolExecutor.CallerRunsPolicy());

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

  @Override
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

  @Override
  public void executeCallback(Runnable callback) {
    if (!callbackExecutorEnabled) {
      // INLINE execution on Kafka callback thread
      callback.run();
      return;
    }

    try {
      callbackExecutor.execute(callback);
    } catch (RejectedExecutionException e) {
      // Fallback: execute inline to ensure future completes
      LOGGER.warn("Callback executor rejected task, executing inline", e);
      callback.run();
    }
  }

  @Override
  public boolean isWorkersEnabled() {
    return workersEnabled;
  }

  @Override
  public boolean isCallbackExecutorEnabled() {
    return callbackExecutorEnabled;
  }

  @Override
  public int getWorkerQueueSize(int workerIndex) {
    if (!workersEnabled) {
      return 0;
    }
    // Use bitwise AND to handle Integer.MIN_VALUE correctly (Math.abs would return negative)
    return workers[(workerIndex & Integer.MAX_VALUE) % workerCount].getQueue().size();
  }

  @Override
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

  @Override
  public int getCallbackQueueSize() {
    if (!callbackExecutorEnabled) {
      return 0;
    }
    return callbackExecutor.getQueue().size();
  }

  @Override
  public int getWorkerCount() {
    return workerCount;
  }

  @Override
  public void shutdown() {
    if (workers != null) {
      for (ThreadPoolExecutor worker: workers) {
        worker.shutdownNow();
      }
    }
    if (callbackExecutor != null) {
      callbackExecutor.shutdownNow();
    }
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    boolean terminated = true;
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);

    if (workers != null) {
      for (ThreadPoolExecutor worker: workers) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          return false;
        }
        terminated &= worker.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
      }
    }

    if (callbackExecutor != null) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        return false;
      }
      terminated &= callbackExecutor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
    }

    return terminated;
  }
}
