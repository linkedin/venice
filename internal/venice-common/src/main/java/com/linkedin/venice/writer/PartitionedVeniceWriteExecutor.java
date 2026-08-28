package com.linkedin.venice.writer;

import com.linkedin.venice.stats.ThreadPoolStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/** Partition-striped executor shared by Venice producers, with zero workers preserving inline execution. */
public class PartitionedVeniceWriteExecutor {
  private static final Logger LOGGER = LogManager.getLogger(PartitionedVeniceWriteExecutor.class);

  private final BlockingBoundedExecutor[] workers;
  private final BlockingBoundedExecutor callbackExecutor;
  private final int workerCount;
  private final AtomicBoolean inlineWorkerAdmissionOpen = new AtomicBoolean(true);
  private final ReentrantLock inlineWorkerLock = new ReentrantLock();
  private final Condition inlineWorkersDrained = inlineWorkerLock.newCondition();
  private final ThreadLocal<Integer> inlineWorkerDepth = new ThreadLocal<>();
  private int activeInlineWorkers;

  /** Creates an executor with the historical Online Venice Producer thread and metric names. */
  public PartitionedVeniceWriteExecutor(
      int workerCount,
      int workerQueueCapacity,
      int callbackThreadCount,
      int callbackQueueCapacity,
      String storeName,
      MetricsRepository metricsRepository) {
    this(
        workerCount,
        workerQueueCapacity,
        callbackThreadCount,
        callbackQueueCapacity,
        storeName,
        metricsRepository,
        "venice-producer");
  }

  /** Creates an executor with a caller-provided thread name prefix. */
  public PartitionedVeniceWriteExecutor(
      int workerCount,
      int workerQueueCapacity,
      int callbackThreadCount,
      int callbackQueueCapacity,
      String storeName,
      MetricsRepository metricsRepository,
      String threadNamePrefix) {
    this.workerCount = Math.max(workerCount, 0);
    if (this.workerCount > 0 && workerQueueCapacity <= 0) {
      throw new IllegalArgumentException("Worker queue capacity must be greater than zero");
    }
    if (callbackThreadCount > 0 && callbackQueueCapacity <= 0) {
      throw new IllegalArgumentException("Callback queue capacity must be greater than zero");
    }

    this.workers = createWorkers(this.workerCount, workerQueueCapacity, storeName, metricsRepository, threadNamePrefix);
    this.callbackExecutor = createCallbackExecutor(
        callbackThreadCount,
        callbackQueueCapacity,
        storeName,
        metricsRepository,
        threadNamePrefix);
  }

  /** Submits work for a partition, blocking for queue capacity when needed. */
  public void submit(int partition, Runnable task) {
    submit(partition, task, null);
  }

  /** Submits work and reports immediate or forced-shutdown rejection through {@code rejectionCallback}. */
  public void submit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
    if (workers == null) {
      runInline(task, rejectionCallback);
      return;
    }
    workers[stripe(partition)].execute(task, rejectionCallback);
  }

  /** Attempts partitioned admission without waiting for worker queue capacity. */
  public boolean trySubmit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
    if (workers == null) {
      runInline(task, rejectionCallback);
      return true;
    }
    return workers[stripe(partition)].tryExecute(task, rejectionCallback);
  }

  /** Executes a callback on its configured executor, or inline when disabled. */
  public void executeCallback(Runnable callback) {
    executeCallback(callback, null);
  }

  /** Executes a callback and reports immediate or forced-shutdown rejection. */
  public void executeCallback(Runnable callback, Consumer<Throwable> rejectionCallback) {
    if (callbackExecutor == null) {
      callback.run();
      return;
    }
    callbackExecutor.execute(callback, rejectionCallback);
  }

  /** Attempts callback admission without blocking, leaving ownership with the caller when rejected. */
  public boolean tryExecuteCallback(Runnable callback, Consumer<Throwable> rejectionCallback) {
    if (callbackExecutor == null) {
      callback.run();
      return true;
    }
    return callbackExecutor.tryExecute(callback, rejectionCallback);
  }

  public boolean isWorkersEnabled() {
    return workers != null;
  }

  public boolean isCallbackExecutorEnabled() {
    return callbackExecutor != null;
  }

  /** Returns whether the current thread is executing a configured worker task. */
  public boolean isCurrentThreadExecutingWorker() {
    if (workers == null) {
      return false;
    }
    for (BlockingBoundedExecutor worker: workers) {
      if (worker.isCurrentThreadExecutingTask()) {
        return true;
      }
    }
    return false;
  }

  /** Returns whether the current thread is executing a configured callback task. */
  public boolean isCurrentThreadExecutingCallback() {
    return callbackExecutor != null && callbackExecutor.isCurrentThreadExecutingTask();
  }

  public int getWorkerQueueSize(int workerIndex) {
    return workers == null ? 0 : workers[stripe(workerIndex)].getQueueSize();
  }

  public int getTotalWorkerQueueSize() {
    if (workers == null) {
      return 0;
    }
    int total = 0;
    for (BlockingBoundedExecutor worker: workers) {
      total += worker.getQueueSize();
    }
    return total;
  }

  public int getCallbackQueueSize() {
    return callbackExecutor == null ? 0 : callbackExecutor.getQueueSize();
  }

  public int getWorkerCount() {
    return workerCount;
  }

  protected boolean awaitWorkerAdmission(int workerIndex, long timeout, TimeUnit unit) throws InterruptedException {
    return workers != null && workers[stripe(workerIndex)].awaitBlockedAdmission(timeout, unit);
  }

  protected boolean awaitCallbackAdmission(long timeout, TimeUnit unit) throws InterruptedException {
    return callbackExecutor != null && callbackExecutor.awaitBlockedAdmission(timeout, unit);
  }

  public void shutdownWorkers() {
    if (workers == null) {
      inlineWorkerLock.lock();
      try {
        inlineWorkerAdmissionOpen.set(false);
      } finally {
        inlineWorkerLock.unlock();
      }
      return;
    }
    for (BlockingBoundedExecutor worker: workers) {
      worker.shutdown();
    }
  }

  public void shutdownCallbacks() {
    if (callbackExecutor != null) {
      callbackExecutor.shutdown();
    }
  }

  public void shutdown() {
    shutdownWorkers();
    shutdownCallbacks();
  }

  public void shutdownWorkersNow() {
    if (workers == null) {
      shutdownWorkers();
      return;
    }
    for (BlockingBoundedExecutor worker: workers) {
      worker.shutdownNow();
    }
  }

  public void shutdownCallbacksNow() {
    if (callbackExecutor != null) {
      callbackExecutor.shutdownNow();
    }
  }

  public void shutdownNow() {
    shutdownWorkersNow();
    shutdownCallbacksNow();
  }

  public boolean awaitWorkerTermination(long timeout, TimeUnit unit) throws InterruptedException {
    if (workers == null) {
      return awaitInlineWorkerTermination(timeout, unit);
    }
    return awaitTermination(workers, timeout, unit);
  }

  public boolean awaitCallbackTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return callbackExecutor == null || callbackExecutor.awaitTermination(timeout, unit);
  }

  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    if (!awaitWorkerTermination(timeout, unit)) {
      return false;
    }
    long remainingNanos = deadlineNanos - System.nanoTime();
    return awaitCallbackTermination(Math.max(0, remainingNanos), TimeUnit.NANOSECONDS);
  }

  /** Drains accepted worker tasks within one total timeout, forcing interruption with the remaining time. */
  public boolean shutdownWorkersAndAwait(long timeout, TimeUnit unit) {
    long timeoutNanos = Math.max(0, unit.toNanos(timeout));
    long deadlineNanos = System.nanoTime() + timeoutNanos;
    shutdownWorkers();
    boolean interrupted = false;
    boolean terminated = false;
    try {
      terminated = awaitWorkerTermination(timeoutNanos, TimeUnit.NANOSECONDS);
    } catch (InterruptedException exception) {
      interrupted = true;
    }
    if (!terminated) {
      shutdownWorkersNow();
      long remainingNanos;
      do {
        remainingNanos = Math.max(0, deadlineNanos - System.nanoTime());
        try {
          terminated = awaitWorkerTermination(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException exception) {
          interrupted = true;
        }
      } while (!terminated && remainingNanos > 0);
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return terminated;
  }

  boolean shutdownWorkersAndAwait(
      long gracefulTimeout,
      TimeUnit gracefulTimeoutUnit,
      long forcedTimeout,
      TimeUnit forcedTimeoutUnit) {
    shutdownWorkers();
    boolean interrupted = false;
    boolean terminated = false;
    try {
      terminated = awaitWorkerTermination(gracefulTimeout, gracefulTimeoutUnit);
    } catch (InterruptedException exception) {
      interrupted = true;
    }
    if (!terminated) {
      shutdownWorkersNow();
      long deadlineNanos = System.nanoTime() + forcedTimeoutUnit.toNanos(forcedTimeout);
      while (!terminated && System.nanoTime() < deadlineNanos) {
        try {
          terminated = awaitWorkerTermination(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException exception) {
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return terminated;
  }

  private BlockingBoundedExecutor[] createWorkers(
      int count,
      int queueCapacity,
      String storeName,
      MetricsRepository metricsRepository,
      String threadNamePrefix) {
    if (count == 0) {
      LOGGER.info("Workers disabled for store {}, tasks will execute inline on caller thread", storeName);
      return null;
    }
    BlockingBoundedExecutor[] createdWorkers = new BlockingBoundedExecutor[count];
    for (int workerIndex = 0; workerIndex < count; workerIndex++) {
      String workerName = threadNamePrefix + "-worker-" + storeName + "-" + workerIndex;
      createdWorkers[workerIndex] = new BlockingBoundedExecutor(1, queueCapacity, workerName);
      if (metricsRepository != null) {
        new ThreadPoolStats(
            metricsRepository,
            createdWorkers[workerIndex].getThreadPoolExecutor(),
            storeName + "_producer_worker_" + workerIndex);
      }
    }
    LOGGER.info("Created {} partition workers for store {} with queue capacity {}", count, storeName, queueCapacity);
    return createdWorkers;
  }

  private BlockingBoundedExecutor createCallbackExecutor(
      int threadCount,
      int queueCapacity,
      String storeName,
      MetricsRepository metricsRepository,
      String threadNamePrefix) {
    if (threadCount <= 0) {
      LOGGER.info("Callback executor disabled for store {}, callbacks will run on caller thread", storeName);
      return null;
    }
    String callbackPoolName = threadNamePrefix + "-callback-" + storeName;
    BlockingBoundedExecutor createdExecutor = new BlockingBoundedExecutor(threadCount, queueCapacity, callbackPoolName);
    if (metricsRepository != null) {
      new ThreadPoolStats(
          metricsRepository,
          createdExecutor.getThreadPoolExecutor(),
          storeName + "_producer_callback_pool");
    }
    LOGGER.info(
        "Created callback executor for store {} with {} threads and queue capacity {}",
        storeName,
        threadCount,
        queueCapacity);
    return createdExecutor;
  }

  private void runInline(Runnable task, Consumer<Throwable> rejectionCallback) {
    Integer previousDepth;
    inlineWorkerLock.lock();
    try {
      if (!inlineWorkerAdmissionOpen.get()) {
        RejectedExecutionException exception = new RejectedExecutionException("Worker executor has been shut down");
        BlockingBoundedExecutor.notifyRejection(rejectionCallback, exception);
        throw exception;
      }
      activeInlineWorkers++;
      previousDepth = inlineWorkerDepth.get();
      inlineWorkerDepth.set(previousDepth == null ? 1 : previousDepth + 1);
    } finally {
      inlineWorkerLock.unlock();
    }
    try {
      task.run();
    } finally {
      inlineWorkerLock.lock();
      try {
        activeInlineWorkers--;
        inlineWorkersDrained.signalAll();
        if (previousDepth == null) {
          inlineWorkerDepth.remove();
        } else {
          inlineWorkerDepth.set(previousDepth);
        }
      } finally {
        inlineWorkerLock.unlock();
      }
    }
  }

  private int stripe(int partition) {
    return (partition & Integer.MAX_VALUE) % workerCount;
  }

  private boolean awaitInlineWorkerTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long remainingNanos = unit.toNanos(timeout);
    inlineWorkerLock.lockInterruptibly();
    try {
      Integer currentDepth = inlineWorkerDepth.get();
      int currentThreadInlineDepth = currentDepth == null ? 0 : currentDepth;
      while (activeInlineWorkers > currentThreadInlineDepth) {
        if (remainingNanos <= 0) {
          return false;
        }
        remainingNanos = inlineWorkersDrained.awaitNanos(remainingNanos);
      }
      return true;
    } finally {
      inlineWorkerLock.unlock();
    }
  }

  private static boolean awaitTermination(BlockingBoundedExecutor[] executors, long timeout, TimeUnit unit)
      throws InterruptedException {
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    for (BlockingBoundedExecutor executor: executors) {
      long remainingNanos = Math.max(0, deadlineNanos - System.nanoTime());
      if (!executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
        return false;
      }
    }
    return true;
  }

}
