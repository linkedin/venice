package com.linkedin.venice.writer;

import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Partition-striped executor shared by Venice producers.
 *
 * <p>Each enabled worker is single-threaded. A partition is always assigned to
 * {@code (partition & Integer.MAX_VALUE) % workerCount}, which preserves FIFO ordering within a stripe while allowing
 * different stripes to make progress independently. Queue saturation applies blocking backpressure to the submitting
 * thread; rejected work is never run inline.</p>
 *
 * <p>Both the worker pool and callback pool are optional. A count of zero keeps the corresponding execution inline.
 * The rejection callback overloads let adapters complete futures for work removed by {@link #shutdownNow()}.</p>
 */
public class PartitionedVeniceWriteExecutor {
  private static final Logger LOGGER = LogManager.getLogger(PartitionedVeniceWriteExecutor.class);
  private static final long ADMISSION_POLL_INTERVAL_MS = 100;

  private final BoundedExecutor[] workers;
  private final BoundedExecutor callbackExecutor;
  private final int workerCount;
  private final AtomicBoolean inlineWorkerAdmissionOpen = new AtomicBoolean(true);
  private final ReentrantLock inlineWorkerLock = new ReentrantLock();
  private final Condition inlineWorkersDrained = inlineWorkerLock.newCondition();
  private int activeInlineWorkers;

  /**
   * Creates an executor with the historical Online Venice Producer thread and metric names.
   */
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

  PartitionedVeniceWriteExecutor(
      int workerCount,
      int workerQueueCapacity,
      int callbackThreadCount,
      int callbackQueueCapacity,
      String storeName,
      MetricsRepository metricsRepository,
      String threadNamePrefix) {
    this.workerCount = workerCount > 0 ? workerCount : 0;
    if (this.workerCount > 0 && workerQueueCapacity <= 0) {
      throw new IllegalArgumentException("Worker queue capacity must be greater than zero");
    }
    if (callbackThreadCount > 0 && callbackQueueCapacity <= 0) {
      throw new IllegalArgumentException("Callback queue capacity must be greater than zero");
    }
    if (this.workerCount > 0) {
      workers = new BoundedExecutor[this.workerCount];
      for (int workerIndex = 0; workerIndex < this.workerCount; workerIndex++) {
        String workerName = threadNamePrefix + "-worker-" + storeName + "-" + workerIndex;
        workers[workerIndex] = new BoundedExecutor(1, workerQueueCapacity, workerName);
        if (metricsRepository != null) {
          new ThreadPoolStats(
              metricsRepository,
              workers[workerIndex].getThreadPoolExecutor(),
              storeName + "_producer_worker_" + workerIndex);
        }
      }
      LOGGER.info(
          "Created {} partition workers for store {} with queue capacity {}",
          this.workerCount,
          storeName,
          workerQueueCapacity);
    } else {
      workers = null;
      LOGGER.info("Workers disabled for store {}, tasks will execute inline on caller thread", storeName);
    }

    if (callbackThreadCount > 0) {
      String callbackPoolName = threadNamePrefix + "-callback-" + storeName;
      callbackExecutor = new BoundedExecutor(callbackThreadCount, callbackQueueCapacity, callbackPoolName);
      if (metricsRepository != null) {
        new ThreadPoolStats(
            metricsRepository,
            callbackExecutor.getThreadPoolExecutor(),
            storeName + "_producer_callback_pool");
      }
      LOGGER.info(
          "Created callback executor for store {} with {} threads and queue capacity {}",
          storeName,
          callbackThreadCount,
          callbackQueueCapacity);
    } else {
      callbackExecutor = null;
      LOGGER.info("Callback executor disabled for store {}, callbacks will run on caller thread", storeName);
    }
  }

  /**
   * Submit work for a partition.
   *
   * @throws RejectedExecutionException if worker admission has stopped or the caller is interrupted while blocked on a
   *                                     full queue
   */
  public void submit(int partition, Runnable task) {
    submit(partition, task, null);
  }

  /**
   * Submit work for a partition and notify {@code rejectionCallback} if it cannot run, including when queued work is
   * removed by {@link #shutdownNow()}.
   */
  public void submit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
    if (workers == null) {
      inlineWorkerLock.lock();
      try {
        if (!inlineWorkerAdmissionOpen.get()) {
          RejectedExecutionException exception = new RejectedExecutionException("Worker executor has been shut down");
          notifyRejection(rejectionCallback, exception);
          throw exception;
        }
        activeInlineWorkers++;
      } finally {
        inlineWorkerLock.unlock();
      }
      try {
        task.run();
      } finally {
        inlineWorkerLock.lock();
        try {
          activeInlineWorkers--;
          if (activeInlineWorkers == 0) {
            inlineWorkersDrained.signalAll();
          }
        } finally {
          inlineWorkerLock.unlock();
        }
      }
      return;
    }

    int workerIndex = (partition & Integer.MAX_VALUE) % workerCount;
    workers[workerIndex].execute(task, rejectionCallback);
  }

  void submitControl(int workerIndex, Runnable task, Consumer<Throwable> rejectionCallback) {
    if (workers == null) {
      submit(workerIndex, task, rejectionCallback);
      return;
    }
    workers[(workerIndex & Integer.MAX_VALUE) % workerCount].executeControl(task, rejectionCallback);
  }

  /**
   * Execute a completion callback on the configured callback executor, or inline when callback threads are disabled.
   */
  public void executeCallback(Runnable callback) {
    executeCallback(callback, null);
  }

  /**
   * Execute a completion callback and notify {@code rejectionCallback} if it cannot run. Adapters should keep rejection
   * callbacks non-blocking because immediate rejection can be observed on a PubSub completion thread.
   */
  public void executeCallback(Runnable callback, Consumer<Throwable> rejectionCallback) {
    if (callbackExecutor == null) {
      callback.run();
      return;
    }
    callbackExecutor.execute(callback, rejectionCallback);
  }

  public boolean isWorkersEnabled() {
    return workers != null;
  }

  public boolean isCallbackExecutorEnabled() {
    return callbackExecutor != null;
  }

  public int getWorkerQueueSize(int workerIndex) {
    if (workers == null) {
      return 0;
    }
    return workers[(workerIndex & Integer.MAX_VALUE) % workerCount].getQueueSize();
  }

  public int getTotalWorkerQueueSize() {
    if (workers == null) {
      return 0;
    }
    int total = 0;
    for (BoundedExecutor worker: workers) {
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

  /**
   * Stop worker admission and drain accepted worker tasks.
   */
  public void shutdownWorkers() {
    if (workers != null) {
      for (BoundedExecutor worker: workers) {
        worker.shutdown();
      }
    } else {
      inlineWorkerLock.lock();
      try {
        inlineWorkerAdmissionOpen.set(false);
      } finally {
        inlineWorkerLock.unlock();
      }
    }
  }

  /**
   * Stop callback admission and drain accepted callbacks.
   */
  public void shutdownCallbacks() {
    if (callbackExecutor != null) {
      callbackExecutor.shutdown();
    }
  }

  public void shutdown() {
    shutdownWorkers();
    shutdownCallbacks();
  }

  /**
   * Interrupt active worker tasks, reject queued worker tasks, and notify their rejection callbacks.
   */
  public void shutdownWorkersNow() {
    if (workers != null) {
      for (BoundedExecutor worker: workers) {
        worker.shutdownNow();
      }
    } else {
      inlineWorkerLock.lock();
      try {
        inlineWorkerAdmissionOpen.set(false);
      } finally {
        inlineWorkerLock.unlock();
      }
    }
  }

  /**
   * Interrupt active callbacks, reject queued callbacks, and notify their rejection callbacks.
   */
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
    if (callbackExecutor == null) {
      return true;
    }
    return callbackExecutor.awaitTermination(timeout, unit);
  }

  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    if (workers == null) {
      if (!awaitInlineWorkerTermination(timeout, unit)) {
        return false;
      }
    } else {
      for (BoundedExecutor worker: workers) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0 || !worker.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
          return false;
        }
      }
    }
    if (callbackExecutor != null) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      return remainingNanos > 0 && callbackExecutor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
    }
    return true;
  }

  private boolean awaitInlineWorkerTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long remainingNanos = unit.toNanos(timeout);
    inlineWorkerLock.lockInterruptibly();
    try {
      while (activeInlineWorkers > 0) {
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

  private static boolean awaitTermination(BoundedExecutor[] executors, long timeout, TimeUnit unit)
      throws InterruptedException {
    if (executors == null) {
      return true;
    }
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    for (BoundedExecutor executor: executors) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0 || !executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
        return false;
      }
    }
    return true;
  }

  private static void notifyRejection(Consumer<Throwable> rejectionCallback, Throwable throwable) {
    if (rejectionCallback == null) {
      return;
    }
    try {
      rejectionCallback.accept(throwable);
    } catch (Throwable callbackFailure) {
      LOGGER.warn("Work rejection callback failed", callbackFailure);
    }
  }

  /**
   * A bounded executor whose queue capacity is reserved before calling {@link ThreadPoolExecutor#execute(Runnable)}.
   * Reserving queue slots explicitly allows admission to block without a caller-runs fallback and lets shutdown wake
   * blocked submitters without racing a direct insertion into a shut-down executor queue.
   */
  private static final class BoundedExecutor {
    private final ThreadPoolExecutor executor;
    private final Semaphore queueSlots;
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicBoolean forceShutdown = new AtomicBoolean(false);
    private final Set<FailureAwareTask> activeTasks =
        Collections.newSetFromMap(new ConcurrentHashMap<FailureAwareTask, Boolean>());
    private final Object lifecycleLock = new Object();
    private final String executorName;

    private BoundedExecutor(int threadCount, int queueCapacity, String executorName) {
      this.executorName = executorName;
      this.queueSlots = new Semaphore(queueCapacity);
      int executorQueueCapacity = queueCapacity == Integer.MAX_VALUE ? queueCapacity : queueCapacity + 1;
      this.executor = new ThreadPoolExecutor(
          threadCount,
          threadCount,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(executorQueueCapacity),
          new DaemonThreadFactory(executorName),
          new ThreadPoolExecutor.AbortPolicy());
    }

    private void execute(Runnable task, Consumer<Throwable> rejectionCallback) {
      acquireQueueSlot(rejectionCallback);
      FailureAwareTask failureAwareTask =
          new FailureAwareTask(task, rejectionCallback, queueSlots, activeTasks, forceShutdown, executorName);
      synchronized (lifecycleLock) {
        if (!accepting.get()) {
          RejectedExecutionException exception =
              new RejectedExecutionException("Executor " + executorName + " has been shut down");
          failureAwareTask.reject(exception);
          throw exception;
        }
        try {
          executor.execute(failureAwareTask);
        } catch (RejectedExecutionException exception) {
          failureAwareTask.reject(exception);
          throw exception;
        }
      }
    }

    private void executeControl(Runnable task, Consumer<Throwable> rejectionCallback) {
      FailureAwareTask failureAwareTask =
          new FailureAwareTask(task, rejectionCallback, null, activeTasks, forceShutdown, executorName);
      synchronized (lifecycleLock) {
        if (!accepting.get()) {
          RejectedExecutionException exception =
              new RejectedExecutionException("Executor " + executorName + " has been shut down");
          failureAwareTask.reject(exception);
          throw exception;
        }
        try {
          executor.execute(failureAwareTask);
        } catch (RejectedExecutionException exception) {
          failureAwareTask.reject(exception);
          throw exception;
        }
      }
    }

    private void acquireQueueSlot(Consumer<Throwable> rejectionCallback) {
      try {
        while (accepting.get()) {
          if (queueSlots.tryAcquire(ADMISSION_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
            if (accepting.get()) {
              return;
            }
            queueSlots.release();
            break;
          }
        }
        RejectedExecutionException exception =
            new RejectedExecutionException("Executor " + executorName + " has been shut down");
        notifyRejection(rejectionCallback, exception);
        throw exception;
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        RejectedExecutionException rejection =
            new RejectedExecutionException("Interrupted while waiting for queue space in " + executorName, exception);
        notifyRejection(rejectionCallback, rejection);
        throw rejection;
      }
    }

    private void shutdown() {
      synchronized (lifecycleLock) {
        accepting.set(false);
        executor.shutdown();
      }
    }

    private void shutdownNow() {
      List<Runnable> queuedTasks;
      List<FailureAwareTask> activeTaskSnapshot;
      synchronized (lifecycleLock) {
        accepting.set(false);
        forceShutdown.set(true);
        activeTaskSnapshot = new ArrayList<>(activeTasks);
        queuedTasks = executor.shutdownNow();
      }
      RejectedExecutionException exception =
          new RejectedExecutionException("Executor " + executorName + " was shut down immediately");
      for (Runnable queuedTask: queuedTasks) {
        ((FailureAwareTask) queuedTask).reject(exception);
      }
      for (FailureAwareTask activeTask: activeTaskSnapshot) {
        activeTask.reject(exception);
      }
      for (FailureAwareTask activeTask: activeTasks) {
        activeTask.reject(exception);
      }
    }

    private int getQueueSize() {
      return executor.getQueue().size();
    }

    private ThreadPoolExecutor getThreadPoolExecutor() {
      return executor;
    }

    private boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return executor.awaitTermination(timeout, unit);
    }
  }

  private static final class FailureAwareTask implements Runnable {
    private final Runnable task;
    private final Consumer<Throwable> rejectionCallback;
    private final Semaphore queueSlots;
    private final Set<FailureAwareTask> activeTasks;
    private final AtomicBoolean forceShutdown;
    private final String executorName;
    private final AtomicBoolean queueSlotReleased = new AtomicBoolean(false);
    private final AtomicBoolean rejectionNotified = new AtomicBoolean(false);

    private FailureAwareTask(
        Runnable task,
        Consumer<Throwable> rejectionCallback,
        Semaphore queueSlots,
        Set<FailureAwareTask> activeTasks,
        AtomicBoolean forceShutdown,
        String executorName) {
      this.task = task;
      this.rejectionCallback = rejectionCallback;
      this.queueSlots = queueSlots;
      this.activeTasks = activeTasks;
      this.forceShutdown = forceShutdown;
      this.executorName = executorName;
    }

    @Override
    public void run() {
      activeTasks.add(this);
      releaseQueueSlot();
      try {
        if (forceShutdown.get()) {
          reject(new RejectedExecutionException("Executor " + executorName + " was shut down immediately"));
          return;
        }
        task.run();
      } finally {
        activeTasks.remove(this);
      }
    }

    private void reject(Throwable throwable) {
      releaseQueueSlot();
      if (rejectionNotified.compareAndSet(false, true)) {
        notifyRejection(rejectionCallback, throwable);
      }
    }

    private void releaseQueueSlot() {
      if (queueSlots != null && queueSlotReleased.compareAndSet(false, true)) {
        queueSlots.release();
      }
    }
  }
}
