package com.linkedin.venice.writer;

import com.linkedin.venice.utils.DaemonThreadFactory;
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
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/** A bounded executor whose admission blocks instead of running rejected work on the caller. */
final class BlockingBoundedExecutor {
  private static final Logger LOGGER = LogManager.getLogger(BlockingBoundedExecutor.class);
  private static final long ADMISSION_POLL_INTERVAL_MS = 100;
  private static final ThreadLocal<BlockingBoundedExecutor> CURRENT_EXECUTOR = new ThreadLocal<>();

  private final ThreadPoolExecutor executor;
  private final Semaphore queueSlots;
  private final AtomicBoolean accepting = new AtomicBoolean(true);
  private final AtomicBoolean forceShutdown = new AtomicBoolean(false);
  private final Set<TrackedTask> activeTasks = Collections.newSetFromMap(new ConcurrentHashMap<TrackedTask, Boolean>());
  private final Object lifecycleLock = new Object();
  private final String name;

  BlockingBoundedExecutor(int threadCount, int queueCapacity, String name) {
    this.name = name;
    this.queueSlots = new Semaphore(queueCapacity);
    this.executor = new ThreadPoolExecutor(
        threadCount,
        threadCount,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(queueCapacity),
        new DaemonThreadFactory(name),
        new ThreadPoolExecutor.AbortPolicy());
  }

  void execute(Runnable task, Consumer<Throwable> rejectionCallback) {
    acquireQueueSlot(rejectionCallback);
    TrackedTask trackedTask = new TrackedTask(this, task, rejectionCallback);
    synchronized (lifecycleLock) {
      if (!accepting.get()) {
        RejectedExecutionException exception = shutdownRejection();
        trackedTask.reject(exception);
        throw exception;
      }
      try {
        executor.execute(trackedTask);
      } catch (RejectedExecutionException exception) {
        trackedTask.reject(exception);
        throw exception;
      }
    }
  }

  /** Attempts admission without waiting for queue capacity. */
  boolean tryExecute(Runnable task, Consumer<Throwable> rejectionCallback) {
    if (!accepting.get() || !queueSlots.tryAcquire()) {
      return false;
    }

    TrackedTask trackedTask = new TrackedTask(this, task, rejectionCallback);
    synchronized (lifecycleLock) {
      if (!accepting.get()) {
        trackedTask.releaseQueueSlot();
        return false;
      }
      try {
        executor.execute(trackedTask);
        return true;
      } catch (RejectedExecutionException exception) {
        trackedTask.releaseQueueSlot();
        return false;
      }
    }
  }

  void shutdown() {
    synchronized (lifecycleLock) {
      accepting.set(false);
      executor.shutdown();
    }
  }

  void shutdownNow() {
    List<Runnable> queuedTasks;
    List<TrackedTask> activeTaskSnapshot;
    synchronized (lifecycleLock) {
      accepting.set(false);
      forceShutdown.set(true);
      activeTaskSnapshot = new ArrayList<>(activeTasks);
      queuedTasks = executor.shutdownNow();
    }

    RejectedExecutionException exception =
        new RejectedExecutionException("Executor " + name + " was shut down immediately");
    for (Runnable queuedTask: queuedTasks) {
      ((TrackedTask) queuedTask).reject(exception);
    }
    for (TrackedTask activeTask: activeTaskSnapshot) {
      activeTask.reject(exception);
    }
    for (TrackedTask activeTask: activeTasks) {
      activeTask.reject(exception);
    }
  }

  int getQueueSize() {
    return executor.getQueue().size();
  }

  ThreadPoolExecutor getThreadPoolExecutor() {
    return executor;
  }

  boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return executor.awaitTermination(timeout, unit);
  }

  boolean isCurrentThreadExecutingTask() {
    return CURRENT_EXECUTOR.get() == this;
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
      RejectedExecutionException exception = shutdownRejection();
      notifyRejection(rejectionCallback, exception);
      throw exception;
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      RejectedExecutionException rejection =
          new RejectedExecutionException("Interrupted while waiting for queue space in " + name, exception);
      notifyRejection(rejectionCallback, rejection);
      throw rejection;
    }
  }

  private RejectedExecutionException shutdownRejection() {
    return new RejectedExecutionException("Executor " + name + " has been shut down");
  }

  static void notifyRejection(Consumer<Throwable> rejectionCallback, Throwable throwable) {
    if (rejectionCallback == null) {
      return;
    }
    try {
      rejectionCallback.accept(throwable);
    } catch (Throwable callbackFailure) {
      LOGGER.warn("Work rejection callback failed", callbackFailure);
    }
  }

  private static final class TrackedTask implements Runnable {
    private final BlockingBoundedExecutor owner;
    private final Runnable task;
    private final Consumer<Throwable> rejectionCallback;
    private final AtomicBoolean queueSlotReleased = new AtomicBoolean(false);
    private final AtomicBoolean rejectionNotified = new AtomicBoolean(false);

    private TrackedTask(BlockingBoundedExecutor owner, Runnable task, Consumer<Throwable> rejectionCallback) {
      this.owner = owner;
      this.task = task;
      this.rejectionCallback = rejectionCallback;
    }

    @Override
    public void run() {
      BlockingBoundedExecutor previousExecutor = CURRENT_EXECUTOR.get();
      CURRENT_EXECUTOR.set(owner);
      try {
        owner.activeTasks.add(this);
        releaseQueueSlot();
        if (owner.forceShutdown.get()) {
          reject(new RejectedExecutionException("Executor " + owner.name + " was shut down immediately"));
          return;
        }
        task.run();
      } finally {
        owner.activeTasks.remove(this);
        if (previousExecutor == null) {
          CURRENT_EXECUTOR.remove();
        } else {
          CURRENT_EXECUTOR.set(previousExecutor);
        }
      }
    }

    private void reject(Throwable throwable) {
      releaseQueueSlot();
      if (rejectionNotified.compareAndSet(false, true)) {
        notifyRejection(rejectionCallback, throwable);
      }
    }

    private void releaseQueueSlot() {
      if (queueSlotReleased.compareAndSet(false, true)) {
        owner.queueSlots.release();
      }
    }
  }
}
