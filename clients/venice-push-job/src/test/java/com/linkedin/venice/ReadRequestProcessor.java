package com.linkedin.venice;

import com.linkedin.venice.utils.Utils;
import java.time.Instant;
import java.util.concurrent.*;


public class ReadRequestProcessor {
  public enum TriggerType {
    READ, WRITE
  }

  // Task updated to remove Comparable implementation
  public static class ReadRequestTask implements Runnable {
    private final TriggerType triggerType;
    private final long maxDelayedUntil; // Epoch time in seconds
    private final String name;
    private final Runnable operation;

    public ReadRequestTask(TriggerType triggerType, long maxDelayedUntil, String name, Runnable operation) {
      this.triggerType = triggerType;
      this.maxDelayedUntil = maxDelayedUntil;
      this.name = name;
      this.operation = operation;
    }

    @Override
    public void run() {
      try {
        System.out.println(this.name + " (" + this.triggerType + ") triggered successfully");
        Thread.sleep(2000); // Simulating processing delay
        System.out.println(this.name + " (" + this.triggerType + ") completed successfully");
      } catch (InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(interruptedException);
      }
    }
  }

  // A custom single-threaded ThreadPoolExecutor that overrides newTaskFor to enforce prioritization
  public static class CustomSingleThreadPoolExecutor extends ThreadPoolExecutor {
    public CustomSingleThreadPoolExecutor(BlockingQueue<Runnable> workQueue) {
      super(1, 1, 1000, TimeUnit.SECONDS, workQueue);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
      return new CustomFutureTask<>(runnable);
    }
  }

  // A comparable FutureTask that ensures prioritization
  public static class CustomFutureTask<T> extends FutureTask<T> implements Comparable<CustomFutureTask<T>> {
    private final ReadRequestTask task;

    public CustomFutureTask(Runnable task) {
      super(task, null);
      this.task = (ReadRequestTask) task;
    }

    @Override
    public int compareTo(CustomFutureTask<T> that) {
      long currentTime = Instant.now().getEpochSecond();

      boolean thisWriteElevated =
          this.task.triggerType == TriggerType.WRITE && currentTime >= this.task.maxDelayedUntil;
      boolean otherWriteElevated =
          that.task.triggerType == TriggerType.WRITE && currentTime >= that.task.maxDelayedUntil;

      if (thisWriteElevated && !otherWriteElevated) {
        return -1; // This WRITE has been elevated to high priority
      } else if (!thisWriteElevated && otherWriteElevated) {
        return 1; // Other WRITE has been elevated, so it runs first
      }

      // READ vs WRITE: READ has priority unless WRITE is elevated
      if (this.task.triggerType == TriggerType.READ && that.task.triggerType == TriggerType.WRITE) {
        return -1; // READ takes priority over non-elevated WRITE
      } else if (this.task.triggerType == TriggerType.WRITE && that.task.triggerType == TriggerType.READ) {
        return 1; // WRITE has lower priority unless elevated
      }

      // Both are READ: prioritize by earlier maxDelayedUntil
      if (this.task.triggerType == TriggerType.READ && that.task.triggerType == TriggerType.READ) {
        return Long.compare(this.task.maxDelayedUntil, that.task.maxDelayedUntil);
      }

      return 0; // Otherwise, equal priority
    }
  }

  private final BlockingQueue<Runnable> taskQueue = new PriorityBlockingQueue<>();
  private final ExecutorService executorService;

  public ReadRequestProcessor() {
    this.executorService = new CustomSingleThreadPoolExecutor(taskQueue);
  }

  public void addTask(ReadRequestTask task) {
    executorService.submit(task);
  }

  public void shutdown() {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public static void main(String[] args) {
    ReadRequestProcessor processor = new ReadRequestProcessor();

    long now = Instant.now().getEpochSecond();

    // Adding tasks
    processor.addTask(new ReadRequestTask(TriggerType.WRITE, now, "------BLOCK------", () -> {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }));
    Utils.sleep(1000);
    processor.addTask(new ReadRequestTask(TriggerType.WRITE, now + 10, "Write Task 1", () -> {}));
    processor.addTask(new ReadRequestTask(TriggerType.READ, now + 5, "Read Task 1", () -> {}));
    processor.addTask(new ReadRequestTask(TriggerType.READ, now + 2, "Read Task 2", () -> {}));
    processor.addTask(new ReadRequestTask(TriggerType.WRITE, now + 1, "Write Task 2", () -> {}));
    processor.addTask(new ReadRequestTask(TriggerType.WRITE, now, "Write Task Elevated", () -> {}));

    try {
      Thread.sleep(11000); // Wait for 11 seconds to allow WRITE tasks to elevate
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Shutdown after some time
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    processor.shutdown();
  }
}
