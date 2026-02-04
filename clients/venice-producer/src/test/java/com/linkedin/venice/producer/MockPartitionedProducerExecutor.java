package com.linkedin.venice.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Mock implementation of {@link PartitionedProducerExecutor} for testing purposes.
 * Supports configurable modes (workers enabled/disabled, callback enabled/disabled)
 * and provides tracking for test assertions.
 */
public class MockPartitionedProducerExecutor implements PartitionedProducerExecutor {
  // Configuration (simulate different modes)
  private final boolean workersEnabled;
  private final boolean callbackEnabled;
  private final int workerCount;

  // Tracking for assertions
  private final List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
  private final List<String> executionThreads = Collections.synchronizedList(new ArrayList<>());
  private final AtomicInteger workerQueueSize = new AtomicInteger(0);
  private final AtomicInteger callbackQueueSize = new AtomicInteger(0);

  // Optional delay injection for testing timing scenarios
  private volatile long taskDelayMs = 0;

  /**
   * Create a mock executor with specified configuration.
   *
   * @param workersEnabled whether to simulate worker threads (true) or inline execution (false)
   * @param callbackEnabled whether to simulate callback executor (true) or inline callback (false)
   */
  public MockPartitionedProducerExecutor(boolean workersEnabled, boolean callbackEnabled) {
    this(workersEnabled, callbackEnabled, 4);
  }

  /**
   * Create a mock executor with specified configuration and worker count.
   *
   * @param workersEnabled whether to simulate worker threads
   * @param callbackEnabled whether to simulate callback executor
   * @param workerCount number of simulated workers
   */
  public MockPartitionedProducerExecutor(boolean workersEnabled, boolean callbackEnabled, int workerCount) {
    this.workersEnabled = workersEnabled;
    this.callbackEnabled = callbackEnabled;
    this.workerCount = workersEnabled ? workerCount : 0;
  }

  /**
   * Set task delay for testing timing scenarios.
   *
   * @param delayMs delay in milliseconds to add to each task execution
   */
  public void setTaskDelay(long delayMs) {
    this.taskDelayMs = delayMs;
  }

  @Override
  public void submit(int partition, Runnable task) {
    workerQueueSize.incrementAndGet();

    Runnable wrappedTask = () -> {
      try {
        if (taskDelayMs > 0) {
          Thread.sleep(taskDelayMs);
        }
        executionOrder.add("SUBMIT_P" + partition);
        executionThreads.add(Thread.currentThread().getName());
        task.run();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        workerQueueSize.decrementAndGet();
      }
    };

    if (workersEnabled) {
      // Simulate async execution
      CompletableFuture.runAsync(wrappedTask);
    } else {
      // Inline execution
      wrappedTask.run();
    }
  }

  @Override
  public void executeCallback(Runnable callback) {
    callbackQueueSize.incrementAndGet();
    try {
      executionOrder.add("CALLBACK");
      executionThreads.add(Thread.currentThread().getName());
      callback.run();
    } finally {
      callbackQueueSize.decrementAndGet();
    }
  }

  @Override
  public boolean isWorkersEnabled() {
    return workersEnabled;
  }

  @Override
  public boolean isCallbackExecutorEnabled() {
    return callbackEnabled;
  }

  @Override
  public int getWorkerQueueSize(int workerIndex) {
    return workerQueueSize.get();
  }

  @Override
  public int getTotalWorkerQueueSize() {
    return workerQueueSize.get();
  }

  @Override
  public int getCallbackQueueSize() {
    return callbackQueueSize.get();
  }

  @Override
  public int getWorkerCount() {
    return workerCount;
  }

  // ==================== Test Helpers ====================

  /**
   * Get the execution order for assertions.
   *
   * @return copy of execution order list
   */
  public List<String> getExecutionOrder() {
    return new ArrayList<>(executionOrder);
  }

  /**
   * Get the execution threads for assertions.
   *
   * @return copy of execution threads list
   */
  public List<String> getExecutionThreads() {
    return new ArrayList<>(executionThreads);
  }

  /**
   * Reset tracking state between tests.
   */
  public void reset() {
    executionOrder.clear();
    executionThreads.clear();
    workerQueueSize.set(0);
    callbackQueueSize.set(0);
  }

  @Override
  public void shutdown() {
    // No-op for mock
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) {
    return true;
  }
}
