package com.linkedin.venice.producer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link DefaultPartitionedProducerExecutor} covering all 4 execution modes:
 * <ul>
 *   <li>Workers=0, Callback=0: Fully inline</li>
 *   <li>Workers>0, Callback=0: Parallel workers, inline callback</li>
 *   <li>Workers=0, Callback>0: Inline work, callback isolation</li>
 *   <li>Workers>0, Callback>0: Full async</li>
 * </ul>
 */
public class PartitionedProducerExecutorTest {
  private static final String TEST_STORE = "test-store";

  // ==================== Workers Enabled Tests ====================

  @Test
  public void testWorkersEnabled_SubmitRoutesToCorrectWorker() throws InterruptedException {
    int workerCount = 4;
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(workerCount, 100, 0, 100, TEST_STORE, null);

    try {
      assertTrue(executor.isWorkersEnabled());
      assertEquals(executor.getWorkerCount(), workerCount);

      // Track which worker handled each partition
      List<String> workerThreads = Collections.synchronizedList(new ArrayList<>());
      CountDownLatch latch = new CountDownLatch(workerCount);

      // Submit tasks for different partitions
      for (int partition = 0; partition < workerCount; partition++) {
        final int p = partition;
        executor.submit(partition, () -> {
          workerThreads.add(Thread.currentThread().getName() + "-p" + p);
          latch.countDown();
        });
      }

      assertTrue(latch.await(5, TimeUnit.SECONDS), "Tasks should complete");
      assertEquals(workerThreads.size(), workerCount);

      // Verify tasks went to different workers
      for (int i = 0; i < workerCount; i++) {
        String expectedWorkerPrefix = "venice-producer-worker-" + TEST_STORE + "-" + i;
        boolean found = workerThreads.stream().anyMatch(t -> t.contains(expectedWorkerPrefix));
        assertTrue(found, "Partition " + i + " should be handled by worker " + i);
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testWorkersEnabled_SamePartitionExecutesInOrder() throws InterruptedException {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(4, 100, 0, 100, TEST_STORE, null);

    try {
      List<Integer> executionOrder = Collections.synchronizedList(new ArrayList<>());
      int taskCount = 100;
      CountDownLatch latch = new CountDownLatch(taskCount);

      // Submit all tasks to the same partition (partition 0)
      for (int i = 0; i < taskCount; i++) {
        final int taskNum = i;
        executor.submit(0, () -> {
          executionOrder.add(taskNum);
          latch.countDown();
        });
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS), "All tasks should complete");
      assertEquals(executionOrder.size(), taskCount);

      // Verify order is preserved
      for (int i = 0; i < taskCount; i++) {
        assertEquals(executionOrder.get(i).intValue(), i, "Task " + i + " should execute in order");
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testWorkersEnabled_DifferentPartitionsParallel() throws InterruptedException {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(4, 100, 0, 100, TEST_STORE, null);

    try {
      CountDownLatch partition0Started = new CountDownLatch(1);
      CountDownLatch partition1Started = new CountDownLatch(1);
      CountDownLatch bothStarted = new CountDownLatch(2);
      AtomicBoolean parallelExecution = new AtomicBoolean(false);

      // Task for partition 0
      executor.submit(0, () -> {
        partition0Started.countDown();
        bothStarted.countDown();
        try {
          // Wait for partition 1 to also start
          if (partition1Started.await(1, TimeUnit.SECONDS)) {
            parallelExecution.set(true);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      // Task for partition 1
      executor.submit(1, () -> {
        partition1Started.countDown();
        bothStarted.countDown();
        try {
          // Wait for partition 0 to also start
          if (partition0Started.await(1, TimeUnit.SECONDS)) {
            parallelExecution.set(true);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      assertTrue(bothStarted.await(2, TimeUnit.SECONDS), "Both tasks should start");
      assertTrue(parallelExecution.get(), "Partitions should execute in parallel");
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testWorkersEnabled_QueueFullTriggersCallerRuns() throws InterruptedException {
    // Very small queue to easily trigger CallerRunsPolicy
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(1, 1, 0, 100, TEST_STORE, null);

    try {
      CountDownLatch blockingTaskStarted = new CountDownLatch(1);
      CountDownLatch allowBlockingTaskToFinish = new CountDownLatch(1);
      AtomicBoolean callerThreadUsed = new AtomicBoolean(false);
      String callerThreadName = Thread.currentThread().getName();

      // Submit a blocking task to fill the worker
      executor.submit(0, () -> {
        blockingTaskStarted.countDown();
        try {
          if (!allowBlockingTaskToFinish.await(10, TimeUnit.SECONDS)) {
            Thread.currentThread().interrupt();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      // Wait for blocking task to start
      assertTrue(blockingTaskStarted.await(2, TimeUnit.SECONDS), "Blocking task should start");

      // Submit additional tasks to fill queue and trigger CallerRunsPolicy
      CountDownLatch tasksCompleted = new CountDownLatch(5);
      for (int i = 0; i < 5; i++) {
        executor.submit(0, () -> {
          if (Thread.currentThread().getName().equals(callerThreadName)) {
            callerThreadUsed.set(true);
          }
          tasksCompleted.countDown();
        });
      }

      // Allow blocking task to finish
      allowBlockingTaskToFinish.countDown();

      assertTrue(tasksCompleted.await(5, TimeUnit.SECONDS), "All tasks should complete");
      assertTrue(callerThreadUsed.get(), "CallerRunsPolicy should have been triggered");
    } finally {
      executor.shutdown();
    }
  }

  // ==================== Workers Disabled Tests ====================

  @Test
  public void testWorkersDisabled_ExecutesInlineOnCallerThread() throws InterruptedException {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(0, 100, 0, 100, TEST_STORE, null);

    try {
      assertFalse(executor.isWorkersEnabled());
      assertEquals(executor.getWorkerCount(), 0);

      String callerThreadName = Thread.currentThread().getName();
      AtomicBoolean executedOnCallerThread = new AtomicBoolean(false);

      executor.submit(0, () -> {
        if (Thread.currentThread().getName().equals(callerThreadName)) {
          executedOnCallerThread.set(true);
        }
      });

      assertTrue(executedOnCallerThread.get(), "Task should execute inline on caller thread");
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testWorkersDisabled_OrderPreserved() {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(0, 100, 0, 100, TEST_STORE, null);

    try {
      List<Integer> executionOrder = new ArrayList<>();
      int taskCount = 10;

      // Submit tasks - they should execute inline in order
      for (int i = 0; i < taskCount; i++) {
        final int taskNum = i;
        executor.submit(0, () -> executionOrder.add(taskNum));
      }

      assertEquals(executionOrder.size(), taskCount);
      for (int i = 0; i < taskCount; i++) {
        assertEquals(executionOrder.get(i).intValue(), i, "Task " + i + " should execute in order");
      }
    } finally {
      executor.shutdown();
    }
  }

  // ==================== Callback Executor Tests ====================

  @Test
  public void testCallbackEnabled_HandsOffToCallbackThread() throws InterruptedException {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(0, 100, 2, 100, TEST_STORE, null);

    try {
      assertTrue(executor.isCallbackExecutorEnabled());

      CountDownLatch latch = new CountDownLatch(1);
      AtomicBoolean callbackOnDifferentThread = new AtomicBoolean(false);
      String callerThreadName = Thread.currentThread().getName();

      executor.executeCallback(() -> {
        if (!Thread.currentThread().getName().equals(callerThreadName)) {
          callbackOnDifferentThread.set(true);
        }
        latch.countDown();
      });

      assertTrue(latch.await(5, TimeUnit.SECONDS), "Callback should complete");
      assertTrue(callbackOnDifferentThread.get(), "Callback should execute on callback thread");
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testCallbackDisabled_ExecutesOnCallerThread() {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(0, 100, 0, 100, TEST_STORE, null);

    try {
      assertFalse(executor.isCallbackExecutorEnabled());

      String callerThreadName = Thread.currentThread().getName();
      AtomicBoolean callbackOnCallerThread = new AtomicBoolean(false);

      executor.executeCallback(() -> {
        if (Thread.currentThread().getName().equals(callerThreadName)) {
          callbackOnCallerThread.set(true);
        }
      });

      assertTrue(callbackOnCallerThread.get(), "Callback should execute inline on caller thread");
    } finally {
      executor.shutdown();
    }
  }

  // ==================== Mode Combination Tests ====================

  @Test
  public void testBothDisabled_FullyInline() {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(0, 100, 0, 100, TEST_STORE, null);

    try {
      assertFalse(executor.isWorkersEnabled());
      assertFalse(executor.isCallbackExecutorEnabled());
      assertEquals(executor.getWorkerCount(), 0);

      String callerThreadName = Thread.currentThread().getName();
      List<Boolean> results = new ArrayList<>();

      // Submit work
      executor.submit(0, () -> {
        results.add(Thread.currentThread().getName().equals(callerThreadName));
      });

      // Execute callback
      executor.executeCallback(() -> {
        results.add(Thread.currentThread().getName().equals(callerThreadName));
      });

      assertEquals(results.size(), 2);
      assertTrue(results.get(0), "Work should execute on caller thread");
      assertTrue(results.get(1), "Callback should execute on caller thread");
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testBothEnabled_FullAsync() throws InterruptedException {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(4, 100, 2, 100, TEST_STORE, null);

    try {
      assertTrue(executor.isWorkersEnabled());
      assertTrue(executor.isCallbackExecutorEnabled());
      assertEquals(executor.getWorkerCount(), 4);

      String callerThreadName = Thread.currentThread().getName();
      CountDownLatch latch = new CountDownLatch(2);
      AtomicBoolean workOnDifferentThread = new AtomicBoolean(false);
      AtomicBoolean callbackOnDifferentThread = new AtomicBoolean(false);

      // Submit work
      executor.submit(0, () -> {
        if (!Thread.currentThread().getName().equals(callerThreadName)) {
          workOnDifferentThread.set(true);
        }
        latch.countDown();
      });

      // Execute callback
      executor.executeCallback(() -> {
        if (!Thread.currentThread().getName().equals(callerThreadName)) {
          callbackOnDifferentThread.set(true);
        }
        latch.countDown();
      });

      assertTrue(latch.await(5, TimeUnit.SECONDS), "Both should complete");
      assertTrue(workOnDifferentThread.get(), "Work should execute on worker thread");
      assertTrue(callbackOnDifferentThread.get(), "Callback should execute on callback thread");
    } finally {
      executor.shutdown();
    }
  }

  // ==================== Queue Size Metrics Tests ====================

  @Test
  public void testQueueSizeMetrics_ReturnsCorrectValues() throws InterruptedException {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(2, 100, 2, 100, TEST_STORE, null);

    try {
      // Initially queues should be empty
      assertEquals(executor.getTotalWorkerQueueSize(), 0);
      assertEquals(executor.getCallbackQueueSize(), 0);

      CountDownLatch blockingLatch = new CountDownLatch(1);
      CountDownLatch tasksSubmitted = new CountDownLatch(1);

      // Submit a blocking task to worker 0
      executor.submit(0, () -> {
        try {
          tasksSubmitted.countDown();
          if (!blockingLatch.await(10, TimeUnit.SECONDS)) {
            Thread.currentThread().interrupt();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      // Wait for blocking task to start
      assertTrue(tasksSubmitted.await(2, TimeUnit.SECONDS));

      // Submit more tasks to queue them up
      for (int i = 0; i < 5; i++) {
        executor.submit(0, () -> {});
      }

      // Queue should have tasks now
      assertTrue(executor.getTotalWorkerQueueSize() > 0, "Worker queue should have tasks");
      assertTrue(executor.getWorkerQueueSize(0) > 0, "Worker 0 queue should have tasks");

      // Release blocking task
      blockingLatch.countDown();

      // Wait for queue to drain
      Thread.sleep(100);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testQueueSizeMetrics_ReturnsZeroWhenDisabled() {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(0, 100, 0, 100, TEST_STORE, null);

    try {
      assertEquals(executor.getTotalWorkerQueueSize(), 0);
      assertEquals(executor.getWorkerQueueSize(0), 0);
      assertEquals(executor.getCallbackQueueSize(), 0);
    } finally {
      executor.shutdown();
    }
  }

  // ==================== Shutdown Tests ====================

  @Test
  public void testShutdown_TerminatesAllPools() throws InterruptedException {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(4, 100, 2, 100, TEST_STORE, null);

    // Submit some tasks
    AtomicInteger completedTasks = new AtomicInteger(0);
    for (int i = 0; i < 10; i++) {
      executor.submit(i % 4, () -> {
        completedTasks.incrementAndGet();
      });
    }

    executor.shutdown();
    boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);

    assertTrue(terminated, "All pools should terminate");
  }

  @Test
  public void testAwaitTermination_ReturnsAfterShutdown() throws InterruptedException {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(1, 100, 0, 100, TEST_STORE, null);

    try {
      // Submit a quick task
      CountDownLatch taskCompleted = new CountDownLatch(1);
      executor.submit(0, () -> {
        taskCompleted.countDown();
      });

      assertTrue(taskCompleted.await(2, TimeUnit.SECONDS), "Task should complete");

      // Shutdown and await - should terminate cleanly
      executor.shutdown();
      boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);

      assertTrue(terminated, "Should terminate after shutdown with no pending tasks");
    } finally {
      executor.shutdown();
    }
  }

  // ==================== Partition Routing Tests ====================

  @Test
  public void testPartitionRouting_ModuloWorkerCount() throws InterruptedException {
    int workerCount = 3;
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(workerCount, 100, 0, 100, TEST_STORE, null);

    try {
      List<String> partitionToWorker = Collections.synchronizedList(new ArrayList<>());
      CountDownLatch latch = new CountDownLatch(6);

      // Test partitions 0-5 (should wrap around to workers 0,1,2,0,1,2)
      for (int partition = 0; partition < 6; partition++) {
        final int p = partition;
        executor.submit(partition, () -> {
          partitionToWorker.add("p" + p + "->" + Thread.currentThread().getName());
          latch.countDown();
        });
      }

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      // Verify partition routing (partition % workerCount)
      for (int partition = 0; partition < 6; partition++) {
        int expectedWorker = partition % workerCount;
        String expectedWorkerPrefix = "venice-producer-worker-" + TEST_STORE + "-" + expectedWorker;
        final int p = partition;
        boolean found =
            partitionToWorker.stream().anyMatch(s -> s.startsWith("p" + p) && s.contains(expectedWorkerPrefix));
        assertTrue(found, "Partition " + partition + " should route to worker " + expectedWorker);
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testNegativePartition_HandledCorrectly() throws InterruptedException {
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(4, 100, 0, 100, TEST_STORE, null);

    try {
      CountDownLatch latch = new CountDownLatch(1);
      AtomicBoolean taskExecuted = new AtomicBoolean(false);

      // Submit with negative partition
      executor.submit(-5, () -> {
        taskExecuted.set(true);
        latch.countDown();
      });

      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertTrue(taskExecuted.get(), "Task should execute even with negative partition");
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testIntegerMinValue_HandledCorrectly() throws InterruptedException {
    // Integer.MIN_VALUE is a special case: Math.abs(Integer.MIN_VALUE) returns Integer.MIN_VALUE (negative)
    // This test verifies the fix using bitwise AND instead of Math.abs
    DefaultPartitionedProducerExecutor executor =
        new DefaultPartitionedProducerExecutor(4, 100, 0, 100, TEST_STORE, null);

    try {
      CountDownLatch latch = new CountDownLatch(1);
      AtomicBoolean taskExecuted = new AtomicBoolean(false);

      // Submit with Integer.MIN_VALUE - this would cause ArrayIndexOutOfBoundsException with Math.abs
      executor.submit(Integer.MIN_VALUE, () -> {
        taskExecuted.set(true);
        latch.countDown();
      });

      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertTrue(taskExecuted.get(), "Task should execute even with Integer.MIN_VALUE partition");

      // Also verify getWorkerQueueSize handles Integer.MIN_VALUE
      int queueSize = executor.getWorkerQueueSize(Integer.MIN_VALUE);
      assertTrue(queueSize >= 0, "Queue size should be non-negative");
    } finally {
      executor.shutdown();
    }
  }
}
