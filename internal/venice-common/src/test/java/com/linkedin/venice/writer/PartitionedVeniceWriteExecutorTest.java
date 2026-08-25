package com.linkedin.venice.writer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;


public class PartitionedVeniceWriteExecutorTest {
  @Test
  public void testEnabledPoolsRejectZeroQueueCapacity() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new PartitionedVeniceWriteExecutor(1, 0, 0, 1, "invalid-worker-store", null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PartitionedVeniceWriteExecutor(0, 1, 1, 0, "invalid-callback-store", null));
  }

  @Test
  public void testSameStripePreservesFifo() throws Exception {
    PartitionedVeniceWriteExecutor executor = new PartitionedVeniceWriteExecutor(2, 100, 0, 1, "fifo-store", null);
    List<Integer> order = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch completed = new CountDownLatch(20);
    try {
      for (int index = 0; index < 20; index++) {
        final int value = index;
        executor.submit(4, () -> {
          order.add(value);
          completed.countDown();
        });
      }

      assertTrue(completed.await(10, TimeUnit.SECONDS));
      for (int index = 0; index < 20; index++) {
        assertEquals(order.get(index).intValue(), index);
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testDifferentStripeProgressesWhileOneIsBlocked() throws Exception {
    PartitionedVeniceWriteExecutor executor = new PartitionedVeniceWriteExecutor(2, 10, 0, 1, "parallel-store", null);
    CountDownLatch blockedStripeStarted = new CountDownLatch(1);
    CountDownLatch unblockStripe = new CountDownLatch(1);
    CountDownLatch otherStripeCompleted = new CountDownLatch(1);
    try {
      executor.submit(0, () -> {
        blockedStripeStarted.countDown();
        try {
          unblockStripe.await();
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
        }
      });
      assertTrue(blockedStripeStarted.await(5, TimeUnit.SECONDS));

      executor.submit(1, otherStripeCompleted::countDown);
      assertTrue(otherStripeCompleted.await(5, TimeUnit.SECONDS));
    } finally {
      unblockStripe.countDown();
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testQueueFullBlocksAndUnblocksWhenCapacityDrains() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 1, 0, 1, "backpressure-store", null);
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    CountDownLatch queuedTaskCompleted = new CountDownLatch(1);
    AtomicBoolean thirdSubmitReturned = new AtomicBoolean(false);
    Thread submitter = null;
    try {
      executor.submit(0, () -> {
        workerStarted.countDown();
        try {
          releaseWorker.await();
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
        }
      });
      assertTrue(workerStarted.await(5, TimeUnit.SECONDS));
      executor.submit(0, queuedTaskCompleted::countDown);

      submitter = new Thread(() -> {
        executor.submit(0, () -> {});
        thirdSubmitReturned.set(true);
      });
      submitter.start();
      Thread.sleep(200);
      assertFalse(thirdSubmitReturned.get(), "The caller should block while the worker queue is full");

      releaseWorker.countDown();
      assertTrue(queuedTaskCompleted.await(5, TimeUnit.SECONDS));
      submitter.join(TimeUnit.SECONDS.toMillis(5));
      assertTrue(thirdSubmitReturned.get());
    } finally {
      releaseWorker.countDown();
      if (submitter != null) {
        submitter.join(TimeUnit.SECONDS.toMillis(5));
      }
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testShutdownRejectionNeverRunsTaskInline() throws Exception {
    PartitionedVeniceWriteExecutor executor = new PartitionedVeniceWriteExecutor(1, 10, 0, 1, "shutdown-store", null);
    executor.shutdownWorkers();

    AtomicBoolean taskRan = new AtomicBoolean(false);
    CompletableFuture<Void> rejected = new CompletableFuture<>();
    assertThrows(
        RejectedExecutionException.class,
        () -> executor.submit(0, () -> taskRan.set(true), rejected::completeExceptionally));

    assertFalse(taskRan.get());
    assertTrue(rejected.isCompletedExceptionally());
    assertTrue(executor.awaitWorkerTermination(5, TimeUnit.SECONDS));
  }

  @Test
  public void testShutdownWakesSubmitterBlockedOnFullQueue() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 1, 0, 1, "shutdown-waiter-store", null);
    CountDownLatch activeStarted = new CountDownLatch(1);
    CountDownLatch releaseActive = new CountDownLatch(1);
    CompletableFuture<Throwable> rejection = new CompletableFuture<>();
    AtomicBoolean blockedTaskRan = new AtomicBoolean(false);
    executor.submit(0, () -> {
      activeStarted.countDown();
      await(releaseActive);
    });
    assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
    executor.submit(0, () -> {});

    Thread submitter = new Thread(() -> {
      try {
        executor.submit(0, () -> blockedTaskRan.set(true), rejection::complete);
      } catch (RejectedExecutionException ignored) {
        // Expected after shutdown wakes bounded admission.
      }
    });
    submitter.start();
    Thread.sleep(200);
    assertFalse(rejection.isDone());

    executor.shutdownWorkers();
    submitter.join(TimeUnit.SECONDS.toMillis(5));
    assertTrue(rejection.get(5, TimeUnit.SECONDS) instanceof RejectedExecutionException);
    assertFalse(blockedTaskRan.get());

    releaseActive.countDown();
    assertTrue(executor.awaitWorkerTermination(5, TimeUnit.SECONDS));
  }

  @Test
  public void testShutdownNowRejectsActiveAndQueuedTasks() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 2, 0, 1, "shutdown-now-store", null);
    CountDownLatch activeStarted = new CountDownLatch(1);
    CountDownLatch keepActive = new CountDownLatch(1);
    CompletableFuture<Void> activeRejected = new CompletableFuture<>();
    CompletableFuture<Void> queuedRejected = new CompletableFuture<>();

    executor.submit(0, () -> {
      activeStarted.countDown();
      try {
        keepActive.await();
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
      }
    }, activeRejected::completeExceptionally);
    assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
    executor.submit(0, () -> {}, queuedRejected::completeExceptionally);

    executor.shutdownWorkersNow();

    assertTrue(activeRejected.isCompletedExceptionally());
    assertTrue(queuedRejected.isCompletedExceptionally());
    assertTrue(executor.awaitWorkerTermination(5, TimeUnit.SECONDS));
  }

  @Test(timeOut = 5000)
  public void testForcedShutdownGetsIndependentTerminationWindow() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 1, 0, 1, "forced-shutdown-window-store", null);
    CountDownLatch activeStarted = new CountDownLatch(1);
    executor.submit(0, () -> {
      activeStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException exception) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
      }
    });
    assertTrue(activeStarted.await(5, TimeUnit.SECONDS));

    assertTrue(executor.shutdownWorkersAndAwait(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWorkerCountZeroExecutesInlineUntilShutdown() {
    PartitionedVeniceWriteExecutor executor = new PartitionedVeniceWriteExecutor(0, 1, 0, 1, "inline-store", null);
    Thread caller = Thread.currentThread();
    AtomicBoolean inline = new AtomicBoolean(false);

    assertTrue(executor.trySubmit(7, () -> inline.set(Thread.currentThread() == caller), null));
    assertTrue(inline.get());

    executor.shutdownWorkers();
    assertThrows(RejectedExecutionException.class, () -> executor.submit(7, () -> {}));
  }

  @Test
  public void testTrySubmitReturnsFalseWithoutBlockingWhenWorkerQueueIsFull() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 1, 0, 1, "nonblocking-worker-store", null);
    CountDownLatch activeStarted = new CountDownLatch(1);
    CountDownLatch releaseActive = new CountDownLatch(1);
    try {
      executor.submit(0, () -> {
        activeStarted.countDown();
        await(releaseActive);
      });
      assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
      executor.submit(0, () -> {});

      AtomicBoolean rejectedTaskRan = new AtomicBoolean(false);
      assertFalse(executor.trySubmit(0, () -> rejectedTaskRan.set(true), null));
      assertFalse(rejectedTaskRan.get());
    } finally {
      releaseActive.countDown();
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testWorkerCountZeroShutdownWaitsForAcceptedInlineTask() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(0, 1, 0, 1, "inline-drain-store", null);
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch releaseTask = new CountDownLatch(1);
    Thread submitter = new Thread(() -> executor.submit(0, () -> {
      taskStarted.countDown();
      try {
        releaseTask.await();
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
      }
    }));
    submitter.start();
    assertTrue(taskStarted.await(5, TimeUnit.SECONDS));

    executor.shutdownWorkers();
    assertFalse(executor.awaitWorkerTermination(100, TimeUnit.MILLISECONDS));

    releaseTask.countDown();
    submitter.join(TimeUnit.SECONDS.toMillis(5));
    assertTrue(executor.awaitWorkerTermination(5, TimeUnit.SECONDS));
  }

  @Test
  public void testConfiguredCallbackExecutorUsesCallbackThread() throws Exception {
    PartitionedVeniceWriteExecutor executor = new PartitionedVeniceWriteExecutor(0, 1, 1, 1, "callback-store", null);
    CompletableFuture<String> callbackThread = new CompletableFuture<>();
    CompletableFuture<Boolean> callbackOwnership = new CompletableFuture<>();
    try {
      assertFalse(executor.isCurrentThreadExecutingCallback());
      executor.executeCallback(() -> {
        callbackThread.complete(Thread.currentThread().getName());
        callbackOwnership.complete(executor.isCurrentThreadExecutingCallback());
      });
      assertTrue(callbackThread.get(5, TimeUnit.SECONDS).contains("venice-producer-callback-callback-store"));
      assertTrue(callbackOwnership.get(5, TimeUnit.SECONDS));
      assertFalse(executor.isCurrentThreadExecutingCallback());
    } finally {
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testTryExecuteCallbackReturnsFalseWithoutBlockingWhenQueueIsFull() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(0, 1, 1, 1, "nonblocking-callback-store", null);
    CountDownLatch activeCallbackStarted = new CountDownLatch(1);
    CountDownLatch releaseActiveCallback = new CountDownLatch(1);
    CountDownLatch queuedCallbackCompleted = new CountDownLatch(1);
    AtomicBoolean rejectedCallbackRan = new AtomicBoolean(false);
    try {
      assertTrue(executor.tryExecuteCallback(() -> {
        activeCallbackStarted.countDown();
        await(releaseActiveCallback);
      }, null));
      assertTrue(activeCallbackStarted.await(5, TimeUnit.SECONDS));
      assertTrue(executor.tryExecuteCallback(queuedCallbackCompleted::countDown, null));

      assertFalse(executor.tryExecuteCallback(() -> rejectedCallbackRan.set(true), null));
      assertFalse(rejectedCallbackRan.get());

      releaseActiveCallback.countDown();
      assertTrue(queuedCallbackCompleted.await(5, TimeUnit.SECONDS));
      executor.shutdownCallbacks();
      assertFalse(executor.tryExecuteCallback(() -> rejectedCallbackRan.set(true), null));
    } finally {
      releaseActiveCallback.countDown();
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testExecuteCallbackRetainsBlockingBackpressure() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(0, 1, 1, 1, "blocking-callback-store", null);
    CountDownLatch activeCallbackStarted = new CountDownLatch(1);
    CountDownLatch releaseActiveCallback = new CountDownLatch(1);
    CountDownLatch thirdSubmissionStarted = new CountDownLatch(1);
    AtomicBoolean thirdSubmissionReturned = new AtomicBoolean(false);
    Thread submitter = null;
    try {
      executor.executeCallback(() -> {
        activeCallbackStarted.countDown();
        await(releaseActiveCallback);
      });
      assertTrue(activeCallbackStarted.await(5, TimeUnit.SECONDS));
      executor.executeCallback(() -> {});

      submitter = new Thread(() -> {
        thirdSubmissionStarted.countDown();
        executor.executeCallback(() -> {});
        thirdSubmissionReturned.set(true);
      });
      submitter.start();
      assertTrue(thirdSubmissionStarted.await(5, TimeUnit.SECONDS));
      Thread.sleep(200);
      assertFalse(thirdSubmissionReturned.get());

      releaseActiveCallback.countDown();
      submitter.join(TimeUnit.SECONDS.toMillis(5));
      assertTrue(thirdSubmissionReturned.get());
    } finally {
      releaseActiveCallback.countDown();
      if (submitter != null) {
        submitter.join(TimeUnit.SECONDS.toMillis(5));
      }
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }
}
