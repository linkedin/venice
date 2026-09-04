package com.linkedin.venice.writer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
  public void testAwaitTerminationReportsAlreadyTerminatedExecutorsWithZeroTimeout() throws Exception {
    PartitionedVeniceWriteExecutor inlineExecutor =
        new PartitionedVeniceWriteExecutor(0, 1, 0, 1, "terminated-store", null);
    PartitionedVeniceWriteExecutor workerExecutor =
        new PartitionedVeniceWriteExecutor(1, 1, 0, 1, "terminated-store", null);
    inlineExecutor.shutdown();
    workerExecutor.shutdown();
    assertTrue(workerExecutor.awaitWorkerTermination(5, TimeUnit.SECONDS));

    assertTrue(inlineExecutor.awaitTermination(0, TimeUnit.NANOSECONDS));
    assertTrue(workerExecutor.awaitTermination(0, TimeUnit.NANOSECONDS));
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
    CountDownLatch thirdTaskCompleted = new CountDownLatch(1);
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
        executor.submit(0, thirdTaskCompleted::countDown);
        thirdSubmitReturned.set(true);
      });
      submitter.start();
      assertEquals(executor.getWorkerQueueSize(0), 1, "The worker queue must be full before checking backpressure");
      assertTrue(executor.awaitWorkerAdmission(0, 5, TimeUnit.SECONDS));
      assertFalse(thirdSubmitReturned.get(), "The caller should block while the worker queue is full");

      releaseWorker.countDown();
      assertTrue(queuedTaskCompleted.await(5, TimeUnit.SECONDS));
      submitter.join(TimeUnit.SECONDS.toMillis(5));
      assertTrue(thirdSubmitReturned.get());
      assertTrue(thirdTaskCompleted.await(5, TimeUnit.SECONDS), "The blocked task must execute without being dropped");
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
    CountDownLatch blockedSubmitStarted = new CountDownLatch(1);
    Thread submitter = null;
    try {
      executor.submit(0, () -> {
        activeStarted.countDown();
        await(releaseActive);
      });
      assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
      executor.submit(0, () -> {});

      submitter = new Thread(() -> {
        blockedSubmitStarted.countDown();
        try {
          executor.submit(0, () -> blockedTaskRan.set(true), rejection::complete);
        } catch (RejectedExecutionException ignored) {
          // Expected after shutdown rejects the submission contending for bounded admission.
        }
      });
      submitter.start();
      assertTrue(blockedSubmitStarted.await(5, TimeUnit.SECONDS));
      assertEquals(executor.getWorkerQueueSize(0), 1, "The worker queue must be full before shutdown");
      assertTrue(executor.awaitWorkerAdmission(0, 5, TimeUnit.SECONDS));
      assertFalse(rejection.isDone());

      executor.shutdownWorkers();
      submitter.join(TimeUnit.SECONDS.toMillis(5));
      assertTrue(rejection.get(5, TimeUnit.SECONDS) instanceof RejectedExecutionException);
      assertFalse(blockedTaskRan.get());

      releaseActive.countDown();
      assertTrue(executor.awaitWorkerTermination(5, TimeUnit.SECONDS));
    } finally {
      releaseActive.countDown();
      executor.shutdownWorkersNow();
      if (submitter != null) {
        submitter.interrupt();
        submitter.join(TimeUnit.SECONDS.toMillis(5));
      }
      executor.awaitWorkerTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testForcedShutdownLeavesActiveTaskCompletionOwnedByTask() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 2, 0, 1, "shutdown-now-store", null);
    CountDownLatch activeStarted = new CountDownLatch(1);
    CountDownLatch interruptObserved = new CountDownLatch(1);
    CountDownLatch releaseActive = new CountDownLatch(1);
    AtomicInteger activeRejections = new AtomicInteger();
    AtomicInteger queuedRejections = new AtomicInteger();
    AtomicBoolean queuedTaskRan = new AtomicBoolean();

    try {
      executor.submit(0, () -> {
        activeStarted.countDown();
        boolean interrupted = false;
        while (true) {
          try {
            releaseActive.await();
            break;
          } catch (InterruptedException exception) {
            interrupted = true;
            interruptObserved.countDown();
          }
        }
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }, ignored -> activeRejections.incrementAndGet());
      assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
      executor.submit(0, () -> queuedTaskRan.set(true), ignored -> queuedRejections.incrementAndGet());

      assertFalse(executor.shutdownWorkersAndAwait(0, TimeUnit.NANOSECONDS));
      assertTrue(interruptObserved.await(5, TimeUnit.SECONDS));

      assertEquals(activeRejections.get(), 0);
      assertEquals(queuedRejections.get(), 1);
      assertFalse(queuedTaskRan.get());
      assertFalse(executor.awaitWorkerTermination(0, TimeUnit.NANOSECONDS));

      executor.shutdownWorkersNow();
      assertEquals(activeRejections.get(), 0);
      assertEquals(queuedRejections.get(), 1);

      releaseActive.countDown();
      assertTrue(executor.awaitWorkerTermination(5, TimeUnit.SECONDS));
      assertEquals(activeRejections.get(), 0);
      assertEquals(queuedRejections.get(), 1);
    } finally {
      releaseActive.countDown();
      executor.shutdownWorkersNow();
      executor.awaitWorkerTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test(timeOut = 15000)
  public void testTwoArgShutdownSharesOneTotalTerminationWindow() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 1, 0, 1, "shared-shutdown-window-store", null);
    CountDownLatch activeStarted = new CountDownLatch(1);
    CountDownLatch forcedInterruptObserved = new CountDownLatch(1);
    CountDownLatch allowTaskTermination = new CountDownLatch(1);
    executor.submit(0, () -> {
      activeStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException exception) {
        forcedInterruptObserved.countDown();
        await(allowTaskTermination);
      }
    });
    assertTrue(activeStarted.await(5, TimeUnit.SECONDS));

    CompletableFuture<Boolean> shutdownResult = new CompletableFuture<>();
    Thread shutdownThread = new Thread(
        () -> shutdownResult.complete(executor.shutdownWorkersAndAwait(5, TimeUnit.SECONDS)),
        "test-shared-shutdown-window");
    shutdownThread.start();
    try {
      assertTrue(forcedInterruptObserved.await(10, TimeUnit.SECONDS));
      assertFalse(shutdownResult.get(1, TimeUnit.SECONDS));
    } finally {
      allowTaskTermination.countDown();
      shutdownThread.interrupt();
      shutdownThread.join(TimeUnit.SECONDS.toMillis(5));
      executor.shutdownWorkersNow();
      executor.awaitWorkerTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test(timeOut = 30000)
  public void testForcedShutdownGetsIndependentTerminationWindow() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 1, 0, 1, "forced-shutdown-window-store", null);
    CountDownLatch activeStarted = new CountDownLatch(1);
    CountDownLatch forcedInterruptObserved = new CountDownLatch(1);
    CountDownLatch allowTaskTermination = new CountDownLatch(1);
    executor.submit(0, () -> {
      activeStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException exception) {
        forcedInterruptObserved.countDown();
        await(allowTaskTermination);
      }
    });
    assertTrue(activeStarted.await(5, TimeUnit.SECONDS));

    CompletableFuture<Boolean> shutdownResult = new CompletableFuture<>();
    Thread shutdownThread = new Thread(
        () -> shutdownResult.complete(executor.shutdownWorkersAndAwait(0, TimeUnit.NANOSECONDS, 5, TimeUnit.SECONDS)),
        "test-forced-shutdown");
    shutdownThread.start();
    try {
      assertTrue(forcedInterruptObserved.await(5, TimeUnit.SECONDS));
      allowTaskTermination.countDown();
      assertTrue(shutdownResult.get(5, TimeUnit.SECONDS));
      shutdownThread.join(TimeUnit.SECONDS.toMillis(5));
    } finally {
      allowTaskTermination.countDown();
      shutdownThread.interrupt();
      shutdownThread.join(TimeUnit.SECONDS.toMillis(5));
      executor.shutdownWorkersNow();
      executor.awaitWorkerTermination(5, TimeUnit.SECONDS);
    }
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
    try {
      submitter.start();
      assertTrue(taskStarted.await(5, TimeUnit.SECONDS));

      executor.shutdownWorkers();
      assertFalse(executor.awaitWorkerTermination(0, TimeUnit.NANOSECONDS));

      releaseTask.countDown();
      submitter.join(TimeUnit.SECONDS.toMillis(5));
      assertTrue(executor.awaitWorkerTermination(5, TimeUnit.SECONDS));
    } finally {
      releaseTask.countDown();
      submitter.interrupt();
      submitter.join(TimeUnit.SECONDS.toMillis(5));
      executor.shutdownWorkersNow();
      executor.awaitWorkerTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testReentrantInlineShutdownWaitsForOtherThread() throws Exception {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(0, 1, 0, 1, "inline-reentrant-close-store", null);
    CountDownLatch otherTaskStarted = new CountDownLatch(1);
    CountDownLatch releaseOtherTask = new CountDownLatch(1);
    Thread otherThread = new Thread(() -> executor.submit(0, () -> {
      otherTaskStarted.countDown();
      await(releaseOtherTask);
    }));

    try {
      otherThread.start();
      assertTrue(otherTaskStarted.await(5, TimeUnit.SECONDS));
      executor.submit(0, () -> {
        executor.shutdownWorkers();
        try {
          assertFalse(executor.awaitWorkerTermination(0, TimeUnit.NANOSECONDS));
          releaseOtherTask.countDown();
          otherThread.join(TimeUnit.SECONDS.toMillis(5));
          assertTrue(executor.awaitWorkerTermination(0, TimeUnit.NANOSECONDS));
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
          throw new AssertionError("Interrupted while awaiting inline worker termination", exception);
        }
      });
    } finally {
      releaseOtherTask.countDown();
      otherThread.interrupt();
      otherThread.join(TimeUnit.SECONDS.toMillis(5));
      executor.shutdownWorkersNow();
      executor.awaitWorkerTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testNestedInlineDepthRestoresExactly() {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(0, 1, 0, 1, "nested-inline-depth-store", null);
    ThreadLocal<Integer> inlineWorkerDepth = getField(executor, "inlineWorkerDepth");
    List<Integer> observedDepths = new ArrayList<>();

    executor.submit(0, () -> {
      observedDepths.add(inlineWorkerDepth.get());
      executor.submit(0, () -> observedDepths.add(inlineWorkerDepth.get()));
      observedDepths.add(inlineWorkerDepth.get());
    });

    assertEquals(observedDepths, Arrays.asList(1, 2, 1));
    assertNull(inlineWorkerDepth.get());
    executor.shutdownWorkers();
  }

  @Test
  public void testInterruptedInlineSelfShutdownRestoresInterrupt() {
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(0, 1, 0, 1, "interrupted-inline-close-store", null);
    AtomicBoolean terminated = new AtomicBoolean();
    AtomicBoolean interruptRestored = new AtomicBoolean();

    executor.submit(0, () -> {
      Thread.currentThread().interrupt();
      terminated.set(executor.shutdownWorkersAndAwait(1, TimeUnit.SECONDS));
      interruptRestored.set(Thread.currentThread().isInterrupted());
      Thread.interrupted();
    });

    assertTrue(terminated.get());
    assertTrue(interruptRestored.get());
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
        executor.executeCallback(() -> {});
        thirdSubmissionReturned.set(true);
      });
      submitter.start();
      assertEquals(executor.getCallbackQueueSize(), 1, "The callback queue must be full before checking backpressure");
      assertTrue(executor.awaitCallbackAdmission(5, TimeUnit.SECONDS));
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

  @SuppressWarnings("unchecked")
  private static <T> T getField(Object target, String fieldName) {
    try {
      Field field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return (T) field.get(target);
    } catch (ReflectiveOperationException exception) {
      throw new AssertionError("Unable to inspect executor state", exception);
    }
  }

}
