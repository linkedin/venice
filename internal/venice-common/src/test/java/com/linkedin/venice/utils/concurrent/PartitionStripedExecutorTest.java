package com.linkedin.venice.utils.concurrent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;


/**
 * Deterministic (latch/barrier based, no sleeps for positive waits) tests for the producer-agnostic
 * {@link PartitionStripedExecutor} kernel. These exercise the guarantees both consumers rely on: stable
 * partition-to-stripe mapping, per-stripe FIFO, cross-stripe progress, bounded blocking admission (never
 * caller-runs, never drops), interrupt handling, shutdown semantics, and a single shared await deadline.
 */
public class PartitionStripedExecutorTest {
  private static final int AWAIT_SECONDS = 10;
  private static final long NEGATIVE_CHECK_MS = 300;

  @Test
  public void stripeMappingIsDeterministicAndNonNegative() {
    PartitionStripedExecutor executor = new PartitionStripedExecutor(4, 10, "map-test");
    try {
      assertEquals(executor.getStripeCount(), 4);
      // Integer.MIN_VALUE must not produce a negative index (Math.abs(MIN_VALUE) is still negative).
      assertEquals(executor.stripeFor(Integer.MIN_VALUE), 0);
      for (int partition = -1000; partition <= 1000; partition++) {
        int stripe = executor.stripeFor(partition);
        assertTrue(stripe >= 0 && stripe < 4, "stripe out of range for partition " + partition);
        // Deterministic: same partition always maps to the same stripe.
        assertEquals(executor.stripeFor(partition), stripe);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void samePartitionPreservesFifoOrder() throws InterruptedException {
    PartitionStripedExecutor executor = new PartitionStripedExecutor(4, 100, "fifo-test");
    CountDownLatch gate = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(50);
    List<Integer> order = new CopyOnWriteArrayList<>();
    try {
      for (int i = 0; i < 50; i++) {
        int value = i;
        executor.submit(7, () -> {
          awaitQuietly(gate);
          order.add(value);
          done.countDown();
        });
      }
      gate.countDown();
      assertTrue(done.await(AWAIT_SECONDS, TimeUnit.SECONDS), "tasks did not finish");
      for (int i = 0; i < 50; i++) {
        assertEquals(order.get(i).intValue(), i, "FIFO order violated at index " + i);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void blockedPartitionDoesNotStallOtherStripesButStaysFifo() throws InterruptedException {
    PartitionStripedExecutor executor = new PartitionStripedExecutor(2, 100, "cross-test");
    CountDownLatch blockStripe0 = new CountDownLatch(1);
    CountDownLatch stripe0Started = new CountDownLatch(1);
    CountDownLatch otherStripeDone = new CountDownLatch(1);
    CountDownLatch samePartitionLaterDone = new CountDownLatch(1);
    try {
      // Partition 0 -> stripe 0: hold it.
      executor.submit(0, () -> {
        stripe0Started.countDown();
        awaitQuietly(blockStripe0);
      });
      assertTrue(stripe0Started.await(AWAIT_SECONDS, TimeUnit.SECONDS));

      // A later task on the SAME partition must remain FIFO-blocked behind the held task.
      executor.submit(0, samePartitionLaterDone::countDown);

      // Partition 1 -> stripe 1: must make progress while stripe 0 is blocked.
      executor.submit(1, otherStripeDone::countDown);

      assertTrue(otherStripeDone.await(AWAIT_SECONDS, TimeUnit.SECONDS), "different stripe was stalled");
      assertFalse(
          samePartitionLaterDone.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS),
          "same-partition task ran out of FIFO order");

      blockStripe0.countDown();
      assertTrue(samePartitionLaterDone.await(AWAIT_SECONDS, TimeUnit.SECONDS), "same-partition task never ran");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void fullQueueBlocksSubmitterWithoutDroppingOrCallerRunning() throws InterruptedException {
    // One stripe, capacity one: worker busy + one queued fills capacity, so the next admission must block.
    PartitionStripedExecutor executor = new PartitionStripedExecutor(1, 1, "backpressure-test");
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch allRan = new CountDownLatch(3);
    ConcurrentLinkedQueue<String> runnerThreads = new ConcurrentLinkedQueue<>();
    AtomicBoolean thirdTaskRan = new AtomicBoolean(false);
    String callerThreadName = "backpressure-submitter";
    try {
      executor.submit(0, () -> {
        workerStarted.countDown();
        awaitQuietly(release);
        runnerThreads.add(Thread.currentThread().getName());
        allRan.countDown();
      });
      assertTrue(workerStarted.await(AWAIT_SECONDS, TimeUnit.SECONDS));

      // Fills the single queue slot.
      executor.submit(0, () -> {
        runnerThreads.add(Thread.currentThread().getName());
        allRan.countDown();
      });
      assertEquals(executor.getTotalQueueSize(), 1);

      CountDownLatch admitted = new CountDownLatch(1);
      Thread submitter = new Thread(() -> {
        executor.submit(0, () -> {
          thirdTaskRan.set(true);
          runnerThreads.add(Thread.currentThread().getName());
          allRan.countDown();
        });
        admitted.countDown();
      }, callerThreadName);
      submitter.start();

      // The submitter must be blocked in admission: not admitted, and the task must NOT run on the caller.
      assertFalse(admitted.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS), "submit did not block on a full queue");
      assertFalse(thirdTaskRan.get(), "task ran while admission was supposedly blocked (caller-run/drop)");

      release.countDown();
      assertTrue(admitted.await(AWAIT_SECONDS, TimeUnit.SECONDS), "blocked submit never completed after drain");
      submitter.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));

      // No drop: all three tasks ran, and every one ran on a worker thread (never the caller).
      assertTrue(allRan.await(AWAIT_SECONDS, TimeUnit.SECONDS), "not all tasks ran");
      for (String name: runnerThreads) {
        assertTrue(name.startsWith("backpressure-test"), "task ran off the worker thread pool: " + name);
        assertFalse(name.equals(callerThreadName), "task ran on the calling thread");
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void interruptedAdmissionThrowsAndRestoresInterruptFlag() throws InterruptedException {
    PartitionStripedExecutor executor = new PartitionStripedExecutor(1, 1, "interrupt-test");
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    AtomicBoolean interruptFlag = new AtomicBoolean(false);
    CountDownLatch finished = new CountDownLatch(1);
    try {
      executor.submit(0, () -> {
        workerStarted.countDown();
        awaitQuietly(release);
      });
      assertTrue(workerStarted.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      executor.submit(0, () -> {}); // fill queue

      Thread submitter = new Thread(() -> {
        try {
          executor.submit(0, () -> {});
        } catch (Throwable t) {
          thrown.set(t);
          interruptFlag.set(Thread.currentThread().isInterrupted());
        } finally {
          finished.countDown();
        }
      }, "interrupt-submitter");
      submitter.start();
      // Confirm the submitter is parked in blocking admission, then interrupt it.
      assertFalse(finished.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS));
      submitter.interrupt();

      assertTrue(finished.await(AWAIT_SECONDS, TimeUnit.SECONDS), "interrupted submit never returned");
      assertNotNull(thrown.get(), "interrupted admission did not throw");
      assertTrue(thrown.get() instanceof RuntimeException, "expected RuntimeException, got " + thrown.get());
      assertTrue(interruptFlag.get(), "interrupt flag was not restored");
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void shutdownWakesBlockedSubmitter() throws InterruptedException {
    PartitionStripedExecutor executor = new PartitionStripedExecutor(1, 1, "shutdown-wake-test");
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    CountDownLatch finished = new CountDownLatch(1);
    try {
      executor.submit(0, () -> {
        workerStarted.countDown();
        awaitQuietly(release);
      });
      assertTrue(workerStarted.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      executor.submit(0, () -> {}); // fill queue

      Thread submitter = new Thread(() -> {
        try {
          executor.submit(0, () -> {});
        } catch (Throwable t) {
          thrown.set(t);
        } finally {
          finished.countDown();
        }
      }, "shutdown-wake-submitter");
      submitter.start();
      assertFalse(finished.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS), "submitter should be blocked");

      executor.shutdown();
      assertTrue(finished.await(AWAIT_SECONDS, TimeUnit.SECONDS), "shutdown did not wake blocked submitter");
      assertNotNull(thrown.get(), "blocked submit should fail once shut down");
      assertTrue(thrown.get() instanceof RuntimeException);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void shutdownNowReturnsOwnershipOfQueuedTasks() throws InterruptedException {
    PartitionStripedExecutor executor = new PartitionStripedExecutor(1, 100, "shutdownnow-test");
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    AtomicInteger queuedTaskRuns = new AtomicInteger(0);
    try {
      executor.submit(0, () -> {
        workerStarted.countDown();
        awaitQuietly(release);
      });
      assertTrue(workerStarted.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      for (int i = 0; i < 5; i++) {
        executor.submit(0, queuedTaskRuns::incrementAndGet);
      }

      List<Runnable> drained = executor.shutdownNow();
      release.countDown();
      // The five queued tasks are handed back to the caller and must not have executed.
      assertEquals(drained.size(), 5, "shutdownNow did not return the queued tasks");
      assertEquals(queuedTaskRuns.get(), 0, "queued tasks ran despite shutdownNow ownership transfer");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void shutdownNowRejectsBlockedSubmitterInsteadOfStrandingItInTheDrainedQueue() throws InterruptedException {
    // Regression: a submitter blocked in admission must not win the offer race after shutdownNow() has
    // already drained the queue (which would strand its task, neither run nor returned to the caller).
    PartitionStripedExecutor executor = new PartitionStripedExecutor(1, 1, "shutdownnow-race-test");
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    AtomicBoolean rejectedTaskRan = new AtomicBoolean(false);
    CountDownLatch finished = new CountDownLatch(1);
    try {
      executor.submit(0, () -> {
        workerStarted.countDown();
        awaitQuietly(release);
      });
      assertTrue(workerStarted.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      executor.submit(0, () -> {}); // fill the single queue slot so the next submit blocks in admission

      Thread submitter = new Thread(() -> {
        try {
          executor.submit(0, () -> rejectedTaskRan.set(true));
        } catch (Throwable t) {
          thrown.set(t);
        } finally {
          finished.countDown();
        }
      }, "shutdownnow-race-submitter");
      submitter.start();
      assertFalse(finished.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS), "submitter should block on the full queue");

      // shutdownNow drains the filler; the blocked submitter then wins the freed slot, but the post-offer
      // shutdown recheck must pull the task back out and reject it rather than leave it stranded.
      executor.shutdownNow();
      release.countDown();
      assertTrue(finished.await(AWAIT_SECONDS, TimeUnit.SECONDS), "shutdownNow did not wake the blocked submitter");
      assertNotNull(thrown.get(), "blocked submitter must be rejected, not silently enqueued after shutdownNow");
      assertTrue(thrown.get() instanceof RejectedExecutionException, "expected rejection, got " + thrown.get());
      assertFalse(rejectedTaskRan.get(), "a task rejected after shutdownNow must never run");
      submitter.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void awaitTerminationUsesOneSharedDeadline() throws InterruptedException {
    PartitionStripedExecutor executor = new PartitionStripedExecutor(4, 10, "deadline-test");
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(4);
    try {
      for (int stripe = 0; stripe < 4; stripe++) {
        executor.executeOnStripe(stripe, () -> {
          started.countDown();
          awaitQuietly(release);
        });
      }
      assertTrue(started.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      executor.shutdown();

      long startNanos = System.nanoTime();
      boolean terminated = executor.awaitTermination(500, TimeUnit.MILLISECONDS);
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

      assertFalse(terminated, "should not terminate while tasks are blocked");
      // A shared deadline waits ~500ms total, NOT 4 x 500ms; allow generous slack but well under the naive sum.
      assertTrue(elapsedMillis < 1500, "await did not share one deadline across stripes: " + elapsedMillis + "ms");
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void executeOnStripeTargetsExactStripe() throws InterruptedException {
    PartitionStripedExecutor executor = new PartitionStripedExecutor(3, 10, "exact-stripe-test");
    AtomicReference<String> threadName = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(1);
    try {
      executor.executeOnStripe(2, () -> {
        threadName.set(Thread.currentThread().getName());
        done.countDown();
      });
      assertTrue(done.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      assertTrue(threadName.get().contains("exact-stripe-test-2"), "ran on wrong stripe: " + threadName.get());
    } finally {
      executor.shutdownNow();
    }
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      if (!latch.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
        fail("latch was not released within timeout");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
