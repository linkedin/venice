package com.linkedin.venice.samza;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;


/**
 * Deterministic tests for {@link VeniceSystemProducerWriteDispatcher} using a mocked
 * {@link AbstractVeniceWriter} whose {@code put}/{@code flush} can be blocked with latches. These prove the
 * dispatch contract the {@link VeniceSystemProducer} STREAM path relies on: dispatch never waits for the
 * writer; the same Venice partition keeps FIFO order while a different partition (different stripe) makes
 * progress; synchronous and asynchronous writer failures become sticky; flush is a fence that excludes new
 * admissions and then flushes the writer; and stop drains workers without closing the writer and cannot
 * deadlock a submitter blocked on a full queue. Kernel-level guarantees (striping, bounded admission, shared
 * await) are covered separately in {@code PartitionStripedExecutorTest}.
 */
public class VeniceSystemProducerWriteDispatcherTest {
  private static final int AWAIT_SECONDS = 10;
  private static final long NEGATIVE_CHECK_MS = 300;

  @SuppressWarnings("unchecked")
  private static AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter() {
    return mock(AbstractVeniceWriter.class);
  }

  private static VeniceSystemProducerWriteCommand putCommand(int partition) {
    return VeniceSystemProducerWriteCommand.put(new byte[] { (byte) partition }, new byte[] { 9 }, 1, 0L);
  }

  @Test
  public void dispatchReturnsBeforeWriterAndDurableCompletesOnCallback() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch putEntered = new CountDownLatch(1);
    CountDownLatch releasePut = new CountDownLatch(1);
    AtomicReference<PubSubProducerCallback> callbackRef = new AtomicReference<>();
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      callbackRef.set(invocation.getArgument(4));
      putEntered.countDown();
      assertTrue(releasePut.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      return null;
    });

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = dispatcher.dispatch(putCommand(0));
      // dispatch returned even though the worker is parked inside writer.put.
      assertTrue(putEntered.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      CompletableFuture<Void> submission = durable.getSubmissionFuture();
      assertFalse(submission.isDone(), "submission must not complete until writer.put returns");
      assertFalse(durable.isDone());

      releasePut.countDown();
      submission.get(AWAIT_SECONDS, TimeUnit.SECONDS);
      assertFalse(durable.isDone(), "durable must wait for the asynchronous callback");

      callbackRef.get().onCompletion(null, null);
      durable.get(AWAIT_SECONDS, TimeUnit.SECONDS);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void differentPartitionProgressesWhileSamePartitionStaysFifo() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch partition0Entered = new CountDownLatch(1);
    CountDownLatch releasePartition0 = new CountDownLatch(1);
    CountDownLatch partition1Reached = new CountDownLatch(1);
    AtomicInteger partition0PutCount = new AtomicInteger();
    when(writer.getPartitionId(any())).thenAnswer(invocation -> (int) ((byte[]) invocation.getArgument(0))[0]);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      int partition = (int) ((byte[]) invocation.getArgument(0))[0];
      if (partition == 0) {
        if (partition0PutCount.getAndIncrement() == 0) {
          partition0Entered.countDown();
          assertTrue(releasePartition0.await(AWAIT_SECONDS, TimeUnit.SECONDS));
        }
      } else {
        partition1Reached.countDown();
      }
      return null;
    });

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      // Partition 0 -> stripe 0 blocks; a later partition-0 record must stay FIFO-blocked behind it.
      VeniceSystemProducerWriteCommand.DurableWriteFuture first0 = dispatcher.dispatch(putCommand(0));
      assertTrue(partition0Entered.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      VeniceSystemProducerWriteCommand.DurableWriteFuture second0 = dispatcher.dispatch(putCommand(0));
      // Partition 1 -> stripe 1 must reach the writer despite stripe 0 being blocked.
      dispatcher.dispatch(putCommand(1));
      assertTrue(partition1Reached.await(AWAIT_SECONDS, TimeUnit.SECONDS), "different stripe must make progress");

      assertEquals(partition0PutCount.get(), 1, "second same-partition record must not run yet");
      assertFalse(second0.getSubmissionFuture().isDone());

      releasePartition0.countDown();
      first0.getSubmissionFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS);
      second0.getSubmissionFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS);
      assertEquals(partition0PutCount.get(), 2);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void synchronousWriterFailureBecomesStickyAndSurfaces() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    VeniceException boom = new VeniceException("synchronous put failure");
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenThrow(boom);

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = dispatcher.dispatch(putCommand(0));
      assertSame(expectCause(durable.getSubmissionFuture()), boom);
      assertSame(expectCause(durable), boom);

      // Sticky failure is surfaced by both flush and a subsequent dispatch.
      assertSame(expectVeniceException(dispatcher::flush).getCause(), boom);
      assertSame(expectVeniceException(() -> dispatcher.dispatch(putCommand(0))).getCause(), boom);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void asynchronousCallbackFailureBecomesStickyAndSurfaces() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    AtomicReference<PubSubProducerCallback> callbackRef = new AtomicReference<>();
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      callbackRef.set(invocation.getArgument(4));
      return null;
    });

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = dispatcher.dispatch(putCommand(0));
      durable.getSubmissionFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS);

      VeniceException boom = new VeniceException("broker failure");
      callbackRef.get().onCompletion(null, boom);
      assertSame(expectCause(durable), boom);
      assertSame(expectVeniceException(dispatcher::flush).getCause(), boom);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void flushWaitsForPreFenceWritesThenFlushesWriter() throws Exception {
    // Flush is a global pre-fence durability boundary: every write admitted before the fence must reach the
    // writer before writer.flush() runs. A pre-fence record whose worker is parked inside writer.put holds the
    // fence marker on its stripe, so flush cannot reach writer.flush() until that write returns.
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch putEntered = new CountDownLatch(1);
    CountDownLatch releasePut = new CountDownLatch(1);
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      putEntered.countDown();
      assertTrue(releasePut.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      return null;
    });

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      dispatcher.dispatch(putCommand(0));
      assertTrue(putEntered.await(AWAIT_SECONDS, TimeUnit.SECONDS), "pre-fence write must reach the writer");

      CountDownLatch flushReturned = new CountDownLatch(1);
      Thread flusher = new Thread(() -> {
        dispatcher.flush();
        flushReturned.countDown();
      });
      flusher.start();
      // Flush must not reach writer.flush() while the pre-fence write is still parked in writer.put.
      assertFalse(flushReturned.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS), "flush must wait for pre-fence write");
      verify(writer, never()).flush();

      releasePut.countDown();
      assertTrue(flushReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS), "flush must return once pre-fence write drains");
      flusher.join();
      verify(writer, atLeastOnce()).flush();
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void flushDoesNotDeadlockWithCallbackRetryContinuation() throws Exception {
    // Regression: a broker callback continuation that re-dispatches (a retry) must not deadlock against a
    // concurrent flush. Because flush releases the admission write lock before awaiting markers and calling
    // writer.flush(), the callback thread can acquire the read lock and admit the retry while flush waits.
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    AtomicReference<PubSubProducerCallback> callbackRef = new AtomicReference<>();
    CountDownLatch flushEntered = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      callbackRef.set(invocation.getArgument(4));
      return null;
    });
    doAnswer(invocation -> {
      flushEntered.countDown();
      assertTrue(releaseFlush.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      return null;
    }).when(writer).flush();

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = dispatcher.dispatch(putCommand(0));
      durable.getSubmissionFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS);

      // When the broker callback completes the durable future, re-dispatch a retry on the callback thread.
      CountDownLatch retryDispatched = new CountDownLatch(1);
      durable.whenComplete((v, t) -> {
        dispatcher.dispatch(putCommand(0));
        retryDispatched.countDown();
      });

      Thread flusher = new Thread(dispatcher::flush);
      flusher.start();
      assertTrue(flushEntered.await(AWAIT_SECONDS, TimeUnit.SECONDS), "flush must reach writer.flush");

      // Fire the callback on this thread; its retry continuation must admit without blocking on flush.
      callbackRef.get().onCompletion(null, null);
      assertTrue(retryDispatched.await(AWAIT_SECONDS, TimeUnit.SECONDS), "callback retry must not deadlock with flush");

      releaseFlush.countDown();
      flusher.join();
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void stopDrainsActiveWorkerLosslesslyAndDoesNotForceInterrupt() throws Exception {
    // Stop must drain every accepted task without force-interrupting an active worker: it wakes blocked
    // admissions via kernel shutdown, then waits until workers actually terminate. A queued task behind a
    // parked worker must still run, and its future must not be stranded.
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch workerEntered = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    AtomicInteger putCount = new AtomicInteger();
    AtomicBoolean forcedInterrupt = new AtomicBoolean();
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      if (putCount.getAndIncrement() == 0) {
        workerEntered.countDown();
        try {
          assertTrue(releaseWorker.await(AWAIT_SECONDS, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          forcedInterrupt.set(true);
          Thread.currentThread().interrupt();
          throw new VeniceException("worker was force-interrupted");
        }
      }
      return null;
    });

    // One worker, capacity 100: first record is in-flight (parked), second is queued behind it.
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 1, 100, "s");
    dispatcher.dispatch(putCommand(0));
    assertTrue(workerEntered.await(AWAIT_SECONDS, TimeUnit.SECONDS));
    VeniceSystemProducerWriteCommand.DurableWriteFuture queued = dispatcher.dispatch(putCommand(0));

    CountDownLatch stopReturned = new CountDownLatch(1);
    Thread stopper = new Thread(() -> {
      dispatcher.stop();
      stopReturned.countDown();
    });
    stopper.start();
    // Stop must not return while the worker is still active, and must not force-interrupt it.
    assertFalse(stopReturned.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS), "stop must wait for the active worker");
    assertFalse(forcedInterrupt.get(), "stop must not force-interrupt the active worker");

    releaseWorker.countDown();
    assertTrue(stopReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS), "stop must return once workers terminate");
    stopper.join();

    // The queued task ran to completion (lossless drain) and its future is not stranded.
    queued.getSubmissionFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS);
    assertEquals(putCount.get(), 2, "the queued write must be drained, not dropped");
    assertFalse(forcedInterrupt.get());
    verify(writer, never()).close(anyBoolean());
  }

  @Test
  public void synchronousWriterErrorCompletesFuturesStickyAndRethrowsOnWorker() throws Exception {
    // A fatal Error thrown synchronously by the writer must still complete submission+durable exceptionally
    // with the Error identity preserved and become sticky, then be rethrown on the worker thread.
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    FatalTestError fatal = new FatalTestError();
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenThrow(fatal);

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = dispatcher.dispatch(putCommand(0));
      assertSame(expectCause(durable.getSubmissionFuture()), fatal);
      assertSame(expectCause(durable), fatal);
      // The Error is sticky and surfaces on a subsequent flush.
      assertSame(expectVeniceException(dispatcher::flush).getCause(), fatal);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void stopDrainsWorkersWithoutClosingWriterAndIsIdempotent() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenReturn(null);

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    VeniceSystemProducerWriteCommand.DurableWriteFuture durable = dispatcher.dispatch(putCommand(0));
    durable.getSubmissionFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS);

    dispatcher.stop();
    dispatcher.stop(); // idempotent

    verify(writer, never()).close(anyBoolean());

    // After stop, admissions are rejected and their submission fails fast.
    VeniceSystemProducerWriteCommand.DurableWriteFuture rejected = dispatcher.dispatch(putCommand(0));
    assertTrue(expectCause(rejected.getSubmissionFuture()) instanceof VeniceException);
  }

  @Test
  public void stopWakesSubmitterBlockedOnFullQueueWithoutDeadlock() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch workerEntered = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    AtomicInteger putCount = new AtomicInteger();
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      if (putCount.getAndIncrement() == 0) {
        workerEntered.countDown();
        assertTrue(releaseWorker.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      }
      return null;
    });

    // One worker, queue capacity one: one in-flight + one queued fills capacity, a third admission blocks.
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 1, 1, "s");
    dispatcher.dispatch(putCommand(0));
    assertTrue(workerEntered.await(AWAIT_SECONDS, TimeUnit.SECONDS));
    dispatcher.dispatch(putCommand(0)); // fills the single queue slot

    CountDownLatch thirdReturned = new CountDownLatch(1);
    AtomicReference<VeniceSystemProducerWriteCommand.DurableWriteFuture> third = new AtomicReference<>();
    Thread blocked = new Thread(() -> {
      third.set(dispatcher.dispatch(putCommand(0)));
      thirdReturned.countDown();
    });
    blocked.start();
    assertFalse(
        thirdReturned.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS),
        "third admission must block on full queue");

    // stop() shuts the kernel down before taking any lock, so the blocked submitter is woken and fails fast.
    Thread stopper = new Thread(dispatcher::stop);
    stopper.start();
    assertTrue(thirdReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS), "stop must wake the blocked submitter");
    assertTrue(expectCause(third.get().getSubmissionFuture()) instanceof RuntimeException);

    releaseWorker.countDown();
    stopper.join();
    blocked.join();
  }

  @Test
  public void synchronousCallbackBeforeSubmissionRunsDurableContinuationOffWorker() throws Exception {
    // A callback that fires synchronously (inside writer.put, before submission returns) must not complete the
    // durable future on the stripe worker: a caller continuation (here calling stop()) would otherwise run
    // inline on the worker and self-deadlock the drain. The dispatcher hands the durable completion off the
    // worker onto its VSP-owned completion pool, so the continuation runs on a VSP thread and stop() completes.
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    AtomicReference<Thread> workerThread = new AtomicReference<>();
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      workerThread.set(Thread.currentThread());
      PubSubProducerCallback callback = invocation.getArgument(4);
      callback.onCompletion(null, null); // synchronous callback, before put returns
      return null;
    });

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 1, 100, "s");
    try {
      AtomicReference<Thread> continuationThread = new AtomicReference<>();
      CountDownLatch stopReturned = new CountDownLatch(1);
      // Attach the continuation to the durable future BEFORE dispatch so the worker cannot complete it (and
      // run the continuation) before whenComplete is registered — otherwise the continuation could run on the
      // registering thread and the VSP-owned-thread assertion would be racy.
      VeniceSystemProducerWriteCommand command = putCommand(0);
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = command.getDurableFuture();
      durable.whenComplete((v, t) -> {
        continuationThread.set(Thread.currentThread());
        dispatcher.stop(); // a caller continuation that drains the very worker that would complete it inline
        stopReturned.countDown();
      });
      dispatcher.dispatch(command);

      durable.get(AWAIT_SECONDS, TimeUnit.SECONDS);
      assertTrue(
          stopReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS),
          "durable continuation calling stop() must not self-deadlock on the stripe worker");
      assertNotNull(workerThread.get());
      assertNotSame(
          continuationThread.get(),
          workerThread.get(),
          "durable continuation must not run on the stripe worker thread");
      assertVspOwnedCompletionThread(continuationThread.get());
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void synchronousFailureRunsDurableContinuationOffWorker() throws Exception {
    // A synchronous writer failure completes the durable future exceptionally; its continuation (here calling
    // flush()) must run off the stripe worker so it cannot self-deadlock against the worker it is draining.
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    AtomicReference<Thread> workerThread = new AtomicReference<>();
    VeniceException boom = new VeniceException("sync failure");
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      workerThread.set(Thread.currentThread());
      throw boom;
    });

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 1, 100, "s");
    try {
      AtomicReference<Thread> continuationThread = new AtomicReference<>();
      CountDownLatch flushReturned = new CountDownLatch(1);
      // Attach the continuation to the durable future BEFORE dispatch so the worker cannot complete it (and
      // run the continuation) before whenComplete is registered.
      VeniceSystemProducerWriteCommand command = putCommand(0);
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = command.getDurableFuture();
      durable.whenComplete((v, t) -> {
        continuationThread.set(Thread.currentThread());
        try {
          dispatcher.flush(); // surfaces the sticky failure; we only care that it does not self-wait
        } catch (VeniceException expected) {
          // sticky failure is surfaced by flush
        }
        flushReturned.countDown();
      });
      dispatcher.dispatch(command);

      assertSame(expectCause(durable), boom);
      assertTrue(
          flushReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS),
          "durable continuation calling flush() must not self-wait on the stripe worker");
      assertNotSame(
          continuationThread.get(),
          workerThread.get(),
          "durable continuation must not run on the stripe worker thread");
      assertVspOwnedCompletionThread(continuationThread.get());
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void stopReportsInterruptWithoutReassertingIt() throws Exception {
    // When the draining thread is interrupted mid-drain, stop() keeps draining losslessly, reports the
    // interrupt via its return value, and does NOT re-assert the interrupt flag so the caller can run its own
    // interruptible writer cleanup with the flag clear.
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch workerEntered = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      workerEntered.countDown();
      assertTrue(releaseWorker.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      return null;
    });

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 1, 100, "s");
    dispatcher.dispatch(putCommand(0));
    assertTrue(workerEntered.await(AWAIT_SECONDS, TimeUnit.SECONDS));

    AtomicBoolean observedInterrupt = new AtomicBoolean();
    AtomicBoolean interruptStillSet = new AtomicBoolean(true);
    CountDownLatch stopReturned = new CountDownLatch(1);
    Thread stopper = new Thread(() -> {
      Thread.currentThread().interrupt(); // interrupted while draining
      observedInterrupt.set(dispatcher.stop());
      interruptStillSet.set(Thread.currentThread().isInterrupted());
      stopReturned.countDown();
    });
    stopper.start();
    // Stop must keep draining despite the interrupt; it cannot return until the worker is released.
    assertFalse(
        stopReturned.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS),
        "stop must keep draining losslessly after an interrupt");

    releaseWorker.countDown();
    assertTrue(stopReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS), "stop must return once the worker terminates");
    stopper.join();

    assertTrue(observedInterrupt.get(), "stop must report that it observed the interrupt");
    assertFalse(interruptStillSet.get(), "stop must not re-assert the interrupt on the draining thread");
  }

  /**
   * Asserts the durable continuation ran on a VSP-owned completion pool thread rather than the JDK common pool
   * (the old {@code CompletableFuture.runAsync} behavior). The completion pool names its daemon threads with the
   * {@code venice-samza-writer-completion-} prefix.
   */
  private static void assertVspOwnedCompletionThread(Thread continuationThread) {
    assertNotNull(continuationThread);
    String name = continuationThread.getName();
    assertTrue(
        name.startsWith("venice-samza-writer-completion-"),
        "durable continuation must run on a VSP-owned completion thread, but ran on: " + name);
    assertFalse(
        name.contains("ForkJoinPool") || name.contains("commonPool"),
        "durable continuation must not run on the JDK common pool, but ran on: " + name);
  }

  private static Throwable expectCause(CompletableFuture<Void> future) throws Exception {
    try {
      future.get(AWAIT_SECONDS, TimeUnit.SECONDS);
      fail("future should have completed exceptionally");
      return null;
    } catch (ExecutionException e) {
      return e.getCause();
    }
  }

  private static VeniceException expectVeniceException(Runnable action) {
    try {
      action.run();
      fail("expected a VeniceException");
      return null;
    } catch (VeniceException e) {
      return e;
    }
  }

  /** A distinct Error subtype so identity-preservation assertions cannot accidentally match. */
  private static final class FatalTestError extends Error {
    private static final long serialVersionUID = 1L;
  }
}
