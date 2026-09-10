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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;


/**
 * Deterministic tests for {@link VeniceSystemProducerWriteDispatcher} with a latch-blockable mocked
 * {@link AbstractVeniceWriter}; kernel-level guarantees are covered in {@code PartitionStripedExecutorTest}.
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
      VeniceSystemProducerWriteCommand.DurableWriteFuture first0 = dispatcher.dispatch(putCommand(0));
      assertTrue(partition0Entered.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      VeniceSystemProducerWriteCommand.DurableWriteFuture second0 = dispatcher.dispatch(putCommand(0));
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

      assertSame(expectVeniceException(dispatcher::flush).getCause(), boom);
      assertSame(expectVeniceException(() -> dispatcher.dispatch(putCommand(0))).getCause(), boom);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void partitionRoutingFailureBecomesStickyAndSurfaces() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    RuntimeException boom = new RuntimeException("partition routing failure");
    when(writer.getPartitionId(any())).thenThrow(boom);

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = dispatcher.dispatch(putCommand(0));
      assertSame(expectCause(durable.getSubmissionFuture()), boom);
      assertSame(expectCause(durable), boom);
      verify(writer, never()).put(any(), any(), anyInt(), anyLong(), any());

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

      CountDownLatch retryDispatched = new CountDownLatch(1);
      durable.whenComplete((v, t) -> {
        dispatcher.dispatch(putCommand(0));
        retryDispatched.countDown();
      });

      Thread flusher = new Thread(dispatcher::flush);
      flusher.start();
      assertTrue(flushEntered.await(AWAIT_SECONDS, TimeUnit.SECONDS), "flush must reach writer.flush");

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
    assertFalse(stopReturned.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS), "stop must wait for the active worker");
    assertFalse(forcedInterrupt.get(), "stop must not force-interrupt the active worker");

    releaseWorker.countDown();
    assertTrue(stopReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS), "stop must return once workers terminate");
    stopper.join();

    queued.getSubmissionFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS);
    assertEquals(putCount.get(), 2, "the queued write must be drained, not dropped");
    assertFalse(forcedInterrupt.get());
    verify(writer, never()).close(anyBoolean());
  }

  @Test
  public void synchronousWriterErrorCompletesFuturesStickyAndRethrowsOnWorker() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    FatalTestError fatal = new FatalTestError();
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenThrow(fatal);

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = dispatcher.dispatch(putCommand(0));
      assertSame(expectCause(durable.getSubmissionFuture()), fatal);
      assertSame(expectCause(durable), fatal);
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

    Thread stopper = new Thread(dispatcher::stop);
    stopper.start();
    assertTrue(thirdReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS), "stop must wake the blocked submitter");
    assertTrue(expectCause(third.get().getSubmissionFuture()) instanceof RuntimeException);

    releaseWorker.countDown();
    stopper.join();
    blocked.join();
  }

  @Test
  public void kernelRejectionRacingStopFailsCleanStoppedNotSticky() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch routingEntered = new CountDownLatch(1);
    CountDownLatch releaseRouting = new CountDownLatch(1);
    AtomicBoolean firstRouting = new AtomicBoolean(true);
    when(writer.getPartitionId(any())).thenAnswer(invocation -> {
      if (firstRouting.compareAndSet(true, false)) {
        routingEntered.countDown();
        assertTrue(releaseRouting.await(AWAIT_SECONDS, TimeUnit.SECONDS));
      }
      return 0;
    });

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    AtomicReference<VeniceSystemProducerWriteCommand.DurableWriteFuture> raced = new AtomicReference<>();
    CountDownLatch racedReturned = new CountDownLatch(1);
    Thread racer = new Thread(() -> {
      raced.set(dispatcher.dispatch(putCommand(0)));
      racedReturned.countDown();
    });
    try {
      racer.start();
      assertTrue(routingEntered.await(AWAIT_SECONDS, TimeUnit.SECONDS), "raced command must park in routing");

      dispatcher.stop();

      releaseRouting.countDown();
      assertTrue(racedReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS), "raced dispatch must return, not hang");

      Throwable racedCause = expectCause(raced.get().getSubmissionFuture());
      assertTrue(racedCause instanceof VeniceException, "raced failure must be a VeniceException, was: " + racedCause);
      assertEquals(racedCause.getMessage(), "VeniceSystemProducer write dispatcher is stopped");
      assertTrue(
          racedCause.getCause() instanceof RejectedExecutionException,
          "clean stopped rejection must carry the kernel RejectedExecutionException as its cause");
      verify(writer, never()).put(any(), any(), anyInt(), anyLong(), any());

      VeniceSystemProducerWriteCommand.DurableWriteFuture later = dispatcher.dispatch(putCommand(0));
      Throwable laterCause = expectCause(later.getSubmissionFuture());
      assertTrue(laterCause instanceof VeniceException, "later failure must be a VeniceException, was: " + laterCause);
      assertEquals(laterCause.getMessage(), "VeniceSystemProducer write dispatcher is stopped");
      assertNull(laterCause.getCause(), "clean stopped path must not carry a sticky cause");
    } finally {
      releaseRouting.countDown();
      racer.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
      dispatcher.stop();
    }
  }

  @Test
  public void synchronousCallbackBeforeSubmissionRunsDurableContinuationOffWorker() throws Exception {
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
      VeniceSystemProducerWriteCommand command = putCommand(0);
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = command.getDurableFuture();
      // Register before dispatch so a fast completion cannot move the continuation onto the test thread.
      durable.whenComplete((v, t) -> {
        continuationThread.set(Thread.currentThread());
        dispatcher.stop();
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
      VeniceSystemProducerWriteCommand command = putCommand(0);
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = command.getDurableFuture();
      // Register before dispatch so a fast completion cannot move the continuation onto the test thread.
      durable.whenComplete((v, t) -> {
        continuationThread.set(Thread.currentThread());
        try {
          dispatcher.flush();
        } catch (VeniceException expected) {
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
    assertFalse(
        stopReturned.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS),
        "stop must keep draining losslessly after an interrupt");

    releaseWorker.countDown();
    assertTrue(stopReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS), "stop must return once the worker terminates");
    stopper.join();

    assertTrue(observedInterrupt.get(), "stop must report that it observed the interrupt");
    assertFalse(interruptStillSet.get(), "stop must not re-assert the interrupt on the draining thread");
  }

  @Test
  public void routingErrorSettlesFuturesStickyAndRethrowsToCaller() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    FatalTestError fatal = new FatalTestError();
    when(writer.getPartitionId(any())).thenThrow(fatal);

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      VeniceSystemProducerWriteCommand command = putCommand(0);
      try {
        dispatcher.dispatch(command);
        fail("dispatch must rethrow the routing Error");
      } catch (FatalTestError thrown) {
        assertSame(thrown, fatal, "dispatch must rethrow the identical Error, not a copy or wrapper");
      }

      assertSame(expectCause(command.getDurableFuture().getSubmissionFuture()), fatal);
      assertSame(expectCause(command.getDurableFuture()), fatal);
      verify(writer, never()).put(any(), any(), anyInt(), anyLong(), any());
      assertSame(expectVeniceException(dispatcher::flush).getCause(), fatal);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void writerInternalCallbackIsChainedBeforeDurableCompletion() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    when(writer.getPartitionId(any())).thenReturn(0);

    AtomicReference<PubSubProducerCallback> suppliedCallback = new AtomicReference<>();
    AtomicReference<CompletableFuture<Void>> durableRef = new AtomicReference<>();
    AtomicInteger internalCallbackCount = new AtomicInteger();
    AtomicBoolean internalFiredBeforeDurable = new AtomicBoolean();
    AtomicReference<Exception> internalException = new AtomicReference<>();
    CountDownLatch submitted = new CountDownLatch(1);

    PubSubProducerCallback internalCallback = (result, exception) -> {
      internalCallbackCount.incrementAndGet();
      internalFiredBeforeDurable.set(!durableRef.get().isDone());
      internalException.set(exception);
    };

    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      PubSubProducerCallback supplied = invocation.getArgument(4);
      supplied.setInternalCallback(internalCallback);
      suppliedCallback.set(supplied);
      submitted.countDown();
      return null;
    });

    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, 4, 100, "s");
    try {
      VeniceSystemProducerWriteCommand command = putCommand(0);
      VeniceSystemProducerWriteCommand.DurableWriteFuture durable = dispatcher.dispatch(command);
      durableRef.set(durable);
      assertTrue(submitted.await(AWAIT_SECONDS, TimeUnit.SECONDS), "worker must invoke the writer");

      durable.getSubmissionFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS);
      assertFalse(durable.isDone(), "durable must remain pending until the writer callback arrives");
      assertEquals(internalCallbackCount.get(), 0, "internal callback must not fire before the writer completes it");

      VeniceException boom = new VeniceException("async writer failure");
      suppliedCallback.get().onCompletion(null, boom);

      assertEquals(internalCallbackCount.get(), 1, "writer internal callback must be chained and fire exactly once");
      assertTrue(internalFiredBeforeDurable.get(), "internal callback must fire before durable completion");
      assertSame(internalException.get(), boom, "internal callback must receive the exact exception");
      assertSame(expectCause(durable), boom, "durable future must be settled with the callback failure");
      assertSame(expectVeniceException(dispatcher::flush).getCause(), boom, "callback failure must become sticky");
    } finally {
      dispatcher.stop();
    }
  }

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

  private static final class FatalTestError extends Error {
    private static final long serialVersionUID = 1L;
  }
}
