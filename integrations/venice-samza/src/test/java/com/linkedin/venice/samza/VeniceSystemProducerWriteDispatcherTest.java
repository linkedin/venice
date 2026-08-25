package com.linkedin.venice.samza;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.PartitionedVeniceWriteExecutor;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceSystemProducerWriteDispatcherTest {
  @Test
  public void testFlushBlocksPostFenceAdmissionUntilWriterFlushReturns() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch flushEntered = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    doAnswer(invocation -> {
      flushEntered.countDown();
      await(releaseFlush);
      return null;
    }).when(writer).flush();
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 1, 0);

    CompletableFuture<Void> flush = CompletableFuture.runAsync(dispatcher::flush);
    assertTrue(flushEntered.await(5, TimeUnit.SECONDS));
    AtomicBoolean sendReturned = new AtomicBoolean(false);
    Thread sender = new Thread(() -> {
      dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, -1);
      sendReturned.set(true);
    });
    sender.start();
    Thread.sleep(200);
    assertFalse(sendReturned.get(), "Post-fence admission must wait for the core flush");

    releaseFlush.countDown();
    flush.get(5, TimeUnit.SECONDS);
    sender.join(TimeUnit.SECONDS.toMillis(5));
    assertTrue(sendReturned.get());
    dispatcher.stop();
  }

  @Test(timeOut = 5000)
  public void testStopTimesOutBehindBlockedFlushWithoutClosingWriter() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch flushEntered = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    AtomicBoolean flushActive = new AtomicBoolean(false);
    AtomicBoolean closeDuringFlush = new AtomicBoolean(false);
    doAnswer(invocation -> {
      flushActive.set(true);
      flushEntered.countDown();
      try {
        await(releaseFlush);
      } finally {
        flushActive.set(false);
      }
      return null;
    }).when(writer).flush();
    doAnswer(invocation -> {
      closeDuringFlush.set(flushActive.get());
      return null;
    }).when(writer).close();
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 10, 0, 10, "blocked-flush-store", null);
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(
        writer,
        executor,
        200,
        TimeUnit.MILLISECONDS,
        ForkJoinPool.commonPool());
    CompletableFuture<Void> flush = CompletableFuture.runAsync(dispatcher::flush);
    assertTrue(flushEntered.await(2, TimeUnit.SECONDS));

    try {
      CompletableFuture<Void> stop =
          CompletableFuture.runAsync(() -> assertThrows(VeniceException.class, dispatcher::stop));
      stop.get(2, TimeUnit.SECONDS);
      assertFalse(flush.isDone(), "The active writer flush must remain fenced");
      assertFalse(closeDuringFlush.get(), "Writer close must not race an active flush");
      verify(writer, never()).close();
    } finally {
      releaseFlush.countDown();
      assertThrows(ExecutionException.class, () -> flush.get(2, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testSubmissionCompletesOnWorkerBeforeConfiguredCallback() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    CountDownLatch writeEntered = new CountDownLatch(1);
    CountDownLatch releaseWriter = new CountDownLatch(1);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      writeEntered.countDown();
      await(releaseWriter);
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 1, 1);

    CompletableFuture<Void> durable = dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, -1);
    assertTrue(writeEntered.await(5, TimeUnit.SECONDS));
    CompletableFuture<Void> submission = (CompletableFuture<Void>) dispatcher.getSubmissionFuture(durable);
    CompletableFuture<String> submissionThread = new CompletableFuture<>();
    submission.whenComplete((ignored, failure) -> submissionThread.complete(Thread.currentThread().getName()));
    CompletableFuture<String> durableThread = new CompletableFuture<>();
    durable.whenComplete((ignored, failure) -> {
      assertTrue(submission.isDone(), "Durability must never complete before worker submission");
      durableThread.complete(Thread.currentThread().getName());
    });

    releaseWriter.countDown();
    submission.get(5, TimeUnit.SECONDS);
    assertFalse(durable.isDone());
    callback.get().onCompletion(null, null);

    durable.get(5, TimeUnit.SECONDS);
    assertTrue(
        submissionThread.get(5, TimeUnit.SECONDS).contains("venice-system-producer-worker"),
        "The private submission phase must complete directly on the worker");
    assertTrue(
        durableThread.get(5, TimeUnit.SECONDS).contains("venice-system-producer-callback"),
        "Normal durable completion must honor the configured callback executor");
    dispatcher.stop();
  }

  @Test
  public void testCallbackThreadCountZeroCompletesOnPubSubThread() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 1, 0);
    CompletableFuture<Void> durable = dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, -1);
    dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);
    CompletableFuture<String> completionThread = new CompletableFuture<>();
    durable.whenComplete((ignored, failure) -> completionThread.complete(Thread.currentThread().getName()));

    Thread pubSubThread = new Thread(() -> callback.get().onCompletion(null, null), "test-pubsub-callback");
    pubSubThread.start();
    pubSubThread.join(TimeUnit.SECONDS.toMillis(5));

    assertEquals(completionThread.get(5, TimeUnit.SECONDS), "test-pubsub-callback");
    dispatcher.stop();
  }

  @Test
  public void testSaturatedCommonPoolDoesNotBlockSubmissionWait() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch handoffScheduled = new CountDownLatch(1);
    AtomicReference<Runnable> blockedHandoff = new AtomicReference<>();
    Executor saturatedCommonPool = task -> {
      blockedHandoff.set(task);
      handoffScheduled.countDown();
    };
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      PubSubProducerCallback callback = invocation.getArgument(4);
      callback.onCompletion(null, null);
      return new CompletableFuture<>();
    });
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 10, 0, 10, "saturated-common-pool", null);
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, executor, 60, TimeUnit.SECONDS, saturatedCommonPool);

    CompletableFuture<Void> durable = dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, -1);
    dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);
    assertTrue(handoffScheduled.await(5, TimeUnit.SECONDS));
    assertFalse(durable.isDone(), "Caller-visible durability must remain on the blocked handoff");

    blockedHandoff.get().run();
    durable.get(5, TimeUnit.SECONDS);
    dispatcher.stop();
  }

  @Test
  public void testRejectedCallbackAdmissionUsesNonblockingHandoff() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CallbackCompletionScenario rejectedAdmission = callbackCompletionScenario(writer, true);
    CompletableFuture<String> completionThread = new CompletableFuture<>();
    rejectedAdmission.durable
        .whenComplete((ignored, failure) -> completionThread.complete(Thread.currentThread().getName()));

    CompletableFuture<Void> callbackReturned = new CompletableFuture<>();
    Thread pubSubCallbackThread = new Thread(() -> {
      rejectedAdmission.callback.onCompletion(null, null);
      callbackReturned.complete(null);
    }, "test-pubsub-callback");
    pubSubCallbackThread.start();
    try {
      callbackReturned.get(5, TimeUnit.SECONDS);
      rejectedAdmission.durable.get(5, TimeUnit.SECONDS);
      String threadName = completionThread.get(5, TimeUnit.SECONDS);
      assertTrue(threadName.contains("ForkJoinPool.commonPool"));
      assertFalse(threadName.contains("venice-system-producer-callback"));
    } finally {
      pubSubCallbackThread.join(TimeUnit.SECONDS.toMillis(5));
      rejectedAdmission.dispatcher.stop();
    }
  }

  @DataProvider(name = "callbackCompletionPaths")
  public Object[][] callbackCompletionPaths() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "callbackCompletionPaths", timeOut = 10000)
  public void testBlockedUserContinuationDoesNotBlockStop(boolean fallbackHandoff) throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CallbackCompletionScenario scenario = callbackCompletionScenario(writer, fallbackHandoff);
    CountDownLatch continuationEntered = new CountDownLatch(1);
    CountDownLatch releaseContinuation = new CountDownLatch(1);
    CompletableFuture<Void> continuation = scenario.durable.thenRun(() -> {
      continuationEntered.countDown();
      await(releaseContinuation);
    });
    scenario.callback.onCompletion(null, null);
    assertTrue(continuationEntered.await(5, TimeUnit.SECONDS));
    CountDownLatch writerClosed = new CountDownLatch(1);
    doAnswer(invocation -> {
      writerClosed.countDown();
      return null;
    }).when(writer).close();

    CompletableFuture<Void> stop = CompletableFuture.runAsync(scenario.dispatcher::stop);
    assertTrue(writerClosed.await(5, TimeUnit.SECONDS));
    try {
      stop.get(5, TimeUnit.SECONDS);
      assertFalse(continuation.isDone(), "The user continuation must remain blocked until explicitly released");
      assertTrue(scenario.dispatcher.isStopped());
    } finally {
      releaseContinuation.countDown();
    }
    continuation.get(5, TimeUnit.SECONDS);
  }

  @Test(timeOut = 10000)
  public void testCallbackContinuationCanFlushAndStopAfterHandoff() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch callbackInvoked = new CountDownLatch(1);
    CountDownLatch releaseWriter = new CountDownLatch(1);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      PubSubProducerCallback callback = invocation.getArgument(4);
      callback.onCompletion(null, null);
      callbackInvoked.countDown();
      await(releaseWriter);
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 1, 1);
    CompletableFuture<Void> durable = dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, -1);
    assertTrue(callbackInvoked.await(5, TimeUnit.SECONDS));
    assertFalse(durable.isDone(), "Synchronous callback completion must wait for writer submission to return");
    CompletableFuture<String> continuationThread = new CompletableFuture<>();
    durable.thenRun(() -> {
      try {
        dispatcher.flush();
        dispatcher.stop();
        continuationThread.complete(Thread.currentThread().getName());
      } catch (Throwable throwable) {
        continuationThread.completeExceptionally(throwable);
      }
    });

    releaseWriter.countDown();
    dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);
    assertTrue(continuationThread.get(5, TimeUnit.SECONDS).contains("ForkJoinPool.commonPool"));
    assertTrue(dispatcher.isStopped());
  }

  @Test
  public void testCallbackFailureDuringWriterFlushFailsFence() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      return new CompletableFuture<>();
    });
    doAnswer(invocation -> {
      callback.get().onCompletion(null, new VeniceException("broker failure"));
      return null;
    }).when(writer).flush();
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 1, 0);
    CompletableFuture<Void> durable = dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, -1);
    dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);

    assertThrows(VeniceException.class, dispatcher::flush);
    assertThrows(ExecutionException.class, () -> durable.get(5, TimeUnit.SECONDS));
    assertThrows(VeniceException.class, dispatcher::stop);
    verify(writer).close();
  }

  @Test(timeOut = 10000)
  public void testFlushMarkerWaitObservesFailureFromAnotherStripe() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    MarkerObservingExecutor executor = new MarkerObservingExecutor();
    CountDownLatch blockedStripeEntered = new CountDownLatch(1);
    CountDownLatch failingStripeEntered = new CountDownLatch(1);
    CountDownLatch releaseBlockedStripe = new CountDownLatch(1);
    CountDownLatch releaseFailingStripe = new CountDownLatch(1);
    VeniceException writeFailure = new VeniceException("submission failure");
    when(writer.getPartitionId(any())).thenAnswer(invocation -> (int) ((byte[]) invocation.getArgument(0))[0]);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      byte[] key = invocation.getArgument(0);
      if (key[0] == 0) {
        blockedStripeEntered.countDown();
        await(releaseBlockedStripe);
        return new CompletableFuture<>();
      }
      failingStripeEntered.countDown();
      await(releaseFailingStripe);
      throw writeFailure;
    });
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, executor);
    dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    dispatcher.put(new byte[] { 1 }, new byte[] { 1 }, 1, -1);
    assertTrue(blockedStripeEntered.await(5, TimeUnit.SECONDS));
    assertTrue(failingStripeEntered.await(5, TimeUnit.SECONDS));

    CompletableFuture<Void> flush = CompletableFuture.runAsync(dispatcher::flush);
    assertTrue(executor.markersSubmitted.await(5, TimeUnit.SECONDS));
    try {
      releaseFailingStripe.countDown();
      ExecutionException flushFailure = expectThrows(ExecutionException.class, () -> flush.get(2, TimeUnit.SECONDS));
      assertSame(flushFailure.getCause().getCause(), writeFailure);
      verify(writer, never()).flush();
    } finally {
      releaseBlockedStripe.countDown();
      assertThrows(VeniceException.class, dispatcher::stop);
    }
  }

  @Test(timeOut = 10000)
  public void testFailureWakesFlushWaitingForPendingAdmission() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    MarkerAdmissionObservingExecutor executor = new MarkerAdmissionObservingExecutor(2, 1, "blocked-sender-fence");
    CountDownLatch blockedStripeEntered = new CountDownLatch(1);
    CountDownLatch releaseBlockedStripe = new CountDownLatch(1);
    CountDownLatch failingStripeEntered = new CountDownLatch(1);
    CountDownLatch releaseFailure = new CountDownLatch(1);
    VeniceException writeFailure = new VeniceException("other stripe failed");
    when(writer.getPartitionId(any())).thenAnswer(invocation -> (int) ((byte[]) invocation.getArgument(0))[0]);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      byte[] key = invocation.getArgument(0);
      if (key[0] == 0) {
        blockedStripeEntered.countDown();
        await(releaseBlockedStripe);
      } else if (key[0] == 1) {
        failingStripeEntered.countDown();
        await(releaseFailure);
        throw writeFailure;
      }
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, executor);
    dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    assertTrue(blockedStripeEntered.await(5, TimeUnit.SECONDS));
    dispatcher.put(new byte[] { 2 }, new byte[] { 1 }, 1, -1);
    dispatcher.put(new byte[] { 1 }, new byte[] { 1 }, 1, -1);
    assertTrue(failingStripeEntered.await(5, TimeUnit.SECONDS));

    CompletableFuture<Void> sender =
        CompletableFuture.runAsync(() -> dispatcher.put(new byte[] { 4 }, new byte[] { 1 }, 1, -1));
    assertTrue(executor.fourthCommandSubmissionStarted.await(5, TimeUnit.SECONDS));
    assertFalse(sender.isDone(), "Sender must be blocked on the full stripe queue");
    CompletableFuture<Void> flush = CompletableFuture.runAsync(dispatcher::flush);

    releaseFailure.countDown();
    ExecutionException flushFailure = expectThrows(ExecutionException.class, () -> flush.get(2, TimeUnit.SECONDS));
    assertSame(flushFailure.getCause().getCause(), writeFailure);
    assertFalse(sender.isDone(), "Failure must wake the flush without waiting for the blocked admission to drain");
    assertEquals(
        executor.markerSubmissionAttempts.get(),
        0,
        "Flush markers must not overtake a previously admitted send");
    releaseBlockedStripe.countDown();
    sender.get(5, TimeUnit.SECONDS);
    assertThrows(VeniceException.class, dispatcher::stop);
  }

  @Test(timeOut = 10000)
  public void testAdmittedSendPrecedesConcurrentFlushWhenQueueIsBlocked() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    MarkerAdmissionObservingExecutor executor = new MarkerAdmissionObservingExecutor(1, 1, "admission-fence");
    CountDownLatch firstWriteEntered = new CountDownLatch(1);
    CountDownLatch releaseFirstWrite = new CountDownLatch(1);
    AtomicBoolean firstWrite = new AtomicBoolean(true);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      if (firstWrite.compareAndSet(true, false)) {
        firstWriteEntered.countDown();
        await(releaseFirstWrite);
      }
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, executor);
    dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    assertTrue(firstWriteEntered.await(5, TimeUnit.SECONDS));
    dispatcher.put(new byte[] { 1 }, new byte[] { 1 }, 1, -1);

    Thread sender = new Thread(() -> dispatcher.put(new byte[] { 2 }, new byte[] { 1 }, 1, -1));
    sender.start();
    assertTrue(executor.thirdCommandSubmissionStarted.await(5, TimeUnit.SECONDS));
    Thread flush = new Thread(dispatcher::flush);
    flush.start();
    assertTrue(
        awaitCondition(() -> flush.getState() == Thread.State.WAITING || executor.markerSubmissionAttempts.get() > 0),
        "Flush did not reach the admission fence");
    assertEquals(
        executor.markerSubmissionAttempts.get(),
        0,
        "Flush must await the already admitted sender before attempting markers");

    releaseFirstWrite.countDown();
    sender.join(TimeUnit.SECONDS.toMillis(5));
    flush.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(sender.isAlive());
    assertFalse(flush.isAlive());
    verify(writer).flush();
    dispatcher.stop();
  }

  @Test(timeOut = 10000)
  public void testMarkerAdmissionOnFullStripeObservesOtherStripeFailure() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    MarkerAdmissionObservingExecutor executor = new MarkerAdmissionObservingExecutor(2, 1, "marker-admission-failure");
    CountDownLatch blockedStripeEntered = new CountDownLatch(1);
    CountDownLatch releaseBlockedStripe = new CountDownLatch(1);
    CountDownLatch failingStripeEntered = new CountDownLatch(1);
    CountDownLatch releaseFailure = new CountDownLatch(1);
    VeniceException writeFailure = new VeniceException("marker admission failure");
    when(writer.getPartitionId(any())).thenAnswer(invocation -> (int) ((byte[]) invocation.getArgument(0))[0]);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      byte[] key = invocation.getArgument(0);
      if (key[0] == 0) {
        blockedStripeEntered.countDown();
        await(releaseBlockedStripe);
      } else if (key[0] == 1) {
        failingStripeEntered.countDown();
        await(releaseFailure);
        throw writeFailure;
      }
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, executor);
    dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    assertTrue(blockedStripeEntered.await(5, TimeUnit.SECONDS));
    dispatcher.put(new byte[] { 2 }, new byte[] { 1 }, 1, -1);
    dispatcher.put(new byte[] { 1 }, new byte[] { 1 }, 1, -1);
    assertTrue(failingStripeEntered.await(5, TimeUnit.SECONDS));

    CompletableFuture<Void> flush = CompletableFuture.runAsync(dispatcher::flush);
    assertTrue(executor.blockedMarkerAdmission.await(5, TimeUnit.SECONDS));
    try {
      releaseFailure.countDown();
      ExecutionException flushFailure = expectThrows(ExecutionException.class, () -> flush.get(2, TimeUnit.SECONDS));
      assertSame(flushFailure.getCause().getCause(), writeFailure);
      verify(writer, never()).flush();
    } finally {
      releaseBlockedStripe.countDown();
      assertThrows(VeniceException.class, dispatcher::stop);
    }
  }

  @Test(timeOut = 10000)
  public void testMarkerAdmissionOnFullStripeSucceedsAfterQueueDrains() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    MarkerAdmissionObservingExecutor executor = new MarkerAdmissionObservingExecutor(1, 1, "marker-admission-drain");
    CountDownLatch blockedStripeEntered = new CountDownLatch(1);
    CountDownLatch releaseBlockedStripe = new CountDownLatch(1);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      byte[] key = invocation.getArgument(0);
      if (key[0] == 0) {
        blockedStripeEntered.countDown();
        await(releaseBlockedStripe);
      }
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, executor);
    dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    assertTrue(blockedStripeEntered.await(5, TimeUnit.SECONDS));
    dispatcher.put(new byte[] { 1 }, new byte[] { 1 }, 1, -1);

    CompletableFuture<Void> flush = CompletableFuture.runAsync(dispatcher::flush);
    assertTrue(executor.blockedMarkerAdmission.await(5, TimeUnit.SECONDS));
    assertFalse(flush.isDone());
    releaseBlockedStripe.countDown();

    flush.get(5, TimeUnit.SECONDS);
    verify(writer).flush();
    dispatcher.stop();
  }

  @Test
  public void testWorkerErrorFailsBothPhasesAndRemainsSticky() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenThrow(new AssertionError("worker error"));
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 1, 0);
    CompletableFuture<Void> durable = dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, -1);

    assertThrows(ExecutionException.class, () -> dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS));
    assertThrows(ExecutionException.class, () -> durable.get(5, TimeUnit.SECONDS));
    assertThrows(VeniceException.class, dispatcher::flush);
    assertThrows(VeniceException.class, dispatcher::stop);
    verify(writer).close();
  }

  @Test(timeOut = 10000)
  public void testInterruptedStopForcesBlockedHookAndClosesAfterDrain() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch hookEntered = new CountDownLatch(1);
    AtomicBoolean flushRanWithoutInterrupt = new AtomicBoolean(false);
    AtomicBoolean closeRanWithoutInterrupt = new AtomicBoolean(false);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      hookEntered.countDown();
      try {
        new CountDownLatch(1).await();
        return new CompletableFuture<>();
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        throw new VeniceException("Liminal hook interrupted", exception);
      }
    });
    doAnswer(invocation -> {
      flushRanWithoutInterrupt.set(!Thread.currentThread().isInterrupted());
      return null;
    }).when(writer).flush();
    doAnswer(invocation -> {
      closeRanWithoutInterrupt.set(!Thread.currentThread().isInterrupted());
      return null;
    }).when(writer).close();
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 1, 0);
    dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, -1);
    assertTrue(hookEntered.await(5, TimeUnit.SECONDS));

    AtomicBoolean stopFailed = new AtomicBoolean(false);
    AtomicBoolean interruptPreserved = new AtomicBoolean(false);
    Thread stopThread = new Thread(() -> {
      try {
        dispatcher.stop();
      } catch (VeniceException exception) {
        stopFailed.set(true);
        interruptPreserved.set(Thread.currentThread().isInterrupted());
      }
    });
    stopThread.start();
    Thread.sleep(200);
    stopThread.interrupt();
    stopThread.join(TimeUnit.SECONDS.toMillis(5));

    assertFalse(stopThread.isAlive());
    assertTrue(stopFailed.get());
    assertTrue(interruptPreserved.get());
    assertTrue(flushRanWithoutInterrupt.get());
    assertTrue(closeRanWithoutInterrupt.get());
    verify(writer).flush();
    verify(writer).close();
  }

  @Test(timeOut = 10000)
  public void testStopWaitsForAdmittedSenderBlockedOnFullQueueAndRemainsIdempotent() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch firstWriteEntered = new CountDownLatch(1);
    CountDownLatch releaseFirstWrite = new CountDownLatch(1);
    AtomicBoolean firstWrite = new AtomicBoolean(true);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      if (firstWrite.compareAndSet(true, false)) {
        firstWriteEntered.countDown();
        await(releaseFirstWrite);
      }
      return new CompletableFuture<>();
    });
    MarkerAdmissionObservingExecutor executor = new MarkerAdmissionObservingExecutor(1, 1, "full-queue-store");
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, executor);
    dispatcher.put(new byte[] { 1 }, new byte[] { 1 }, 1, -1);
    assertTrue(firstWriteEntered.await(5, TimeUnit.SECONDS));
    dispatcher.put(new byte[] { 2 }, new byte[] { 2 }, 1, -1);

    CompletableFuture<Void> admittedSender =
        CompletableFuture.runAsync(() -> dispatcher.put(new byte[] { 3 }, new byte[] { 3 }, 1, -1));
    assertTrue(executor.thirdCommandSubmissionStarted.await(5, TimeUnit.SECONDS));
    assertFalse(admittedSender.isDone());
    AtomicReference<Throwable> stopFailure = new AtomicReference<>();
    Thread stop = new Thread(() -> {
      try {
        dispatcher.stop();
      } catch (Throwable throwable) {
        stopFailure.set(throwable);
      }
    });
    stop.start();

    assertTrue(
        awaitCondition(
            () -> stop.getState() == Thread.State.TIMED_WAITING || executor.markerSubmissionAttempts.get() > 0
                || !stop.isAlive()));
    assertEquals(stop.getState(), Thread.State.TIMED_WAITING, "Stop must wait for the send admitted before its fence");
    assertEquals(executor.markerSubmissionAttempts.get(), 0);
    releaseFirstWrite.countDown();
    admittedSender.get(5, TimeUnit.SECONDS);
    stop.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(stop.isAlive());
    assertNull(stopFailure.get());
    int markerAttempts = executor.markerSubmissionAttempts.get();
    dispatcher.stop();
    assertTrue(dispatcher.isStopped());
    assertEquals(executor.markerSubmissionAttempts.get(), markerAttempts);
    verify(writer, times(1)).flush();
    verify(writer, times(1)).close();
  }

  @Test
  public void testDefaultWriterRoutingFallsBackToStableSerializedKeyHash() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mock(AbstractVeniceWriter.class, CALLS_REAL_METHODS);
    CountDownLatch blockedStripeEntered = new CountDownLatch(1);
    CountDownLatch releaseBlockedStripe = new CountDownLatch(1);
    CountDownLatch otherStripeExecuted = new CountDownLatch(1);
    byte[] blockedKey = { 0 }; // Arrays.hashCode == 31
    byte[] sameStripeKey = { 2 }; // Arrays.hashCode == 33
    byte[] otherStripeKey = { 1 }; // Arrays.hashCode == 32
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      byte[] key = invocation.getArgument(0);
      if (Arrays.equals(key, blockedKey)) {
        blockedStripeEntered.countDown();
        await(releaseBlockedStripe);
      } else if (Arrays.equals(key, otherStripeKey)) {
        otherStripeExecuted.countDown();
      }
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 2, 0);

    CompletableFuture<Void> blocked = dispatcher.put(blockedKey, new byte[] { 1 }, 1, -1);
    assertTrue(blockedStripeEntered.await(5, TimeUnit.SECONDS));
    CompletableFuture<Void> sameStripe = dispatcher.put(sameStripeKey, new byte[] { 2 }, 1, -1);
    CompletableFuture<Void> otherStripe = dispatcher.put(otherStripeKey, new byte[] { 3 }, 1, -1);

    dispatcher.getSubmissionFuture(otherStripe).get(5, TimeUnit.SECONDS);
    assertTrue(otherStripeExecuted.await(5, TimeUnit.SECONDS));
    assertFalse(dispatcher.getSubmissionFuture(sameStripe).isDone());
    dispatcher.checkForFailure();

    releaseBlockedStripe.countDown();
    dispatcher.getSubmissionFuture(blocked).get(5, TimeUnit.SECONDS);
    dispatcher.getSubmissionFuture(sameStripe).get(5, TimeUnit.SECONDS);
    dispatcher.checkForFailure();
    dispatcher.stop();
  }

  @Test
  public void testUnexpectedWriterRoutingFailureRemainsSticky() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    when(writer.getPartitionId(any())).thenThrow(new IllegalStateException("routing failed"));
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 2, 0);

    assertThrows(IllegalStateException.class, () -> dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, -1));
    assertEquals(dispatcher.getPendingAdmissions(), 0);
    assertThrows(VeniceException.class, dispatcher::checkForFailure);
    assertThrows(VeniceException.class, dispatcher::stop);
    assertThrows(VeniceException.class, dispatcher::stop);
    verify(writer).close();
  }

  @Test
  public void testRejectedSubmissionReleasesPendingAdmission() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    VeniceException rejection = new VeniceException("worker rejected admission");
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, new RejectingWorkerExecutor(rejection));

    assertThrows(VeniceException.class, () -> dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, -1));
    assertEquals(dispatcher.getPendingAdmissions(), 0);
    assertThrows(VeniceException.class, dispatcher::stop);
    verify(writer).close();
  }

  private CallbackCompletionScenario callbackCompletionScenario(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      boolean fallbackHandoff) throws Exception {
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = fallbackHandoff
        ? new VeniceSystemProducerWriteDispatcher(writer, new RejectingCallbackExecutor())
        : dispatcher(writer, 1, 1);
    CompletableFuture<Void> durable = dispatcher.put(new byte[] { 1 }, new byte[] { 1 }, 1, -1);
    dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);
    return new CallbackCompletionScenario(dispatcher, callback.get(), durable);
  }

  private VeniceSystemProducerWriteDispatcher dispatcher(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      int workerCount,
      int callbackThreadCount) {
    return new VeniceSystemProducerWriteDispatcher(writer, workerCount, 10, callbackThreadCount, 10, "test-store");
  }

  @SuppressWarnings("unchecked")
  private AbstractVeniceWriter<byte[], byte[], byte[]> writer() {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mock(AbstractVeniceWriter.class);
    when(writer.getPartitionId(any())).thenReturn(0);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenReturn(new CompletableFuture<>());
    return writer;
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted in test writer", exception);
    }
  }

  private static boolean awaitCondition(BooleanSupplier condition) {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean() && System.nanoTime() < deadlineNanos) {
      Thread.yield();
    }
    return condition.getAsBoolean();
  }

  private static final class CallbackCompletionScenario {
    private final VeniceSystemProducerWriteDispatcher dispatcher;
    private final PubSubProducerCallback callback;
    private final CompletableFuture<Void> durable;

    private CallbackCompletionScenario(
        VeniceSystemProducerWriteDispatcher dispatcher,
        PubSubProducerCallback callback,
        CompletableFuture<Void> durable) {
      this.dispatcher = dispatcher;
      this.callback = callback;
      this.durable = durable;
    }
  }

  private static final class RejectingCallbackExecutor extends PartitionedVeniceWriteExecutor {
    private RejectingCallbackExecutor() {
      super(0, 1, 0, 1, "rejected-callback-test", null);
    }

    @Override
    public boolean tryExecuteCallback(Runnable callback, Consumer<Throwable> rejectionCallback) {
      return false;
    }
  }

  private static final class MarkerObservingExecutor extends PartitionedVeniceWriteExecutor {
    private final AtomicInteger submissionCount = new AtomicInteger();
    private final CountDownLatch markersSubmitted = new CountDownLatch(1);

    private MarkerObservingExecutor() {
      super(2, 10, 0, 10, "marker-wait-test", null);
    }

    @Override
    public boolean trySubmit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
      boolean accepted = super.trySubmit(partition, task, rejectionCallback);
      if (accepted && submissionCount.incrementAndGet() == 2) {
        markersSubmitted.countDown();
      }
      return accepted;
    }
  }

  private static final class MarkerAdmissionObservingExecutor extends PartitionedVeniceWriteExecutor {
    private final CountDownLatch blockedMarkerAdmission = new CountDownLatch(1);
    private final CountDownLatch thirdCommandSubmissionStarted = new CountDownLatch(1);
    private final CountDownLatch fourthCommandSubmissionStarted = new CountDownLatch(1);
    private final AtomicInteger commandSubmissions = new AtomicInteger();
    private final AtomicInteger markerSubmissionAttempts = new AtomicInteger();

    private MarkerAdmissionObservingExecutor(int workerCount, int workerQueueCapacity, String storeName) {
      super(workerCount, workerQueueCapacity, 0, 1, storeName, null);
    }

    @Override
    public void submit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
      int submission = commandSubmissions.incrementAndGet();
      if (submission == 3) {
        thirdCommandSubmissionStarted.countDown();
      } else if (submission == 4) {
        fourthCommandSubmissionStarted.countDown();
      }
      super.submit(partition, task, rejectionCallback);
    }

    @Override
    public boolean trySubmit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
      markerSubmissionAttempts.incrementAndGet();
      boolean accepted = super.trySubmit(partition, task, rejectionCallback);
      if (!accepted) {
        blockedMarkerAdmission.countDown();
      }
      return accepted;
    }
  }

  private static final class RejectingWorkerExecutor extends PartitionedVeniceWriteExecutor {
    private final RuntimeException rejection;

    private RejectingWorkerExecutor(RuntimeException rejection) {
      super(1, 1, 0, 1, "rejected-worker-test", null);
      this.rejection = rejection;
    }

    @Override
    public void submit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
      rejectionCallback.accept(rejection);
      throw new RejectedExecutionException("Rejected for test", rejection);
    }
  }
}
