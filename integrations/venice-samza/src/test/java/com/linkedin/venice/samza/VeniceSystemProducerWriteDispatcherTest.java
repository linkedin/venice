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
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.PartitionedVeniceWriteExecutor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
    CompletableFuture<Void> flush = null;
    CompletableFuture<Void> sender = null;
    AtomicReference<CompletableFuture<Void>> senderDurable = new AtomicReference<>();
    try {
      flush = runOnDedicatedThread("test-flush", dispatcher::flush);
      assertTrue(flushEntered.await(5, TimeUnit.SECONDS));
      CountDownLatch senderInvoked = new CountDownLatch(1);
      sender = runOnDedicatedThread("test-post-fence-sender", () -> {
        senderInvoked.countDown();
        senderDurable.set(put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1));
      });
      assertTrue(senderInvoked.await(5, TimeUnit.SECONDS));
      assertFalse(sender.isDone(), "Post-fence admission must wait for the core flush");
      releaseFlush.countDown();
      flush.get(5, TimeUnit.SECONDS);
      sender.get(5, TimeUnit.SECONDS);
      dispatcher.getSubmissionFuture(senderDurable.get()).get(5, TimeUnit.SECONDS);
    } finally {
      releaseFlush.countDown();
      awaitQuietly(flush);
      awaitQuietly(sender);
      stopQuietly(dispatcher);
    }
  }

  @Test(timeOut = 30000)
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
        new PartitionedVeniceWriteExecutor(1, 10, 1, 10, "blocked-flush-store", null);
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, executor, 200, TimeUnit.MILLISECONDS, Runnable::run);
    CompletableFuture<Void> flush = null;
    try {
      flush = runOnDedicatedThread("test-blocked-flush", dispatcher::flush);
      assertTrue(flushEntered.await(2, TimeUnit.SECONDS));
      CompletableFuture<Void> stop =
          runOnDedicatedThread("test-timeout-stop", () -> assertThrows(VeniceException.class, dispatcher::stop));
      stop.get(2, TimeUnit.SECONDS);
      assertFalse(flush.isDone(), "The active writer flush must remain fenced");
      assertFalse(closeDuringFlush.get(), "Writer close must not race an active flush");
      verify(writer, never()).close();
      releaseFlush.countDown();
      CompletableFuture<Void> blockedFlush = flush;
      assertThrows(ExecutionException.class, () -> blockedFlush.get(2, TimeUnit.SECONDS));

      assertThrows(VeniceException.class, dispatcher::stop);
      assertTrue(dispatcher.isStopped());
      verify(writer).close();
      assertFalse(executor.tryExecuteCallback(() -> {}, null));
    } finally {
      releaseFlush.countDown();
      awaitQuietly(flush);
      stopQuietly(dispatcher);
    }
  }

  @Test(timeOut = 30000)
  public void testBlockedTerminalFlushReusesSharedCleanupTaskWithoutPerDispatcherExecutor() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch flushEntered = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    CountDownLatch closeCalled = new CountDownLatch(1);
    AtomicInteger flushAttempts = new AtomicInteger();
    AtomicBoolean flushActive = new AtomicBoolean();
    AtomicBoolean closeDuringFlush = new AtomicBoolean();
    AtomicBoolean cleanupThreadWasDaemon = new AtomicBoolean();
    doAnswer(invocation -> {
      flushAttempts.incrementAndGet();
      flushActive.set(true);
      cleanupThreadWasDaemon.set(Thread.currentThread().isDaemon());
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
      closeCalled.countDown();
      return null;
    }).when(writer).close();
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 10, 1, 10, "blocked-terminal-flush", null);
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(
        writer,
        executor,
        100,
        TimeUnit.MILLISECONDS,
        Runnable::run,
        ignored -> {});
    assertFalse(
        Arrays.stream(VeniceSystemProducerWriteDispatcher.class.getDeclaredFields())
            .anyMatch(field -> field.getName().equals("writerCleanupExecutor")),
        "A dispatcher must not retain a permanent writer-cleanup executor");
    try {
      CompletableFuture<Void> initialStop = runOnDedicatedThread("test-initial-terminal-stop", dispatcher::stop);
      assertTrue(flushEntered.await(5, TimeUnit.SECONDS));
      ExecutionException initialFailure =
          expectThrows(ExecutionException.class, () -> initialStop.get(2, TimeUnit.SECONDS));
      assertTrue(initialFailure.getCause() instanceof VeniceException);
      assertFalse(dispatcher.isStopped());
      assertTrue(cleanupThreadWasDaemon.get());
      assertEquals(flushAttempts.get(), 1);
      assertFalse(closeDuringFlush.get());
      verify(writer, never()).close();

      CompletableFuture<Void> retryStop = runOnDedicatedThread("test-retry-terminal-stop", dispatcher::stop);
      ExecutionException retryFailure =
          expectThrows(ExecutionException.class, () -> retryStop.get(2, TimeUnit.SECONDS));
      assertTrue(retryFailure.getCause() instanceof VeniceException);
      assertEquals(flushAttempts.get(), 1, "A retry must keep waiting on the same cleanup attempt");
      verify(writer, never()).close();

      releaseFlush.countDown();
      assertTrue(closeCalled.await(5, TimeUnit.SECONDS));
      dispatcher.stop();

      assertTrue(dispatcher.isStopped());
      assertFalse(closeDuringFlush.get());
      assertEquals(flushAttempts.get(), 1);
      verify(writer).flush();
      verify(writer).close();
      assertFalse(executor.tryExecuteCallback(() -> {}, null));
    } finally {
      releaseFlush.countDown();
      if (!dispatcher.isStopped()) {
        stopQuietly(dispatcher);
      }
    }
  }

  @Test
  public void testStopRetriesWriterCloseFailureAndPreservesStickyFailure() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    VeniceException closeFailure = new VeniceException("writer close failed");
    AtomicInteger closeAttempts = new AtomicInteger();
    doAnswer(invocation -> {
      if (closeAttempts.getAndIncrement() == 0) {
        throw closeFailure;
      }
      return null;
    }).when(writer).close();
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 10, 1, 10, "close-retry-store", null);
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, executor);

    VeniceException initialStopFailure = expectThrows(VeniceException.class, dispatcher::stop);
    assertSame(initialStopFailure.getCause(), closeFailure);
    assertFalse(dispatcher.isStopped());

    VeniceException retryStopFailure = expectThrows(VeniceException.class, dispatcher::stop);
    assertSame(retryStopFailure.getCause(), closeFailure);
    assertTrue(dispatcher.isStopped());
    verify(writer).flush();
    verify(writer, times(2)).close();
    assertFalse(executor.tryExecuteCallback(() -> {}, null));
  }

  @Test
  public void testTerminalFlushFailureClosesWriterAndPreservesStickyIdentity() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    VeniceException flushFailure = new VeniceException("terminal writer flush failed");
    doAnswer(invocation -> {
      throw flushFailure;
    }).when(writer).flush();
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 10, 1, 10, "terminal-flush-failure", null);
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, executor);

    VeniceException initialStopFailure = expectThrows(VeniceException.class, dispatcher::stop);
    assertSame(initialStopFailure.getCause(), flushFailure);
    assertTrue(dispatcher.isStopped());
    verify(writer).flush();
    verify(writer).close();
    assertFalse(executor.tryExecuteCallback(() -> {}, null));

    VeniceException retryStopFailure = expectThrows(VeniceException.class, dispatcher::stop);
    assertSame(retryStopFailure.getCause(), flushFailure);
    verify(writer).flush();
    verify(writer).close();
  }

  @Test(timeOut = 30000)
  public void testInterruptedTerminalCleanupWaitPreservesInterruptWithoutStartingAnotherAttempt() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch flushEntered = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    CountDownLatch closeCalled = new CountDownLatch(1);
    CountDownLatch stopReturned = new CountDownLatch(1);
    AtomicInteger flushAttempts = new AtomicInteger();
    AtomicReference<Throwable> stopFailure = new AtomicReference<>();
    AtomicBoolean interruptPreserved = new AtomicBoolean();
    doAnswer(invocation -> {
      flushAttempts.incrementAndGet();
      flushEntered.countDown();
      await(releaseFlush);
      return null;
    }).when(writer).flush();
    doAnswer(invocation -> {
      closeCalled.countDown();
      return null;
    }).when(writer).close();
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(
        writer,
        new PartitionedVeniceWriteExecutor(1, 10, 1, 10, "interrupted-terminal-cleanup", null),
        5,
        TimeUnit.SECONDS,
        Runnable::run);
    Thread stopThread = new Thread(() -> {
      try {
        dispatcher.stop();
      } catch (Throwable throwable) {
        stopFailure.set(throwable);
      } finally {
        interruptPreserved.set(Thread.currentThread().isInterrupted());
        stopReturned.countDown();
      }
    }, "test-interrupted-terminal-stop");
    stopThread.setDaemon(true);
    try {
      stopThread.start();
      assertTrue(flushEntered.await(5, TimeUnit.SECONDS));
      stopThread.interrupt();
      assertTrue(stopReturned.await(2, TimeUnit.SECONDS));

      assertTrue(stopFailure.get() instanceof VeniceException);
      assertTrue(interruptPreserved.get());
      assertFalse(dispatcher.isStopped());
      assertEquals(flushAttempts.get(), 1);
      verify(writer, never()).close();

      releaseFlush.countDown();
      assertTrue(closeCalled.await(5, TimeUnit.SECONDS));
      dispatcher.stop();
      assertTrue(dispatcher.isStopped());
      assertEquals(flushAttempts.get(), 1);
      verify(writer).close();
    } finally {
      releaseFlush.countDown();
      stopThread.interrupt();
      stopThread.join(TimeUnit.SECONDS.toMillis(5));
      if (!dispatcher.isStopped()) {
        stopQuietly(dispatcher);
      }
    }
  }

  @Test(timeOut = 30000)
  public void testCallbackDuringTerminalCleanupCompletesBeforeCallbackShutdown() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    CountDownLatch callbackEmitted = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      return new CompletableFuture<>();
    });
    doAnswer(invocation -> {
      callback.get().onCompletion(null, null);
      callbackEmitted.countDown();
      await(releaseFlush);
      return null;
    }).when(writer).flush();
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 10, 1, 10, "callback-during-cleanup", null);
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, executor, 5, TimeUnit.SECONDS, Runnable::run);
    CompletableFuture<Void> stop = null;
    try {
      CompletableFuture<Void> durable = put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);
      dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);

      stop = runOnDedicatedThread("test-callback-terminal-stop", dispatcher::stop);
      assertTrue(callbackEmitted.await(5, TimeUnit.SECONDS));
      durable.get(5, TimeUnit.SECONDS);
      assertFalse(stop.isDone(), "Stop must still be waiting for terminal writer cleanup");
      assertTrue(executor.tryExecuteCallback(() -> {}, null), "Callback delivery must remain open during cleanup");

      releaseFlush.countDown();
      stop.get(5, TimeUnit.SECONDS);
      assertTrue(dispatcher.isStopped());
      assertFalse(executor.tryExecuteCallback(() -> {}, null));
      verify(writer).close();
    } finally {
      releaseFlush.countDown();
      awaitQuietly(stop);
      if (!dispatcher.isStopped()) {
        stopQuietly(dispatcher);
      }
    }
  }

  @Test(timeOut = 30000, dataProvider = "pendingAdmissionFailureModes")
  public void testStopRetryDoesNotCleanUpWriterWhileRoutingAdmissionIsPending(boolean stickyRoutingFailure)
      throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch routingEntered = new CountDownLatch(1);
    CountDownLatch releaseRouting = new CountDownLatch(1);
    VeniceException routingFailure = new VeniceException("routing failed while another admission is pending");
    when(writer.getPartitionId(any())).thenAnswer(invocation -> {
      byte[] key = invocation.getArgument(0);
      if (key[0] != 1) {
        throw routingFailure;
      }
      routingEntered.countDown();
      await(releaseRouting);
      return 0;
    });
    AtomicReference<VeniceSystemProducerWriteDispatcher> dispatcherReference = new AtomicReference<>();
    AtomicInteger flushPendingAdmissions = new AtomicInteger(-1);
    AtomicInteger closePendingAdmissions = new AtomicInteger(-1);
    doAnswer(invocation -> {
      flushPendingAdmissions.set(dispatcherReference.get().getPendingAdmissions());
      return null;
    }).when(writer).flush();
    doAnswer(invocation -> {
      closePendingAdmissions.set(dispatcherReference.get().getPendingAdmissions());
      return null;
    }).when(writer).close();
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 10, 0, 10, "routing-admission-stop-retry", null);
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, executor, 5, TimeUnit.SECONDS, Runnable::run);
    dispatcherReference.set(dispatcher);
    CompletableFuture<Void> sender = null;
    AtomicReference<Thread> stopThread = new AtomicReference<>();
    try {
      sender = runOnDedicatedThread(
          "test-routing-admission",
          () -> put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1));
      assertTrue(routingEntered.await(5, TimeUnit.SECONDS));
      assertEquals(dispatcher.getPendingAdmissions(), 1);
      if (stickyRoutingFailure) {
        assertSame(
            expectThrows(VeniceException.class, () -> put(dispatcher, new byte[] { 2 }, new byte[] { 2 }, 1, -1)),
            routingFailure);
        assertEquals(dispatcher.getPendingAdmissions(), 1);
      }

      CompletableFuture<Void> initialStop = runOnDedicatedThread("test-initial-pending-admission-stop", () -> {
        stopThread.set(Thread.currentThread());
        dispatcher.stop();
      });
      assertTrue(dispatcher.awaitStopAdmission(5, TimeUnit.SECONDS));
      stopThread.get().interrupt();
      ExecutionException initialStopFailure =
          expectThrows(ExecutionException.class, () -> initialStop.get(5, TimeUnit.SECONDS));
      Throwable stickyFailure = initialStopFailure.getCause().getCause();
      if (stickyRoutingFailure) {
        assertSame(stickyFailure, routingFailure);
      }
      assertEquals(dispatcher.getPendingAdmissions(), 1);
      verify(writer, never()).flush();
      verify(writer, never()).close();

      CompletableFuture<Void> retryStop = runOnDedicatedThread("test-retry-pending-admission-stop", () -> {
        stopThread.set(Thread.currentThread());
        dispatcher.stop();
      });
      assertTrue(dispatcher.awaitStopAdmission(5, TimeUnit.SECONDS));
      stopThread.get().interrupt();
      ExecutionException retryStopFailure =
          expectThrows(ExecutionException.class, () -> retryStop.get(5, TimeUnit.SECONDS));
      assertSame(retryStopFailure.getCause().getCause(), stickyFailure);
      assertEquals(dispatcher.getPendingAdmissions(), 1);
      verify(writer, never()).flush();
      verify(writer, never()).close();

      releaseRouting.countDown();
      CompletableFuture<Void> admittedSender = sender;
      assertThrows(ExecutionException.class, () -> admittedSender.get(5, TimeUnit.SECONDS));
      assertEquals(dispatcher.getPendingAdmissions(), 0);

      VeniceException cleanupStopFailure = expectThrows(VeniceException.class, dispatcher::stop);
      assertSame(cleanupStopFailure.getCause(), stickyFailure);
      assertTrue(dispatcher.isStopped());
      assertEquals(flushPendingAdmissions.get(), 0);
      assertEquals(closePendingAdmissions.get(), 0);
      verify(writer).flush();
      verify(writer).close();
    } finally {
      releaseRouting.countDown();
      awaitQuietly(sender);
      if (!dispatcher.isStopped()) {
        stopQuietly(dispatcher);
      }
    }
  }

  @DataProvider(name = "pendingAdmissionFailureModes")
  public Object[][] pendingAdmissionFailureModes() {
    return new Object[][] { { false }, { true } };
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
    try {
      CompletableFuture<Void> durable = put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);
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
    } finally {
      releaseWriter.countDown();
      stopQuietly(dispatcher);
    }
  }

  @Test
  public void testCallbackThreadCountZeroHandsOffReentrantCompletion() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 1, 0);
    Thread pubSubThread = null;
    try {
      CompletableFuture<Void> durable = put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);
      dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);
      CompletableFuture<Thread> completionThread = new CompletableFuture<>();
      durable.thenRun(() -> {
        try {
          dispatcher.flush();
          dispatcher.stop();
          completionThread.complete(Thread.currentThread());
        } catch (Throwable throwable) {
          completionThread.completeExceptionally(throwable);
        }
      });

      CompletableFuture<Void> callbackReturned = new CompletableFuture<>();
      pubSubThread = new Thread(() -> {
        callback.get().onCompletion(null, null);
        callbackReturned.complete(null);
      }, "test-pubsub-callback");
      pubSubThread.start();
      callbackReturned.get(5, TimeUnit.SECONDS);

      Thread handoffThread = completionThread.get(5, TimeUnit.SECONDS);
      assertNotSame(handoffThread, pubSubThread);
      assertTrue(handoffThread.getName().startsWith("venice-completion-handoff-t"));
      assertTrue(dispatcher.isStopped());
    } finally {
      if (pubSubThread != null) {
        pubSubThread.interrupt();
        pubSubThread.join(TimeUnit.SECONDS.toMillis(5));
      }
      stopQuietly(dispatcher);
    }
  }

  @Test(timeOut = 30000)
  public void testFencedCallbackFailureUsesRejectedHandoffFallbackWithoutReentry() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    AtomicInteger flushCalls = new AtomicInteger();
    AtomicInteger flushDepth = new AtomicInteger();
    AtomicBoolean recursiveFlush = new AtomicBoolean();
    VeniceException callbackFailure = new VeniceException("broker failure during stop");
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      return new CompletableFuture<>();
    });
    doAnswer(invocation -> {
      if (flushDepth.incrementAndGet() > 1) {
        recursiveFlush.set(true);
      }
      try {
        if (flushCalls.getAndIncrement() == 0) {
          callback.get().onCompletion(null, callbackFailure);
        }
        return null;
      } finally {
        flushDepth.decrementAndGet();
      }
    }).when(writer).flush();
    Executor rejectingHandoff = task -> {
      throw new RejectedExecutionException("reject primary handoff");
    };
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 10, 0, 10, "fenced-callback-failure", null);
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, executor, 5, TimeUnit.SECONDS, rejectingHandoff);
    CompletableFuture<Void> durable = put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);
    dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);
    Thread stopThread = Thread.currentThread();
    AtomicReference<Thread> continuationThread = new AtomicReference<>();
    CompletableFuture<Void> reentrantStop = durable.handle((ignored, failure) -> {
      continuationThread.set(Thread.currentThread());
      assertSame(failure, callbackFailure);
      assertThrows(VeniceException.class, dispatcher::stop);
      return null;
    });

    VeniceException stopFailure = expectThrows(VeniceException.class, dispatcher::stop);
    assertSame(stopFailure.getCause(), callbackFailure);
    reentrantStop.get(5, TimeUnit.SECONDS);

    assertFalse(recursiveFlush.get());
    assertNotSame(continuationThread.get(), stopThread);
    assertTrue(continuationThread.get().getName().startsWith("venice-completion-handoff-t"));
    assertEquals(flushCalls.get(), 1);
    assertTrue(dispatcher.isStopped());
    verify(writer).flush();
    verify(writer).close();
  }

  @Test
  public void testBlockedPrimaryHandoffDoesNotBlockSubmissionWait() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch handoffScheduled = new CountDownLatch(1);
    AtomicReference<Runnable> blockedHandoff = new AtomicReference<>();
    Executor blockedPrimaryHandoff = task -> {
      blockedHandoff.set(task);
      handoffScheduled.countDown();
    };
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      PubSubProducerCallback callback = invocation.getArgument(4);
      callback.onCompletion(null, null);
      return new CompletableFuture<>();
    });
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 10, 0, 10, "blocked-primary-handoff", null);
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, executor, 60, TimeUnit.SECONDS, blockedPrimaryHandoff);
    try {
      CompletableFuture<Void> durable = put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);
      dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);
      assertTrue(handoffScheduled.await(5, TimeUnit.SECONDS));
      assertFalse(durable.isDone(), "Caller-visible durability must remain on the blocked handoff");

      blockedHandoff.getAndSet(null).run();
      durable.get(5, TimeUnit.SECONDS);
    } finally {
      Runnable handoff = blockedHandoff.get();
      if (handoff != null) {
        handoff.run();
      }
      stopQuietly(dispatcher);
    }
  }

  @Test
  public void testRejectedCallbackAdmissionUsesNonblockingHandoff() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CallbackCompletionScenario rejectedAdmission = callbackCompletionScenario(writer, true);
    CompletableFuture<Thread> completionThread = new CompletableFuture<>();
    rejectedAdmission.durable.whenComplete((ignored, failure) -> completionThread.complete(Thread.currentThread()));

    CompletableFuture<Void> callbackReturned = new CompletableFuture<>();
    Thread pubSubCallbackThread = new Thread(() -> {
      rejectedAdmission.callback.onCompletion(null, null);
      callbackReturned.complete(null);
    }, "test-pubsub-callback");
    pubSubCallbackThread.start();
    try {
      callbackReturned.get(5, TimeUnit.SECONDS);
      rejectedAdmission.durable.get(5, TimeUnit.SECONDS);
      Thread completion = completionThread.get(5, TimeUnit.SECONDS);
      assertSame(completion, rejectedAdmission.completionHandoffThread.get());
      assertNotSame(completion, pubSubCallbackThread);
      pubSubCallbackThread.join(TimeUnit.SECONDS.toMillis(5));
      assertFalse(pubSubCallbackThread.isAlive());
      rejectedAdmission.close();
    } finally {
      pubSubCallbackThread.interrupt();
      joinQuietly(pubSubCallbackThread);
      rejectedAdmission.closeQuietly();
    }
  }

  @DataProvider(name = "callbackCompletionPaths")
  public Object[][] callbackCompletionPaths() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "callbackCompletionPaths", timeOut = 30000)
  public void testBlockedUserContinuationDoesNotBlockStop(boolean fallbackHandoff) throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CallbackCompletionScenario scenario = callbackCompletionScenario(writer, fallbackHandoff);
    CountDownLatch continuationEntered = new CountDownLatch(1);
    CountDownLatch releaseContinuation = new CountDownLatch(1);
    CompletableFuture<Void> continuation = scenario.durable.thenRun(() -> {
      continuationEntered.countDown();
      await(releaseContinuation);
    });
    try {
      scenario.callback.onCompletion(null, null);
      assertTrue(continuationEntered.await(5, TimeUnit.SECONDS));
      CountDownLatch writerClosed = new CountDownLatch(1);
      doAnswer(invocation -> {
        writerClosed.countDown();
        return null;
      }).when(writer).close();
      CompletableFuture<Void> stop = runOnDedicatedThread("test-dispatcher-stop", scenario.dispatcher::stop);
      assertTrue(writerClosed.await(5, TimeUnit.SECONDS));
      stop.get(5, TimeUnit.SECONDS);
      assertFalse(continuation.isDone(), "The user continuation must remain blocked until explicitly released");
      assertTrue(scenario.dispatcher.isStopped());
      releaseContinuation.countDown();
      continuation.get(5, TimeUnit.SECONDS);
      scenario.close();
    } finally {
      releaseContinuation.countDown();
      scenario.closeQuietly();
    }
  }

  @Test(timeOut = 30000)
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
    AtomicReference<Thread> completionHandoffThread = new AtomicReference<>();
    ExecutorService completionHandoff = newCompletionHandoffExecutor(completionHandoffThread);
    PartitionedVeniceWriteExecutor executor =
        new PartitionedVeniceWriteExecutor(1, 10, 1, 10, "test-store", null, "venice-system-producer");
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, executor, 5, TimeUnit.SECONDS, completionHandoff);
    try {
      CompletableFuture<Void> durable = put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);
      assertTrue(callbackInvoked.await(5, TimeUnit.SECONDS));
      assertFalse(durable.isDone(), "Synchronous callback completion must wait for writer submission to return");
      CompletableFuture<Thread> continuationThread = new CompletableFuture<>();
      durable.thenRun(() -> {
        try {
          dispatcher.flush();
          dispatcher.stop();
          continuationThread.complete(Thread.currentThread());
        } catch (Throwable throwable) {
          continuationThread.completeExceptionally(throwable);
        }
      });

      releaseWriter.countDown();
      dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);
      assertSame(continuationThread.get(5, TimeUnit.SECONDS), completionHandoffThread.get());
      assertTrue(dispatcher.isStopped());
      completionHandoff.shutdown();
      assertTrue(completionHandoff.awaitTermination(5, TimeUnit.SECONDS));
    } finally {
      releaseWriter.countDown();
      stopQuietly(dispatcher);
      completionHandoff.shutdownNow();
      awaitTerminationQuietly(completionHandoff);
    }
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
    CompletableFuture<Void> durable = put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);
    dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);

    assertThrows(VeniceException.class, dispatcher::flush);
    assertThrows(ExecutionException.class, () -> durable.get(5, TimeUnit.SECONDS));
    assertThrows(VeniceException.class, dispatcher::stop);
    verify(writer).close();
  }

  @Test(timeOut = 30000)
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
    CompletableFuture<Void> flush = null;
    try {
      put(dispatcher, new byte[] { 0 }, new byte[] { 1 }, 1, -1);
      put(dispatcher, new byte[] { 1 }, new byte[] { 1 }, 1, -1);
      assertTrue(blockedStripeEntered.await(5, TimeUnit.SECONDS));
      assertTrue(failingStripeEntered.await(5, TimeUnit.SECONDS));

      flush = runOnDedicatedThread("test-marker-flush", dispatcher::flush);
      assertTrue(executor.markersSubmitted.await(5, TimeUnit.SECONDS));
      releaseFailingStripe.countDown();
      CompletableFuture<Void> markerFlush = flush;
      ExecutionException flushFailure =
          expectThrows(ExecutionException.class, () -> markerFlush.get(2, TimeUnit.SECONDS));
      assertSame(flushFailure.getCause().getCause(), writeFailure);
      verify(writer, never()).flush();
    } finally {
      releaseFailingStripe.countDown();
      releaseBlockedStripe.countDown();
      awaitQuietly(flush);
      stopQuietly(dispatcher);
    }
  }

  @Test(timeOut = 30000)
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
    CompletableFuture<Void> sender = null;
    CompletableFuture<Void> flush = null;
    try {
      put(dispatcher, new byte[] { 0 }, new byte[] { 1 }, 1, -1);
      assertTrue(blockedStripeEntered.await(5, TimeUnit.SECONDS));
      put(dispatcher, new byte[] { 2 }, new byte[] { 1 }, 1, -1);
      put(dispatcher, new byte[] { 1 }, new byte[] { 1 }, 1, -1);
      assertTrue(failingStripeEntered.await(5, TimeUnit.SECONDS));

      sender =
          runOnDedicatedThread("test-blocked-sender", () -> put(dispatcher, new byte[] { 4 }, new byte[] { 1 }, 1, -1));
      assertTrue(executor.fourthCommandSubmissionStarted.await(5, TimeUnit.SECONDS));
      assertEquals(dispatcher.getPendingAdmissions(), 1);
      flush = runOnDedicatedThread("test-pending-admission-flush", dispatcher::flush);
      awaitFenceHeld(dispatcher);

      releaseFailure.countDown();
      CompletableFuture<Void> pendingFlush = flush;
      ExecutionException flushFailure =
          expectThrows(ExecutionException.class, () -> pendingFlush.get(2, TimeUnit.SECONDS));
      assertSame(flushFailure.getCause().getCause(), writeFailure);
      assertEquals(
          dispatcher.getPendingAdmissions(),
          1,
          "Failure must wake flush while the admitted sender remains pending");
      assertEquals(
          executor.markerSubmissionAttempts.get(),
          0,
          "Flush markers must not overtake a previously admitted send");
      releaseBlockedStripe.countDown();
      sender.get(5, TimeUnit.SECONDS);
      assertThrows(VeniceException.class, dispatcher::stop);
    } finally {
      releaseFailure.countDown();
      releaseBlockedStripe.countDown();
      awaitQuietly(sender);
      awaitQuietly(flush);
      if (!dispatcher.isStopped()) {
        stopQuietly(dispatcher);
      }
    }
  }

  @Test(timeOut = 30000)
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
    CompletableFuture<Void> sender = null;
    CompletableFuture<Void> flush = null;
    try {
      put(dispatcher, new byte[] { 0 }, new byte[] { 1 }, 1, -1);
      assertTrue(firstWriteEntered.await(5, TimeUnit.SECONDS));
      put(dispatcher, new byte[] { 1 }, new byte[] { 1 }, 1, -1);

      sender = runOnDedicatedThread(
          "test-admitted-sender",
          () -> put(dispatcher, new byte[] { 2 }, new byte[] { 1 }, 1, -1));
      assertTrue(executor.thirdCommandSubmissionStarted.await(5, TimeUnit.SECONDS));
      assertEquals(dispatcher.getPendingAdmissions(), 1);
      flush = runOnDedicatedThread("test-admission-fence-flush", dispatcher::flush);
      awaitFenceHeld(dispatcher);

      releaseFirstWrite.countDown();
      sender.get(5, TimeUnit.SECONDS);
      flush.get(5, TimeUnit.SECONDS);
      assertFalse(executor.markerOvertookThirdCommand.get());
      verify(writer).flush();
    } finally {
      releaseFirstWrite.countDown();
      awaitQuietly(sender);
      awaitQuietly(flush);
      stopQuietly(dispatcher);
    }
  }

  @Test
  public void testMarkerAdmissionWaitsBetweenRejectedAttempts() {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    RejectFirstMarkerExecutor executor = new RejectFirstMarkerExecutor("marker-admission-wait");
    AtomicInteger markerAdmissionWaits = new AtomicInteger();
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, executor, 5, TimeUnit.SECONDS, Runnable::run, waitNanos -> {
          assertEquals(executor.markerSubmissionAttempts.get(), 1);
          assertEquals(waitNanos, TimeUnit.MILLISECONDS.toNanos(100));
          markerAdmissionWaits.incrementAndGet();
        });
    try {
      dispatcher.flush();
      assertEquals(executor.markerSubmissionAttempts.get(), 2);
      assertEquals(markerAdmissionWaits.get(), 1);
      verify(writer).flush();
    } finally {
      stopQuietly(dispatcher);
    }
  }

  @Test
  public void testMarkerAdmissionWaitRechecksInterruptAndPreservesStatus() {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    RejectFirstMarkerExecutor executor = new RejectFirstMarkerExecutor("marker-admission-interrupt");
    AtomicInteger markerAdmissionWaits = new AtomicInteger();
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, executor, 5, TimeUnit.SECONDS, Runnable::run, ignored -> {
          markerAdmissionWaits.incrementAndGet();
          Thread.currentThread().interrupt();
        });
    try {
      VeniceException interruption = expectThrows(VeniceException.class, dispatcher::flush);
      assertTrue(interruption.getMessage().contains("Interrupted while enqueuing"));
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(executor.markerSubmissionAttempts.get(), 1);
      assertEquals(markerAdmissionWaits.get(), 1);
      verify(writer, never()).flush();
    } finally {
      Thread.interrupted();
      stopQuietly(dispatcher);
    }
  }

  @Test(timeOut = 30000)
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
    CompletableFuture<Void> flush = null;
    try {
      put(dispatcher, new byte[] { 0 }, new byte[] { 1 }, 1, -1);
      assertTrue(blockedStripeEntered.await(5, TimeUnit.SECONDS));
      put(dispatcher, new byte[] { 2 }, new byte[] { 1 }, 1, -1);
      put(dispatcher, new byte[] { 1 }, new byte[] { 1 }, 1, -1);
      assertTrue(failingStripeEntered.await(5, TimeUnit.SECONDS));

      flush = runOnDedicatedThread("test-full-stripe-failure-flush", dispatcher::flush);
      assertTrue(executor.blockedMarkerAdmission.await(5, TimeUnit.SECONDS));
      releaseFailure.countDown();
      CompletableFuture<Void> blockedFlush = flush;
      ExecutionException flushFailure =
          expectThrows(ExecutionException.class, () -> blockedFlush.get(2, TimeUnit.SECONDS));
      assertSame(flushFailure.getCause().getCause(), writeFailure);
      verify(writer, never()).flush();
    } finally {
      releaseFailure.countDown();
      releaseBlockedStripe.countDown();
      awaitQuietly(flush);
      stopQuietly(dispatcher);
    }
  }

  @Test(timeOut = 30000)
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
    CompletableFuture<Void> flush = null;
    try {
      put(dispatcher, new byte[] { 0 }, new byte[] { 1 }, 1, -1);
      assertTrue(blockedStripeEntered.await(5, TimeUnit.SECONDS));
      put(dispatcher, new byte[] { 1 }, new byte[] { 1 }, 1, -1);

      flush = runOnDedicatedThread("test-full-stripe-drain-flush", dispatcher::flush);
      assertTrue(executor.blockedMarkerAdmission.await(5, TimeUnit.SECONDS));
      assertFalse(flush.isDone(), "Flush must wait for blocked marker admission to drain");
      releaseBlockedStripe.countDown();

      flush.get(5, TimeUnit.SECONDS);
      verify(writer).flush();
    } finally {
      releaseBlockedStripe.countDown();
      awaitQuietly(flush);
      stopQuietly(dispatcher);
    }
  }

  @Test
  public void testWorkerErrorFailsBothPhasesAndRemainsSticky() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenThrow(new AssertionError("worker error"));
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 1, 0);
    CompletableFuture<Void> durable = put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);

    assertThrows(ExecutionException.class, () -> dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS));
    assertThrows(ExecutionException.class, () -> durable.get(5, TimeUnit.SECONDS));
    assertThrows(VeniceException.class, dispatcher::flush);
    assertThrows(VeniceException.class, dispatcher::stop);
    verify(writer).close();
  }

  @Test(timeOut = 30000)
  public void testInterruptedStopForcesBlockedHookAndClosesAfterDrain() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch hookEntered = new CountDownLatch(1);
    CountDownLatch releaseHook = new CountDownLatch(1);
    AtomicBoolean flushRanWithoutInterrupt = new AtomicBoolean(false);
    AtomicBoolean closeRanWithoutInterrupt = new AtomicBoolean(false);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      hookEntered.countDown();
      try {
        releaseHook.await();
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
    StopMarkerObservingExecutor executor = new StopMarkerObservingExecutor();
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, executor);
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
    try {
      put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);
      assertTrue(hookEntered.await(5, TimeUnit.SECONDS));
      stopThread.start();
      assertTrue(executor.markerSubmitted.await(5, TimeUnit.SECONDS));
      stopThread.interrupt();
      stopThread.join(TimeUnit.SECONDS.toMillis(5));

      assertFalse(stopThread.isAlive());
      assertTrue(stopFailed.get());
      assertTrue(interruptPreserved.get());
      assertTrue(flushRanWithoutInterrupt.get());
      assertTrue(closeRanWithoutInterrupt.get());
      verify(writer).flush();
      verify(writer).close();
    } finally {
      releaseHook.countDown();
      stopThread.interrupt();
      if (stopThread.isAlive()) {
        stopThread.join(TimeUnit.SECONDS.toMillis(5));
      }
      stopQuietly(dispatcher);
    }
  }

  @Test(timeOut = 30000)
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
    CompletableFuture<Void> admittedSender = null;
    CompletableFuture<Void> stop = null;
    try {
      put(dispatcher, new byte[] { 1 }, new byte[] { 1 }, 1, -1);
      assertTrue(firstWriteEntered.await(5, TimeUnit.SECONDS));
      put(dispatcher, new byte[] { 2 }, new byte[] { 2 }, 1, -1);

      admittedSender = runOnDedicatedThread(
          "test-admitted-sender",
          () -> put(dispatcher, new byte[] { 3 }, new byte[] { 3 }, 1, -1));
      assertTrue(executor.thirdCommandSubmissionStarted.await(5, TimeUnit.SECONDS));
      assertEquals(dispatcher.getPendingAdmissions(), 1);
      stop = runOnDedicatedThread("test-admission-fence-stop", dispatcher::stop);
      awaitFenceHeld(dispatcher);

      releaseFirstWrite.countDown();
      admittedSender.get(5, TimeUnit.SECONDS);
      stop.get(5, TimeUnit.SECONDS);
      assertFalse(executor.markerOvertookThirdCommand.get());
      int markerAttempts = executor.markerSubmissionAttempts.get();
      dispatcher.stop();
      assertTrue(dispatcher.isStopped());
      assertEquals(executor.markerSubmissionAttempts.get(), markerAttempts);
      verify(writer, times(1)).flush();
      verify(writer, times(1)).close();
    } finally {
      releaseFirstWrite.countDown();
      awaitQuietly(admittedSender);
      awaitQuietly(stop);
      if (!dispatcher.isStopped()) {
        stopQuietly(dispatcher);
      }
    }
  }

  @Test
  public void testUnsupportedWriterRoutingSerializesDistinctKeysWithBackpressure() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mock(AbstractVeniceWriter.class, CALLS_REAL_METHODS);
    CountDownLatch firstWriteEntered = new CountDownLatch(1);
    CountDownLatch releaseFirstWrite = new CountDownLatch(1);
    CountDownLatch remainingWritesEntered = new CountDownLatch(2);
    AtomicBoolean firstWriteActive = new AtomicBoolean();
    AtomicBoolean writesOverlapped = new AtomicBoolean();
    List<Integer> writeOrder = Collections.synchronizedList(new ArrayList<>());
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      byte[] key = invocation.getArgument(0);
      writeOrder.add((int) key[0]);
      if (key[0] == 0) {
        firstWriteActive.set(true);
        firstWriteEntered.countDown();
        try {
          await(releaseFirstWrite);
        } finally {
          firstWriteActive.set(false);
        }
      } else if (firstWriteActive.get()) {
        writesOverlapped.set(true);
      }
      if (key[0] != 0) {
        remainingWritesEntered.countDown();
      }
      return new CompletableFuture<>();
    });
    MarkerAdmissionObservingExecutor executor =
        new MarkerAdmissionObservingExecutor(2, 1, "unsupported-routing-backpressure");
    VeniceSystemProducerWriteDispatcher dispatcher = new VeniceSystemProducerWriteDispatcher(writer, executor);
    CompletableFuture<Void> thirdSender = null;
    try {
      CompletableFuture<Void> first = put(dispatcher, new byte[] { 0 }, new byte[] { 1 }, 1, -1);
      assertTrue(firstWriteEntered.await(5, TimeUnit.SECONDS));
      CompletableFuture<Void> second = put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);
      thirdSender = runOnDedicatedThread(
          "test-legacy-routing-backpressure",
          () -> put(dispatcher, new byte[] { 1 }, new byte[] { 3 }, 1, -1));
      assertTrue(executor.thirdCommandSubmissionStarted.await(5, TimeUnit.SECONDS));
      assertFalse(thirdSender.isDone(), "A full legacy stripe must apply caller backpressure");
      assertEquals(dispatcher.getPendingAdmissions(), 1);

      releaseFirstWrite.countDown();
      thirdSender.get(5, TimeUnit.SECONDS);
      assertTrue(remainingWritesEntered.await(5, TimeUnit.SECONDS));
      dispatcher.getSubmissionFuture(first).get(5, TimeUnit.SECONDS);
      dispatcher.getSubmissionFuture(second).get(5, TimeUnit.SECONDS);
      dispatcher.checkForFailure();
      assertFalse(writesOverlapped.get(), "Unsupported routing must not execute distinct keys concurrently");
      assertEquals(writeOrder, Arrays.asList(0, 1, 1));
    } finally {
      releaseFirstWrite.countDown();
      awaitQuietly(thirdSender);
      stopQuietly(dispatcher);
    }
  }

  @Test
  public void testSupportedWriterUsesExactPartitionRouting() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    CountDownLatch partitionZeroEntered = new CountDownLatch(1);
    CountDownLatch releasePartitionZero = new CountDownLatch(1);
    CountDownLatch partitionOneEntered = new CountDownLatch(1);
    when(writer.getPartitionId(any())).thenAnswer(invocation -> (int) ((byte[]) invocation.getArgument(0))[0]);
    when(writer.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      byte[] key = invocation.getArgument(0);
      if (key[0] == 0) {
        partitionZeroEntered.countDown();
        await(releasePartitionZero);
      } else {
        partitionOneEntered.countDown();
      }
      return new CompletableFuture<>();
    });
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 2, 0);
    try {
      CompletableFuture<Void> partitionZero = put(dispatcher, new byte[] { 0 }, new byte[] { 1 }, 1, -1);
      assertTrue(partitionZeroEntered.await(5, TimeUnit.SECONDS));
      CompletableFuture<Void> partitionOne = put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1);

      assertTrue(partitionOneEntered.await(5, TimeUnit.SECONDS));
      dispatcher.getSubmissionFuture(partitionOne).get(5, TimeUnit.SECONDS);
      assertFalse(dispatcher.getSubmissionFuture(partitionZero).isDone());
    } finally {
      releasePartitionZero.countDown();
      stopQuietly(dispatcher);
    }
  }

  @Test
  public void testUnexpectedWriterRoutingFailureRemainsSticky() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = writer();
    when(writer.getPartitionId(any())).thenThrow(new IllegalStateException("routing failed"));
    VeniceSystemProducerWriteDispatcher dispatcher = dispatcher(writer, 2, 0);

    assertThrows(IllegalStateException.class, () -> put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1));
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

    assertThrows(VeniceException.class, () -> put(dispatcher, new byte[] { 1 }, new byte[] { 2 }, 1, -1));
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
    AtomicReference<Thread> completionHandoffThread = new AtomicReference<>();
    ExecutorService completionHandoff = newCompletionHandoffExecutor(completionHandoffThread);
    PartitionedVeniceWriteExecutor executor = fallbackHandoff
        ? new RejectingCallbackExecutor()
        : new PartitionedVeniceWriteExecutor(1, 10, 1, 10, "test-store", null, "venice-system-producer");
    VeniceSystemProducerWriteDispatcher dispatcher =
        new VeniceSystemProducerWriteDispatcher(writer, executor, 5, TimeUnit.SECONDS, completionHandoff);
    try {
      CompletableFuture<Void> durable = put(dispatcher, new byte[] { 1 }, new byte[] { 1 }, 1, -1);
      dispatcher.getSubmissionFuture(durable).get(5, TimeUnit.SECONDS);
      return new CallbackCompletionScenario(
          dispatcher,
          callback.get(),
          durable,
          completionHandoff,
          completionHandoffThread);
    } catch (Exception | Error failure) {
      stopQuietly(dispatcher);
      completionHandoff.shutdownNow();
      try {
        completionHandoff.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        failure.addSuppressed(exception);
      }
      throw failure;
    }
  }

  private VeniceSystemProducerWriteDispatcher dispatcher(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      int workerCount,
      int callbackThreadCount) {
    return new VeniceSystemProducerWriteDispatcher(writer, workerCount, 10, callbackThreadCount, 10, "test-store");
  }

  private static CompletableFuture<Void> put(
      VeniceSystemProducerWriteDispatcher dispatcher,
      byte[] key,
      byte[] value,
      int valueSchemaId,
      long logicalTimestamp) {
    return dispatcher.dispatch(VeniceSystemProducerWriteCommand.put(key, value, valueSchemaId, logicalTimestamp));
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

  private static CompletableFuture<Void> runOnDedicatedThread(String threadName, Runnable task) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    Thread thread = new Thread(() -> {
      try {
        task.run();
        result.complete(null);
      } catch (Throwable throwable) {
        result.completeExceptionally(throwable);
      }
    }, threadName);
    thread.setDaemon(true);
    thread.start();
    return result;
  }

  private static void awaitQuietly(CompletableFuture<?> future) {
    if (future == null) {
      return;
    }
    try {
      future.handle((ignored, failure) -> null).get(5, TimeUnit.SECONDS);
    } catch (Throwable ignored) {
      // Preserve the primary test failure while making a best effort to terminate test work.
    }
  }

  private static void stopQuietly(VeniceSystemProducerWriteDispatcher dispatcher) {
    try {
      dispatcher.stop();
    } catch (Throwable ignored) {
      // Failure-path tests assert the expected stop error before cleanup.
    }
  }

  private static void joinQuietly(Thread thread) {
    try {
      thread.join(TimeUnit.SECONDS.toMillis(5));
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  private static void awaitTerminationQuietly(ExecutorService executor) {
    try {
      executor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  private static void awaitFenceHeld(VeniceSystemProducerWriteDispatcher dispatcher) {
    try {
      assertTrue(dispatcher.awaitFence(5, TimeUnit.SECONDS));
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while waiting for dispatcher fence", exception);
    }
  }

  private static ExecutorService newCompletionHandoffExecutor(AtomicReference<Thread> handoffThread) {
    return Executors.newSingleThreadExecutor(task -> {
      Thread thread = new Thread(task, "test-completion-handoff");
      thread.setDaemon(true);
      handoffThread.set(thread);
      return thread;
    });
  }

  private static final class CallbackCompletionScenario {
    private final VeniceSystemProducerWriteDispatcher dispatcher;
    private final PubSubProducerCallback callback;
    private final CompletableFuture<Void> durable;
    private final ExecutorService completionHandoff;
    private final AtomicReference<Thread> completionHandoffThread;

    private CallbackCompletionScenario(
        VeniceSystemProducerWriteDispatcher dispatcher,
        PubSubProducerCallback callback,
        CompletableFuture<Void> durable,
        ExecutorService completionHandoff,
        AtomicReference<Thread> completionHandoffThread) {
      this.dispatcher = dispatcher;
      this.callback = callback;
      this.durable = durable;
      this.completionHandoff = completionHandoff;
      this.completionHandoffThread = completionHandoffThread;
    }

    private void close() throws Exception {
      dispatcher.stop();
      completionHandoff.shutdown();
      assertTrue(completionHandoff.awaitTermination(5, TimeUnit.SECONDS));
    }

    private void closeQuietly() {
      stopQuietly(dispatcher);
      completionHandoff.shutdownNow();
      awaitTerminationQuietly(completionHandoff);
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

  private static final class StopMarkerObservingExecutor extends PartitionedVeniceWriteExecutor {
    private final CountDownLatch markerSubmitted = new CountDownLatch(1);
    private final AtomicInteger acceptedSubmissions = new AtomicInteger();

    private StopMarkerObservingExecutor() {
      super(1, 10, 0, 10, "interrupted-stop-test", null);
    }

    @Override
    public void submit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
      super.submit(partition, task, rejectionCallback);
      acceptedSubmissions.incrementAndGet();
    }

    @Override
    public boolean trySubmit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
      boolean accepted = super.trySubmit(partition, task, rejectionCallback);
      if (accepted && acceptedSubmissions.incrementAndGet() == 2) {
        markerSubmitted.countDown();
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
    private final AtomicBoolean thirdCommandSubmissionReturned = new AtomicBoolean();
    private final AtomicBoolean markerOvertookThirdCommand = new AtomicBoolean();

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
      if (submission == 3) {
        thirdCommandSubmissionReturned.set(true);
      }
    }

    @Override
    public boolean trySubmit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
      markerSubmissionAttempts.incrementAndGet();
      if (commandSubmissions.get() >= 3 && !thirdCommandSubmissionReturned.get()) {
        markerOvertookThirdCommand.set(true);
      }
      boolean accepted = super.trySubmit(partition, task, rejectionCallback);
      if (!accepted) {
        blockedMarkerAdmission.countDown();
      }
      return accepted;
    }
  }

  private static final class RejectFirstMarkerExecutor extends PartitionedVeniceWriteExecutor {
    private final AtomicInteger markerSubmissionAttempts = new AtomicInteger();

    private RejectFirstMarkerExecutor(String storeName) {
      super(1, 1, 0, 1, storeName, null);
    }

    @Override
    public boolean trySubmit(int partition, Runnable task, Consumer<Throwable> rejectionCallback) {
      if (markerSubmissionAttempts.incrementAndGet() == 1) {
        return false;
      }
      return super.trySubmit(partition, task, rejectionCallback);
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
