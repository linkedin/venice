package com.linkedin.venice.writer;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;


public class PartitionedVeniceWriteDispatcherTest {
  @Test
  public void testAbstractWriterRoutingExtensionIsBackwardCompatibleAndFailsClearlyByDefault() {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mock(AbstractVeniceWriter.class, CALLS_REAL_METHODS);
    assertThrows(UnsupportedOperationException.class, () -> writer.getPartitionId(new byte[] { 1 }));
  }

  @Test
  public void testInlineModeDoesNotRequireRoutingOverride() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mock(AbstractVeniceWriter.class, CALLS_REAL_METHODS);
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 0, 1, 0, 1, "compatibility-store");

    PartitionedVeniceWriteDispatcher.WriteHandle handle =
        dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 1, VeniceWriter.APP_DEFAULT_LOGICAL_TS);
    handle.getSubmissionFuture().get(5, TimeUnit.SECONDS);
    dispatcher.stopAndDrain();
    dispatcher.shutdownCallbacks();
  }

  @Test
  public void testLegacyWriterUsesStableSerializedKeyStriping() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mock(AbstractVeniceWriter.class, CALLS_REAL_METHODS);
    CountDownLatch writeCompleted = new CountDownLatch(2);
    List<String> workerThreads = new CopyOnWriteArrayList<>();
    org.mockito.Mockito.when(
        writer.put(
            org.mockito.ArgumentMatchers.any(byte[].class),
            org.mockito.ArgumentMatchers.any(byte[].class),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.any()))
        .thenAnswer(invocation -> {
          workerThreads.add(Thread.currentThread().getName());
          PubSubProducerCallback callback = invocation.getArgument(4);
          callback.onCompletion(null, null);
          writeCompleted.countDown();
          return CompletableFuture.completedFuture(null);
        });
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 4, 10, 0, 10, "legacy-routing-store");

    dispatcher.put(new byte[] { 1, 2, 3 }, new byte[] { 4 }, 1, -1);
    dispatcher.put(new byte[] { 1, 2, 3 }, new byte[] { 5 }, 1, -1);

    assertTrue(writeCompleted.await(5, TimeUnit.SECONDS));
    assertEquals(workerThreads.size(), 2);
    assertEquals(workerThreads.get(0), workerThreads.get(1));
    dispatcher.stopAndDrain();
    dispatcher.shutdownCallbacks();
  }

  @Test
  public void testFlushWaitsForPriorWriteToReachCoreWriter() throws Exception {
    TestWriter writer = new TestWriter();
    writer.blockWrites = true;
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 2, 10, 0, 10, "flush-store");
    PartitionedVeniceWriteDispatcher.WriteHandle handle = dispatcher.put(new byte[] { 1 }, new byte[] { 2 }, 3, -1);
    assertTrue(writer.writeEntered.await(5, TimeUnit.SECONDS));

    Thread flushThread = new Thread(dispatcher::flush);
    flushThread.start();
    Thread.sleep(200);
    assertEquals(writer.flushCount.get(), 0);

    writer.releaseWrite.countDown();
    handle.getSubmissionFuture().get(5, TimeUnit.SECONDS);
    flushThread.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(flushThread.isAlive());
    assertEquals(writer.flushCount.get(), 1);

    dispatcher.stopAndDrain();
    dispatcher.shutdownCallbacks();
  }

  @Test
  public void testFailureCallbackIsSticky() throws Exception {
    TestWriter writer = new TestWriter();
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 1, 10, 0, 10, "failure-store");
    PartitionedVeniceWriteDispatcher.WriteHandle handle = dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    handle.getSubmissionFuture().get(5, TimeUnit.SECONDS);

    RuntimeException asyncFailure = new RuntimeException("broker failure");
    writer.callbacks.get(0).onCompletion(null, asyncFailure);
    assertTrue(handle.getDurableFuture().isCompletedExceptionally());

    assertThrows(VeniceException.class, () -> dispatcher.put(new byte[] { 0 }, new byte[] { 2 }, 1, -1));
    assertThrows(VeniceException.class, dispatcher::flush);
    dispatcher.stopAndDrain();
    assertThrows(VeniceException.class, dispatcher::shutdownCallbacks);
  }

  @Test
  public void testWorkerErrorIsStickyAndFailsSubmission() {
    TestWriter writer = new TestWriter();
    writer.writeError = new AssertionError("worker error");
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 1, 10, 0, 10, "worker-error-store");
    PartitionedVeniceWriteDispatcher.WriteHandle handle = dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);

    assertThrows(ExecutionException.class, () -> handle.getSubmissionFuture().get(5, TimeUnit.SECONDS));
    assertThrows(ExecutionException.class, () -> handle.getDurableFuture().get(5, TimeUnit.SECONDS));
    assertThrows(VeniceException.class, dispatcher::checkForFailure);
    dispatcher.stopAndDrain();
    assertThrows(VeniceException.class, dispatcher::shutdownCallbacks);
  }

  @Test
  public void testRoutingComesFromCoreWriter() throws Exception {
    TestWriter writer = new TestWriter();
    writer.partition = 3;
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 2, 10, 0, 10, "routing-store");

    PartitionedVeniceWriteDispatcher.WriteHandle handle =
        dispatcher.delete(new byte[] { 9, 8, 7 }, VeniceWriter.APP_DEFAULT_LOGICAL_TS);
    handle.getSubmissionFuture().get(5, TimeUnit.SECONDS);

    assertEquals(writer.routedKey[0], 9);
    assertTrue(writer.workerThreadName.contains("venice-system-producer-worker-routing-store-1"));
    dispatcher.stopAndDrain();
    dispatcher.shutdownCallbacks();
  }

  @Test
  public void testFailureRacingCoreFlushPreventsSuccess() throws Exception {
    TestWriter writer = new TestWriter();
    writer.callbackFailureOnFlush = new RuntimeException("flush callback failure");
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 1, 10, 0, 10, "flush-failure-store");
    PartitionedVeniceWriteDispatcher.WriteHandle handle = dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    handle.getSubmissionFuture().get(5, TimeUnit.SECONDS);

    assertThrows(VeniceException.class, dispatcher::flush);
    dispatcher.stopAndDrain();
    assertThrows(VeniceException.class, dispatcher::shutdownCallbacks);
  }

  @Test
  public void testFlushQueuesRacingAdmissionBehindFence() throws Exception {
    TestWriter writer = new TestWriter();
    writer.blockFlush = true;
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 1, 10, 0, 10, "fence-store");

    Thread flushThread = new Thread(dispatcher::flush);
    flushThread.start();
    assertTrue(writer.flushEntered.await(5, TimeUnit.SECONDS));

    writer.blockWrites = true;
    AtomicBoolean laterAdmissionReturned = new AtomicBoolean(false);
    Thread submitter = new Thread(() -> {
      dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
      laterAdmissionReturned.set(true);
    });
    submitter.start();
    Thread.sleep(200);
    assertTrue(laterAdmissionReturned.get());
    assertTrue(writer.writeEntered.await(5, TimeUnit.SECONDS));

    writer.releaseFlush.countDown();
    flushThread.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(flushThread.isAlive(), "A post-fence blocked write must not delay the earlier flush");
    writer.releaseWrite.countDown();
    submitter.join(TimeUnit.SECONDS.toMillis(5));

    dispatcher.stopAndDrain();
    dispatcher.shutdownCallbacks();
  }

  @Test(timeOut = 10000)
  public void testDurableFutureContinuationCanSubmitDuringFlush() throws Exception {
    TestWriter writer = new TestWriter();
    writer.completeSuccessfullyOnFlush = true;
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 1, 1, 0, 10, "callback-fence-store");
    PartitionedVeniceWriteDispatcher.WriteHandle first = dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    first.getSubmissionFuture().get(5, TimeUnit.SECONDS);
    first.getDurableFuture().thenRun(() -> {
      for (int value = 2; value <= 6; value++) {
        dispatcher.put(new byte[] { 0 }, new byte[] { (byte) value }, 1, -1);
      }
    });

    dispatcher.flush();

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (writer.writeCount.get() < 6 && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertEquals(writer.writeCount.get(), 6);
    dispatcher.stopAndDrain();
    dispatcher.shutdownCallbacks();
  }

  @Test(timeOut = 10000)
  public void testSynchronousWriterCallbackContinuationCanFlush() throws Exception {
    TestWriter writer = new TestWriter();
    writer.blockWrites = true;
    writer.completeSynchronouslyOnWrite = true;
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 1, 1, 0, 1, "synchronous-callback-store");
    PartitionedVeniceWriteDispatcher.WriteHandle handle = dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    assertTrue(writer.writeEntered.await(5, TimeUnit.SECONDS));

    CountDownLatch continuationCompleted = new CountDownLatch(1);
    handle.getDurableFuture().thenRun(() -> {
      dispatcher.flush();
      continuationCompleted.countDown();
    });
    writer.releaseWrite.countDown();

    assertTrue(continuationCompleted.await(5, TimeUnit.SECONDS));
    dispatcher.stopAndDrain();
    dispatcher.shutdownCallbacks();
  }

  @Test(timeOut = 10000)
  public void testSubmissionFutureContinuationCanFlush() throws Exception {
    TestWriter writer = new TestWriter();
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 1, 10, 0, 10, "submission-future-store");
    PartitionedVeniceWriteDispatcher.WriteHandle handle = dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    CountDownLatch continuationCompleted = new CountDownLatch(1);

    handle.getSubmissionFuture().thenRun(() -> {
      dispatcher.flush();
      continuationCompleted.countDown();
    });

    assertTrue(continuationCompleted.await(5, TimeUnit.SECONDS));
    dispatcher.stopAndDrain();
    dispatcher.shutdownCallbacks();
  }

  @Test
  public void testInterruptedFlushThrowsAndPreservesInterrupt() throws Exception {
    TestWriter writer = new TestWriter();
    writer.blockWrites = true;
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 1, 10, 0, 10, "interrupt-store");
    dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    assertTrue(writer.writeEntered.await(5, TimeUnit.SECONDS));

    AtomicBoolean failed = new AtomicBoolean(false);
    AtomicBoolean interruptPreserved = new AtomicBoolean(false);
    Thread flushThread = new Thread(() -> {
      try {
        dispatcher.flush();
      } catch (VeniceException exception) {
        failed.set(true);
        interruptPreserved.set(Thread.currentThread().isInterrupted());
      }
    });
    flushThread.start();
    Thread.sleep(200);
    flushThread.interrupt();
    flushThread.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(failed.get());
    assertTrue(interruptPreserved.get());
    writer.releaseWrite.countDown();
    dispatcher.stopAndDrain();
    dispatcher.shutdownCallbacks();
  }

  @Test(timeOut = 10000)
  public void testInterruptedStopStillConfirmsWorkerDrain() throws Exception {
    TestWriter writer = new TestWriter();
    writer.blockWrites = true;
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 1, 10, 0, 10, "interrupted-stop-store");
    dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    assertTrue(writer.writeEntered.await(5, TimeUnit.SECONDS));
    AtomicBoolean stopReturned = new AtomicBoolean(false);
    AtomicBoolean interruptPreserved = new AtomicBoolean(false);
    Thread stopThread = new Thread(() -> {
      dispatcher.stopAndDrain();
      stopReturned.set(true);
      interruptPreserved.set(Thread.currentThread().isInterrupted());
    });
    stopThread.start();
    Thread.sleep(200);
    stopThread.interrupt();
    writer.releaseWrite.countDown();
    stopThread.join(5000);

    assertTrue(stopReturned.get());
    assertTrue(interruptPreserved.get());
    assertThrows(VeniceException.class, dispatcher::checkForFailure);
    assertThrows(VeniceException.class, dispatcher::shutdownCallbacks);
  }

  @Test(timeOut = 10000)
  public void testCallbackShutdownWaitsForDeferredDurableCompletion() throws Exception {
    TestWriter writer = new TestWriter();
    PartitionedVeniceWriteDispatcher dispatcher =
        new PartitionedVeniceWriteDispatcher(writer, 1, 10, 1, 10, "callback-shutdown-store");
    PartitionedVeniceWriteDispatcher.WriteHandle handle = dispatcher.put(new byte[] { 0 }, new byte[] { 1 }, 1, -1);
    handle.getSubmissionFuture().get(5, TimeUnit.SECONDS);
    dispatcher.stopAndDrain();

    CountDownLatch continuationStarted = new CountDownLatch(1);
    CountDownLatch releaseContinuation = new CountDownLatch(1);
    handle.getDurableFuture().thenRun(() -> {
      continuationStarted.countDown();
      TestWriter.await(releaseContinuation);
    });
    writer.callbacks.get(0).onCompletion(null, null);
    assertTrue(continuationStarted.await(5, TimeUnit.SECONDS));

    CompletableFuture<Void> shutdown = CompletableFuture.runAsync(dispatcher::shutdownCallbacks);
    Thread.sleep(200);
    assertFalse(shutdown.isDone(), "Callback shutdown must wait for deferred durable completion");

    releaseContinuation.countDown();
    shutdown.get(5, TimeUnit.SECONDS);
    assertTrue(handle.getDurableFuture().isDone());
  }

  private static final class TestWriter extends AbstractVeniceWriter<byte[], byte[], byte[]> {
    private final List<PubSubProducerCallback> callbacks = new CopyOnWriteArrayList<>();
    private final CountDownLatch writeEntered = new CountDownLatch(1);
    private final CountDownLatch releaseWrite = new CountDownLatch(1);
    private final CountDownLatch flushEntered = new CountDownLatch(1);
    private final CountDownLatch releaseFlush = new CountDownLatch(1);
    private final AtomicInteger flushCount = new AtomicInteger();
    private final AtomicInteger writeCount = new AtomicInteger();
    private volatile boolean blockWrites;
    private volatile boolean blockFlush;
    private volatile int partition;
    private volatile byte[] routedKey;
    private volatile String workerThreadName;
    private volatile RuntimeException callbackFailureOnFlush;
    private volatile boolean completeSuccessfullyOnFlush;
    private volatile boolean completeSynchronouslyOnWrite;
    private volatile Error writeError;

    private TestWriter() {
      super("test-store_rt");
    }

    @Override
    public int getPartitionId(byte[] serializedKey) {
      routedKey = serializedKey;
      return partition;
    }

    @Override
    public void close(boolean gracefulClose) {
    }

    @Override
    public CompletableFuture<PubSubProduceResult> put(
        byte[] key,
        byte[] value,
        int valueSchemaId,
        PubSubProducerCallback callback) {
      return put(key, value, valueSchemaId, VeniceWriter.APP_DEFAULT_LOGICAL_TS, callback);
    }

    @Override
    public CompletableFuture<PubSubProduceResult> put(
        byte[] key,
        byte[] value,
        int valueSchemaId,
        long logicalTimestamp,
        PubSubProducerCallback callback) {
      recordWrite(callback);
      return new CompletableFuture<>();
    }

    @Override
    public Future<PubSubProduceResult> update(
        byte[] key,
        byte[] update,
        int valueSchemaId,
        int derivedSchemaId,
        PubSubProducerCallback callback) {
      return update(key, update, valueSchemaId, derivedSchemaId, VeniceWriter.APP_DEFAULT_LOGICAL_TS, callback);
    }

    @Override
    public CompletableFuture<PubSubProduceResult> update(
        byte[] key,
        byte[] update,
        int valueSchemaId,
        int derivedSchemaId,
        long logicalTimestamp,
        PubSubProducerCallback callback) {
      recordWrite(callback);
      return new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<PubSubProduceResult> delete(byte[] key, PubSubProducerCallback callback) {
      return delete(key, VeniceWriter.APP_DEFAULT_LOGICAL_TS, callback);
    }

    @Override
    public CompletableFuture<PubSubProduceResult> delete(
        byte[] key,
        long logicalTimestamp,
        PubSubProducerCallback callback) {
      recordWrite(callback);
      return new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<PubSubProduceResult> put(
        byte[] key,
        byte[] value,
        int valueSchemaId,
        PubSubProducerCallback callback,
        PutMetadata putMetadata) {
      return put(key, value, valueSchemaId, callback);
    }

    @Override
    public CompletableFuture<PubSubProduceResult> put(
        byte[] key,
        byte[] value,
        int valueSchemaId,
        long logicalTimestamp,
        PubSubProducerCallback callback,
        PutMetadata putMetadata) {
      return put(key, value, valueSchemaId, logicalTimestamp, callback);
    }

    @Override
    public CompletableFuture<PubSubProduceResult> delete(
        byte[] key,
        PubSubProducerCallback callback,
        DeleteMetadata deleteMetadata) {
      return delete(key, callback);
    }

    @Override
    public void flush() {
      flushEntered.countDown();
      if (blockFlush) {
        await(releaseFlush);
      }
      if (callbackFailureOnFlush != null && !callbacks.isEmpty()) {
        callbacks.get(0).onCompletion(null, callbackFailureOnFlush);
      } else if (completeSuccessfullyOnFlush && !callbacks.isEmpty()) {
        callbacks.get(0).onCompletion(null, null);
      }
      flushCount.incrementAndGet();
    }

    @Override
    public void close() throws IOException {
    }

    private void recordWrite(PubSubProducerCallback callback) {
      if (writeError != null) {
        throw writeError;
      }
      writeCount.incrementAndGet();
      workerThreadName = Thread.currentThread().getName();
      callbacks.add(callback);
      writeEntered.countDown();
      if (blockWrites) {
        await(releaseWrite);
      }
      if (completeSynchronouslyOnWrite) {
        callback.onCompletion(null, null);
      }
    }

    private static void await(CountDownLatch latch) {
      try {
        latch.await();
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        throw new VeniceException("Interrupted test writer", exception);
      }
    }
  }
}
