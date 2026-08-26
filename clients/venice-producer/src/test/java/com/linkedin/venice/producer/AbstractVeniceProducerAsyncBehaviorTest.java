package com.linkedin.venice.producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for validating the async behavior of {@link AbstractVeniceProducer} with
 * {@link PartitionedProducerExecutor}.
 *
 * <p>Key behaviors validated:</p>
 * <ol>
 *   <li><b>Partition Ordering:</b> Records heading to the same partition are delivered in the order
 *       the client wrote them to the online producer</li>
 *   <li><b>Non-blocking Writes:</b> Producer does not wait for broker ack before returning from asyncPut</li>
 *   <li><b>Backpressure:</b> When worker queues are full, the caller is blocked until space is available</li>
 * </ol>
 */
public class AbstractVeniceProducerAsyncBehaviorTest {
  private static final String TEST_STORE = "test-async-behavior-store";
  private static final String KEY_SCHEMA_STR = "\"int\"";
  private static final Schema KEY_SCHEMA = Schema.parse(KEY_SCHEMA_STR);
  private static final int PARTITION_COUNT = 4;

  private VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter;
  private SchemaReader mockSchemaReader;
  private List<PubSubProducerCallback> capturedCallbacks;

  @BeforeMethod
  public void setUp() {
    mockVeniceWriter = mock(VeniceWriter.class);
    mockSchemaReader = mock(SchemaReader.class);
    capturedCallbacks = Collections.synchronizedList(new ArrayList<>());

    when(mockSchemaReader.getKeySchema()).thenReturn(KEY_SCHEMA);
    when(mockSchemaReader.getValueSchemaId(any(Schema.class))).thenReturn(1);
  }

  @AfterMethod
  public void tearDown() {
    capturedCallbacks.clear();
  }

  /**
   * Validates that records sent to the same partition are delivered in order.
   */
  @Test(timeOut = 10000)
  public void testRecordsToSamePartitionDeliveredInOrder() throws Exception {
    int workerCount = 4;
    int recordCount = 100;

    List<Integer> deliveryOrder = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch allRecordsDelivered = new CountDownLatch(recordCount);

    AtomicInteger sequenceCounter = new AtomicInteger(0);
    doAnswer(invocation -> {
      int seq = sequenceCounter.getAndIncrement();
      deliveryOrder.add(seq);
      PubSubProducerCallback callback = invocation.getArgument(4);
      callback.onCompletion(null, null);
      allRecordsDelivered.countDown();
      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());

    TestableVeniceProducer producer = createProducer(workerCount, 10000);

    try {
      List<CompletableFuture<DurableWrite>> futures = new ArrayList<>();
      for (int i = 0; i < recordCount; i++) {
        futures.add(producer.asyncPut(0, "value-" + i));
      }

      assertTrue(allRecordsDelivered.await(10, TimeUnit.SECONDS), "All records should be delivered");

      for (CompletableFuture<DurableWrite> future: futures) {
        future.get(5, TimeUnit.SECONDS);
      }

      assertEquals(deliveryOrder.size(), recordCount, "Should have all records");
      for (int i = 0; i < recordCount; i++) {
        assertEquals(deliveryOrder.get(i).intValue(), i, "Record at position " + i + " should be in order");
      }
    } finally {
      producer.close();
    }
  }

  /**
   * Validates ordering with concurrent submissions from multiple threads.
   */
  @Test(timeOut = 10000)
  public void testOrderingWithConcurrentSubmissions() throws Exception {
    int workerCount = 4;
    int threadsCount = 4;
    int recordsPerThread = 50;

    Map<Integer, List<Integer>> keyToSequenceNumbers = Collections.synchronizedMap(new HashMap<>());
    for (int i = 0; i < threadsCount; i++) {
      keyToSequenceNumbers.put(i, Collections.synchronizedList(new ArrayList<>()));
    }

    CountDownLatch allRecordsDelivered = new CountDownLatch(threadsCount * recordsPerThread);

    doAnswer(invocation -> {
      PubSubProducerCallback callback = invocation.getArgument(4);
      callback.onCompletion(null, null);
      allRecordsDelivered.countDown();
      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());

    TestableVeniceProducer producer = createProducer(workerCount, 10000);

    try {
      ExecutorService submitterPool =
          Executors.newFixedThreadPool(threadsCount, new DaemonThreadFactory("test-submitter"));
      List<CompletableFuture<Void>> threadFutures = new ArrayList<>();

      for (int t = 0; t < threadsCount; t++) {
        final int threadKey = t;
        CompletableFuture<Void> threadFuture = CompletableFuture.runAsync(() -> {
          List<Integer> mySequences = keyToSequenceNumbers.get(threadKey);
          for (int seq = 0; seq < recordsPerThread; seq++) {
            try {
              producer.asyncPut(threadKey, "seq-" + seq).get(5, TimeUnit.SECONDS);
              mySequences.add(seq);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }, submitterPool);
        threadFutures.add(threadFuture);
      }

      CompletableFuture.allOf(threadFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
      assertTrue(allRecordsDelivered.await(10, TimeUnit.SECONDS), "All records should be delivered");

      for (int key = 0; key < threadsCount; key++) {
        List<Integer> sequences = keyToSequenceNumbers.get(key);
        assertEquals(sequences.size(), recordsPerThread, "Key " + key + " should have all records");
        for (int i = 0; i < recordsPerThread; i++) {
          assertEquals(sequences.get(i).intValue(), i, "Key " + key + ": sequence " + i + " should be in order");
        }
      }

      submitterPool.shutdown();
    } finally {
      producer.close();
    }
  }

  /**
   * Validates that asyncPut returns immediately without waiting for broker acknowledgment.
   */
  @Test(timeOut = 30000)
  public void testAsyncPutReturnsImmediatelyWithoutWaitingForBrokerAck() throws Exception {
    int workerCount = 4;
    AtomicBoolean callbackInvoked = new AtomicBoolean(false);
    CountDownLatch putCalledLatch = new CountDownLatch(1);
    CountDownLatch allowCallbackLatch = new CountDownLatch(1);

    doAnswer(invocation -> {
      PubSubProducerCallback callback = invocation.getArgument(4);
      capturedCallbacks.add(callback);
      putCalledLatch.countDown();

      new Thread(() -> {
        try {
          if (!allowCallbackLatch.await(10, TimeUnit.SECONDS)) {
            return;
          }
          callbackInvoked.set(true);
          callback.onCompletion(null, null);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }).start();

      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());

    TestableVeniceProducer producer = createProducer(workerCount, 10000);

    try {
      CompletableFuture<DurableWrite> future = producer.asyncPut(1, "test-value");

      assertTrue(putCalledLatch.await(5, TimeUnit.SECONDS), "VeniceWriter.put should be called");
      assertFalse(callbackInvoked.get(), "Callback should not be invoked yet");
      assertFalse(future.isDone(), "Future should not be completed before broker ack");

      allowCallbackLatch.countDown();

      future.get(5, TimeUnit.SECONDS);
      assertTrue(callbackInvoked.get(), "Callback should be invoked after allowing it");
      assertTrue(future.isDone(), "Future should be completed after broker ack");
    } finally {
      producer.close();
    }
  }

  /**
   * Validates that multiple asyncPut calls can be made without waiting for previous ones.
   */
  @Test(timeOut = 30000)
  public void testMultipleAsyncPutsWithoutWaiting() throws Exception {
    int workerCount = 4;
    int recordCount = 100;
    AtomicInteger putCallCount = new AtomicInteger(0);
    List<PubSubProducerCallback> pendingCallbacks = Collections.synchronizedList(new ArrayList<>());

    doAnswer(invocation -> {
      PubSubProducerCallback callback = invocation.getArgument(4);
      pendingCallbacks.add(callback);
      putCallCount.incrementAndGet();
      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());

    TestableVeniceProducer producer = createProducer(workerCount, 10000);

    try {
      List<CompletableFuture<DurableWrite>> futures = new ArrayList<>();
      CountDownLatch allDispatched = new CountDownLatch(recordCount);

      // Update mock to count down latch when put is called
      doAnswer(invocation -> {
        PubSubProducerCallback callback = invocation.getArgument(4);
        pendingCallbacks.add(callback);
        putCallCount.incrementAndGet();
        allDispatched.countDown();
        return null;
      }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());

      for (int i = 0; i < recordCount; i++) {
        futures.add(producer.asyncPut(i, "value-" + i));
      }

      assertTrue(allDispatched.await(5, TimeUnit.SECONDS), "All records should be dispatched to VeniceWriter");
      assertEquals(putCallCount.get(), recordCount, "All records should be dispatched to VeniceWriter");

      int completedCount = 0;
      for (CompletableFuture<DurableWrite> future: futures) {
        if (future.isDone()) {
          completedCount++;
        }
      }
      assertEquals(completedCount, 0, "No futures should be completed without broker acks");

      for (PubSubProducerCallback callback: pendingCallbacks) {
        callback.onCompletion(null, null);
      }

      for (CompletableFuture<DurableWrite> future: futures) {
        future.get(5, TimeUnit.SECONDS);
      }

      for (CompletableFuture<DurableWrite> future: futures) {
        assertTrue(future.isDone(), "All futures should be completed after callbacks");
      }
    } finally {
      producer.close();
    }
  }

  /**
   * Validates backpressure blocks caller when queue is full.
   */
  @Test(timeOut = 30000)
  public void testBackpressureBlocksCallerWhenQueueFull() throws Exception {
    int workerCount = 1;
    int queueCapacity = 2;

    CountDownLatch blockingTaskStarted = new CountDownLatch(1);
    CountDownLatch allowBlockingTaskToFinish = new CountDownLatch(1);
    AtomicBoolean callerWasBlocked = new AtomicBoolean(false);

    AtomicInteger putCallCount = new AtomicInteger(0);
    doAnswer(invocation -> {
      int callNum = putCallCount.incrementAndGet();
      if (callNum == 1) {
        blockingTaskStarted.countDown();
        if (!allowBlockingTaskToFinish.await(30, TimeUnit.SECONDS)) {
          throw new RuntimeException("Timed out waiting for allowBlockingTaskToFinish latch");
        }
      }
      PubSubProducerCallback callback = invocation.getArgument(4);
      callback.onCompletion(null, null);
      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());

    TestableVeniceProducer producer = createProducer(workerCount, queueCapacity);

    try {
      producer.asyncPut(0, "blocking");
      assertTrue(blockingTaskStarted.await(5, TimeUnit.SECONDS), "Blocking task should start");

      CountDownLatch submitterStarted = new CountDownLatch(1);
      AtomicBoolean submitterFinished = new AtomicBoolean(false);
      AtomicLong submitterBlockedTimeMs = new AtomicLong(0);

      Thread submitterThread = new Thread(() -> {
        submitterStarted.countDown();
        long startTime = System.nanoTime();
        for (int i = 1; i <= queueCapacity + 2; i++) {
          producer.asyncPut(0, "value-" + i);
        }
        submitterBlockedTimeMs.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
        submitterFinished.set(true);
      });
      submitterThread.start();

      assertTrue(submitterStarted.await(2, TimeUnit.SECONDS), "Submitter should start");

      // Wait for submitter to become blocked (WAITING or TIMED_WAITING state)
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (System.nanoTime() < deadline && !submitterFinished.get()) {
        Thread.State state = submitterThread.getState();
        if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
          callerWasBlocked.set(true);
          break;
        }
        Thread.yield();
      }

      allowBlockingTaskToFinish.countDown();
      submitterThread.join(10000);

      assertTrue(callerWasBlocked.get(), "Caller should have been blocked when queue was full");
      assertTrue(submitterFinished.get(), "Submitter should eventually finish after queue drains");
    } finally {
      producer.close();
    }
  }

  /**
   * Validates backpressure is released when queue drains.
   */
  @Test(timeOut = 30000)
  public void testBackpressureReleasedWhenQueueDrains() throws Exception {
    int workerCount = 1;
    int queueCapacity = 5;

    CountDownLatch firstBatchStarted = new CountDownLatch(1);
    CountDownLatch allowFirstBatchToFinish = new CountDownLatch(1);
    AtomicInteger completedCount = new AtomicInteger(0);

    doAnswer(invocation -> {
      int callNum = completedCount.incrementAndGet();
      if (callNum == 1) {
        firstBatchStarted.countDown();
        if (!allowFirstBatchToFinish.await(30, TimeUnit.SECONDS)) {
          throw new RuntimeException("Timed out waiting for allowFirstBatchToFinish latch");
        }
      }
      PubSubProducerCallback callback = invocation.getArgument(4);
      callback.onCompletion(null, null);
      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());

    TestableVeniceProducer producer = createProducer(workerCount, queueCapacity);

    try {
      producer.asyncPut(0, "blocking");
      assertTrue(firstBatchStarted.await(5, TimeUnit.SECONDS), "First task should start");

      for (int i = 0; i < queueCapacity; i++) {
        producer.asyncPut(0, "queued-" + i);
      }

      AtomicBoolean additionalSubmitCompleted = new AtomicBoolean(false);
      CountDownLatch additionalSubmitStarted = new CountDownLatch(1);

      Thread additionalSubmitter = new Thread(() -> {
        additionalSubmitStarted.countDown();
        producer.asyncPut(0, "additional");
        additionalSubmitCompleted.set(true);
      });
      additionalSubmitter.start();

      assertTrue(additionalSubmitStarted.await(2, TimeUnit.SECONDS));

      // Wait for additional submitter to become blocked (WAITING or TIMED_WAITING state)
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (System.nanoTime() < deadline && !additionalSubmitCompleted.get()) {
        Thread.State state = additionalSubmitter.getState();
        if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
          break;
        }
        Thread.yield();
      }

      assertFalse(additionalSubmitCompleted.get(), "Additional submit should be blocked");

      allowFirstBatchToFinish.countDown();
      additionalSubmitter.join(10000);

      assertTrue(additionalSubmitCompleted.get(), "Additional submit should complete after queue drains");
    } finally {
      producer.close();
    }
  }

  @Test(timeOut = 30000)
  public void testCloseKeepsCallbackExecutorAliveThroughWriterClose() throws Exception {
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    CountDownLatch writeSubmitted = new CountDownLatch(1);
    doAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      writeSubmitted.countDown();
      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());
    doAnswer(invocation -> {
      callback.get().onCompletion(null, null);
      return null;
    }).when(mockVeniceWriter).close();
    TestableVeniceProducer producer = createProducer(1, 10, 1);
    CompletableFuture<DurableWrite> durableWrite = producer.asyncPut(0, "value");
    assertTrue(writeSubmitted.await(5, TimeUnit.SECONDS));

    producer.close();

    durableWrite.get(5, TimeUnit.SECONDS);
    assertTrue(durableWrite.isDone());
  }

  @Test(timeOut = 30000)
  public void testWorkerWriterFailureContinuationCanCloseProducer() throws Exception {
    CountDownLatch writerEntered = new CountDownLatch(1);
    CountDownLatch failWriter = new CountDownLatch(1);
    AtomicReference<Thread> writerThread = new AtomicReference<>();
    doAnswer(invocation -> {
      writerThread.set(Thread.currentThread());
      writerEntered.countDown();
      if (!failWriter.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("Timed out waiting to fail the writer");
      }
      throw new RuntimeException("synchronous writer failure");
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());
    TestableVeniceProducer producer = createProducer(1, 10, 0);
    AtomicBoolean closeStarted = new AtomicBoolean();

    try {
      CompletableFuture<DurableWrite> durableWrite = producer.asyncPut(0, "value");
      assertTrue(writerEntered.await(5, TimeUnit.SECONDS));
      AtomicReference<Thread> continuationThread = new AtomicReference<>();
      AtomicReference<Throwable> continuationFailure = new AtomicReference<>();
      CompletableFuture<Void> closeReturned = durableWrite.handle((ignored, failure) -> {
        continuationThread.set(Thread.currentThread());
        continuationFailure.set(failure);
        closeStarted.set(true);
        closeUnchecked(producer);
        return null;
      });

      failWriter.countDown();
      closeReturned.get(5, TimeUnit.SECONDS);

      assertTrue(durableWrite.isCompletedExceptionally());
      assertTrue(continuationFailure.get() != null);
      assertFalse(
          continuationThread.get() == writerThread.get(),
          "A worker failure must not run user continuations on the worker");
      verify(mockVeniceWriter).close();
    } finally {
      failWriter.countDown();
      if (!closeStarted.get()) {
        producer.close();
      }
    }
  }

  @Test(timeOut = 30000)
  public void testCallbackShutdownRejectionDefersUserContinuation() throws Exception {
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    CountDownLatch writeSubmitted = new CountDownLatch(1);
    doAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      writeSubmitted.countDown();
      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());
    TestableVeniceProducer producer = createProducer(1, 10, 1);
    Thread pubSubThread = null;
    try {
      CompletableFuture<DurableWrite> durableWrite = producer.asyncPut(0, "value");
      assertTrue(writeSubmitted.await(5, TimeUnit.SECONDS));
      producer.getDispatcher().shutdownCallbacks();
      assertTrue(producer.getDispatcher().awaitCallbackTermination(5, TimeUnit.SECONDS));
      CompletableFuture<Thread> continuationThread = new CompletableFuture<>();
      durableWrite.whenComplete((ignored, failure) -> continuationThread.complete(Thread.currentThread()));

      pubSubThread = new Thread(() -> callback.get().onCompletion(null, null), "test-online-pubsub-callback");
      pubSubThread.start();
      pubSubThread.join(TimeUnit.SECONDS.toMillis(5));

      assertFalse(pubSubThread.isAlive());
      Thread completionThread = continuationThread.get(5, TimeUnit.SECONDS);
      assertTrue(durableWrite.isCompletedExceptionally());
      assertFalse(completionThread == pubSubThread, "PubSub rejection must not run user continuations inline");
    } finally {
      if (pubSubThread != null) {
        pubSubThread.interrupt();
        pubSubThread.join(TimeUnit.SECONDS.toMillis(5));
      }
      producer.close();
    }
  }

  @Test(timeOut = 30000)
  public void testDeferredRejectionContinuationCanCloseProducer() throws Exception {
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    CountDownLatch writeSubmitted = new CountDownLatch(1);
    doAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      writeSubmitted.countDown();
      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());
    TestableVeniceProducer producer = createProducer(1, 10, 1);
    AtomicBoolean closeStarted = new AtomicBoolean();

    try {
      CompletableFuture<DurableWrite> durableWrite = producer.asyncPut(0, "value");
      assertTrue(writeSubmitted.await(5, TimeUnit.SECONDS));
      producer.getDispatcher().shutdownCallbacks();
      assertTrue(producer.getDispatcher().awaitCallbackTermination(5, TimeUnit.SECONDS));
      AtomicReference<Throwable> continuationFailure = new AtomicReference<>();
      CompletableFuture<Void> closeReturned = durableWrite.handle((ignored, failure) -> {
        continuationFailure.set(failure);
        closeStarted.set(true);
        closeUnchecked(producer);
        return null;
      });

      callback.get().onCompletion(null, null);
      closeReturned.get(5, TimeUnit.SECONDS);

      assertTrue(durableWrite.isCompletedExceptionally());
      assertTrue(continuationFailure.get() != null);
      verify(mockVeniceWriter).close();
    } finally {
      if (!closeStarted.get()) {
        producer.close();
      }
    }
  }

  @Test(timeOut = 30000)
  public void testWorkerShutdownRejectionDefersUserContinuation() throws Exception {
    CountDownLatch workerEntered = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    doAnswer(invocation -> {
      workerEntered.countDown();
      releaseWorker.await();
      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());
    TestableVeniceProducer producer = createProducer(1, 10, 0);

    try {
      producer.asyncPut(0, "active");
      assertTrue(workerEntered.await(5, TimeUnit.SECONDS));
      CompletableFuture<DurableWrite> rejectedWrite = producer.asyncPut(0, "queued");
      CompletableFuture<Thread> continuationThread = new CompletableFuture<>();
      rejectedWrite.whenComplete((ignored, failure) -> continuationThread.complete(Thread.currentThread()));

      Thread shutdownThread =
          new Thread(() -> producer.getDispatcher().shutdownWorkersNow(), "test-online-worker-shutdown");
      shutdownThread.start();
      shutdownThread.join(TimeUnit.SECONDS.toMillis(5));

      assertFalse(shutdownThread.isAlive());
      Thread completionThread = continuationThread.get(5, TimeUnit.SECONDS);
      assertTrue(rejectedWrite.isCompletedExceptionally());
      assertFalse(
          completionThread == shutdownThread,
          "Worker shutdown rejection must not run user continuations inline");
    } finally {
      releaseWorker.countDown();
      producer.close();
    }
  }

  @Test(timeOut = 30000)
  public void testCloseDrainsDeferredRejectionCompletion() throws Exception {
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    CountDownLatch writeSubmitted = new CountDownLatch(1);
    CountDownLatch writerCloseEntered = new CountDownLatch(1);
    List<String> events = Collections.synchronizedList(new ArrayList<>());
    doAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      writeSubmitted.countDown();
      return null;
    }).when(mockVeniceWriter).put(any(byte[].class), any(byte[].class), anyInt(), anyLong(), any());
    doAnswer(invocation -> {
      events.add("writer-close");
      writerCloseEntered.countDown();
      return null;
    }).when(mockVeniceWriter).close();
    TestableVeniceProducer producer = createProducer(1, 10, 1);
    CountDownLatch continuationEntered = new CountDownLatch(1);
    CountDownLatch releaseContinuation = new CountDownLatch(1);
    CompletableFuture<Void> close = null;
    try {
      CompletableFuture<DurableWrite> durableWrite = producer.asyncPut(0, "value");
      assertTrue(writeSubmitted.await(5, TimeUnit.SECONDS));
      producer.getDispatcher().shutdownCallbacks();
      assertTrue(producer.getDispatcher().awaitCallbackTermination(5, TimeUnit.SECONDS));
      durableWrite.whenComplete((ignored, failure) -> {
        continuationEntered.countDown();
        try {
          releaseContinuation.await();
          events.add("continuation-returned");
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
        }
      });
      callback.get().onCompletion(null, null);
      assertTrue(continuationEntered.await(5, TimeUnit.SECONDS));
      close = CompletableFuture.runAsync(() -> {
        try {
          producer.close();
          events.add("close-returned");
        } catch (IOException exception) {
          throw new RuntimeException(exception);
        }
      }, dedicatedThreadExecutor("test-producer-close"));

      assertTrue(writerCloseEntered.await(5, TimeUnit.SECONDS));
      releaseContinuation.countDown();
      close.get(5, TimeUnit.SECONDS);
      assertEquals(events, Arrays.asList("writer-close", "continuation-returned", "close-returned"));
    } finally {
      releaseContinuation.countDown();
      awaitQuietly(close);
      producer.close();
    }
  }

  @Test(timeOut = 5000)
  public void testRejectedDeferredCompletionExecutorFallsBackInline() throws Exception {
    TestableVeniceProducer producer = createProducer(1, 10, 0);
    try {
      AtomicReference<Thread> completionThread = new AtomicReference<>();
      producer.scheduleDeferredCompletion(() -> completionThread.set(Thread.currentThread()), task -> {
        throw new RejectedExecutionException("deferred executor unavailable");
      });

      assertSame(completionThread.get(), Thread.currentThread());
    } finally {
      producer.close();
    }
  }

  private TestableVeniceProducer createProducer(int workerCount, int queueCapacity) {
    return createProducer(workerCount, queueCapacity, 0);
  }

  private TestableVeniceProducer createProducer(int workerCount, int queueCapacity, int callbackThreadCount) {
    Properties props = new Properties();
    props.setProperty("client.producer.worker.count", String.valueOf(workerCount));
    props.setProperty("client.producer.worker.queue.capacity", String.valueOf(queueCapacity));
    props.setProperty("client.producer.callback.thread.count", String.valueOf(callbackThreadCount));

    TestableVeniceProducer producer = new TestableVeniceProducer(mockVeniceWriter);
    producer.configure(TEST_STORE, new VeniceProperties(props), null, mockSchemaReader, null);
    return producer;
  }

  private static void closeUnchecked(VeniceProducer<?, ?> producer) {
    try {
      producer.close();
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  private static Executor dedicatedThreadExecutor(String threadName) {
    return task -> {
      Thread thread = new Thread(task, threadName);
      thread.setDaemon(true);
      thread.start();
    };
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

  /**
   * Testable subclass of AbstractVeniceProducer that allows injecting mock dependencies.
   */
  private static class TestableVeniceProducer extends AbstractVeniceProducer<Integer, String> {
    private final VeniceWriter<byte[], byte[], byte[]> mockWriter;
    private PartitionedProducerExecutor dispatcher;

    TestableVeniceProducer(VeniceWriter<byte[], byte[], byte[]> mockWriter) {
      this.mockWriter = mockWriter;
    }

    @Override
    protected VersionCreationResponse requestTopic() {
      VersionCreationResponse response = new VersionCreationResponse();
      response.setKafkaTopic("test-topic_v1");
      response.setKafkaBootstrapServers("localhost:9092");
      response.setPartitions(PARTITION_COUNT);
      response.setPartitionerClass(DefaultVenicePartitioner.class.getName());
      response.setPartitionerParams(new HashMap<>());
      return response;
    }

    @Override
    protected VeniceWriter<byte[], byte[], byte[]> constructVeniceWriter(
        Properties properties,
        VeniceWriterOptions writerOptions) {
      return mockWriter;
    }

    @Override
    protected PartitionedProducerExecutor createDispatcher(
        String storeName,
        VeniceProperties configs,
        MetricsRepository metricsRepository) {
      dispatcher = super.createDispatcher(storeName, configs, metricsRepository);
      return dispatcher;
    }

    private PartitionedProducerExecutor getDispatcher() {
      return dispatcher;
    }
  }
}
