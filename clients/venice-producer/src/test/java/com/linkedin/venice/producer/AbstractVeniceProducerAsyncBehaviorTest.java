package com.linkedin.venice.producer;

import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_CALLBACK_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_WORKER_COUNT;
import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_WORKER_QUEUE_CAPACITY;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterHook;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
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
  private static final Schema UPDATE_SCHEMA = WriteComputeSchemaConverter.getInstance()
      .convertFromValueRecordSchema(
          Schema.parse(
              "{\"type\":\"record\",\"name\":\"HookTestValue\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"default\":\"\"}]}"));

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
  @Test(timeOut = 30000)
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
      assertNull(producer.capturedWriterOptions.getWriterHook(), "An explicitly null hook must remain hook-free");
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
  @Test(timeOut = 30000)
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

  @DataProvider(name = "writerHookExecutionModes")
  public Object[][] writerHookExecutionModes() {
    return new Object[][] { { 0, null }, { 0, 0L }, { 0, 1000L }, { 2, null }, { 2, 0L }, { 2, 1000L } };
  }

  @Test(dataProvider = "writerHookExecutionModes", timeOut = 30000)
  public void testWriterHookUsesSerializedSizesAndLogicalTimestamps(int workerCount, Long logicalTimestamp)
      throws Exception {
    List<KafkaMessageEnvelope> messages = Collections.synchronizedList(new ArrayList<>());
    PubSubProducerAdapter adapter = createPubSubProducer((message, callback) -> {
      messages.add(message);
      callback.onCompletion(null, null);
    });
    VeniceWriterHook writerHook = mock(VeniceWriterHook.class);

    try (TestableVeniceProducer producer = createRealWriterProducer(adapter, workerCount, 0, writerHook)) {
      assertSame(producer.capturedWriterOptions.getWriterHook(), writerHook);

      Consumer<UpdateBuilder> updateFunction = builder -> builder.setNewFieldValue("name", "new-value");
      List<CompletableFuture<DurableWrite>> writes = Arrays.asList(
          logicalTimestamp == null ? producer.asyncPut(1, "value") : producer.asyncPut(logicalTimestamp, 1, "value"),
          logicalTimestamp == null ? producer.asyncDelete(1) : producer.asyncDelete(logicalTimestamp, 1),
          logicalTimestamp == null
              ? producer.asyncUpdate(1, updateFunction)
              : producer.asyncUpdate(logicalTimestamp, 1, updateFunction));
      for (CompletableFuture<DurableWrite> write: writes) {
        write.get(5, TimeUnit.SECONDS);
      }

      UpdateBuilder expectedUpdate = new UpdateBuilderImpl(UPDATE_SCHEMA);
      updateFunction.accept(expectedUpdate);
      int keySize = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KEY_SCHEMA).serialize(1).length;
      int valueSize = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(Schema.create(Schema.Type.STRING))
          .serialize("value").length;
      int updateSize = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(UPDATE_SCHEMA)
          .serialize(expectedUpdate.build()).length;
      verify(writerHook).onBeforeProduce(VeniceWriterHook.OperationType.PUT, keySize, valueSize);
      verify(writerHook).onBeforeProduce(VeniceWriterHook.OperationType.DELETE, keySize, 0);
      verify(writerHook).onBeforeProduce(VeniceWriterHook.OperationType.UPDATE, keySize, updateSize);
      verifyNoMoreInteractions(writerHook);

      assertEquals(messages.size(), 3);
      assertEquals(messages.get(0).messageType, MessageType.PUT.getValue());
      assertEquals(messages.get(1).messageType, MessageType.DELETE.getValue());
      assertEquals(messages.get(2).messageType, MessageType.UPDATE.getValue());
      long expectedTimestamp = logicalTimestamp == null ? APP_DEFAULT_LOGICAL_TS : logicalTimestamp;
      for (KafkaMessageEnvelope message: messages) {
        assertEquals(message.producerMetadata.logicalTimestamp, expectedTimestamp);
      }
    }
  }

  @DataProvider(name = "writerHookThreadModes")
  public Object[][] writerHookThreadModes() {
    return new Object[][] { { 0, 0 }, { 0, 1 }, { 2, 0 }, { 2, 1 } };
  }

  @Test(dataProvider = "writerHookThreadModes", timeOut = 30000)
  public void testBlockingHookDefersSubmissionAndDurabilityWaitsForBrokerAck(int workerCount, int callbackThreadCount)
      throws Exception {
    CountDownLatch hookEntered = new CountDownLatch(1);
    CountDownLatch releaseHook = new CountDownLatch(1);
    AtomicReference<Thread> hookThread = new AtomicReference<>();
    AtomicReference<Thread> callerThread = new AtomicReference<>();
    BlockingQueue<PubSubProducerCallback> acknowledgments = new LinkedBlockingQueue<>();
    PubSubProducerAdapter adapter = createPubSubProducer((message, callback) -> acknowledgments.add(callback));
    VeniceWriterHook writerHook = (operation, keySize, valueSize) -> {
      hookThread.set(Thread.currentThread());
      hookEntered.countDown();
      try {
        if (!releaseHook.await(10, TimeUnit.SECONDS)) {
          throw new VeniceException("Timed out waiting to release writer hook");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new VeniceException("Interrupted while waiting to release writer hook", e);
      }
    };

    try (TestableVeniceProducer producer =
        createRealWriterProducer(adapter, workerCount, callbackThreadCount, writerHook)) {
      ExecutorService submitter = Executors.newSingleThreadExecutor(new DaemonThreadFactory("hook-test-submitter"));
      try {
        CompletableFuture<CompletableFuture<DurableWrite>> submission = CompletableFuture.supplyAsync(() -> {
          callerThread.set(Thread.currentThread());
          return producer.asyncPut(1, "value");
        }, submitter);

        assertTrue(hookEntered.await(5, TimeUnit.SECONDS));
        verify(adapter, never()).sendMessage(any(), any(), any(), any(), any(), any());
        if (workerCount == 0) {
          assertSame(hookThread.get(), callerThread.get());
          assertFalse(submission.isDone(), "Worker-zero submission must block inline in the hook");
        } else {
          assertNotSame(hookThread.get(), callerThread.get());
          assertFalse(submission.get(5, TimeUnit.SECONDS).isDone());
        }

        releaseHook.countDown();
        CompletableFuture<DurableWrite> durableWrite = submission.get(5, TimeUnit.SECONDS);
        PubSubProducerCallback acknowledgment = acknowledgments.poll(5, TimeUnit.SECONDS);
        assertNotNull(acknowledgment, "Releasing the hook must allow broker submission");
        assertFalse(durableWrite.isDone(), "Hook completion alone must not complete durability");
        CompletableFuture<Thread> completionThread = durableWrite.thenApply(ignored -> Thread.currentThread());
        acknowledgment.onCompletion(null, null);
        durableWrite.get(5, TimeUnit.SECONDS);
        if (callbackThreadCount == 0) {
          assertSame(completionThread.get(5, TimeUnit.SECONDS), Thread.currentThread());
        } else {
          assertNotSame(completionThread.get(5, TimeUnit.SECONDS), Thread.currentThread());
        }
      } finally {
        releaseHook.countDown();
        submitter.shutdownNow();
        assertTrue(submitter.awaitTermination(5, TimeUnit.SECONDS), "The submitting thread must terminate");
      }
    }
  }

  @Test(dataProvider = "writerHookThreadModes", timeOut = 30000)
  public void testWriterHookFailureCompletesFutureWithoutSubmission(int workerCount, int callbackThreadCount)
      throws Exception {
    IllegalStateException hookFailure = new IllegalStateException("hook failure");
    VeniceWriterHook writerHook = (operation, keySize, valueSize) -> {
      throw hookFailure;
    };
    PubSubProducerAdapter adapter = createPubSubProducer((message, callback) -> callback.onCompletion(null, null));

    try (TestableVeniceProducer producer =
        createRealWriterProducer(adapter, workerCount, callbackThreadCount, writerHook)) {
      List<CompletableFuture<DurableWrite>> writes = Arrays.asList(
          producer.asyncPut(1, "value"),
          producer.asyncDelete(1),
          producer.asyncUpdate(1, builder -> builder.setNewFieldValue("name", "new-value")));
      for (CompletableFuture<DurableWrite> write: writes) {
        VeniceException failure = assertWriteRejected(write, "Write operation failed: hook failure");
        assertSame(failure.getCause(), hookFailure);
      }
      verify(adapter, never()).sendMessage(any(), any(), any(), any(), any(), any());
    }
  }

  @Test(dataProvider = "writerHookThreadModes", timeOut = 30000)
  public void testRejectedWritesDoNotInvokeWriterHook(int workerCount, int callbackThreadCount) throws Exception {
    VeniceWriterHook writerHook = mock(VeniceWriterHook.class);
    PubSubProducerAdapter adapter = createPubSubProducer((message, callback) -> callback.onCompletion(null, null));

    try (TestableVeniceProducer producer =
        createRealWriterProducer(adapter, workerCount, callbackThreadCount, writerHook)) {
      assertWriteRejected(producer.asyncPut(-1, 1, "value"), "Logical time must be a non-negative value");
      assertWriteRejected(producer.asyncDelete(-1, 1), "Logical time must be a non-negative value");
      assertWriteRejected(
          producer.asyncUpdate(-1, 1, builder -> builder.setNewFieldValue("name", "new-value")),
          "Logical time must be a non-negative value");
      assertWriteRejected(producer.asyncPut(1, null), "Write operation failed:");
      String keyError = workerCount == 0 ? "Write operation failed:" : "Key serialization failed";
      assertWriteRejected(producer.asyncDelete(null), keyError);

      when(mockSchemaReader.getValueSchemaId(any(Schema.class))).thenThrow(new VeniceException("missing schema"));
      assertWriteRejected(producer.asyncPut(1, "value"), "Could not find a registered schema id");
      when(mockSchemaReader.getLatestUpdateSchema()).thenReturn(null);
      assertWriteRejected(producer.asyncUpdate(1, builder -> {}), "Update schema not found");
      when(mockSchemaReader.getLatestUpdateSchema())
          .thenReturn(new DerivedSchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, 1, UPDATE_SCHEMA));
      assertWriteRejected(producer.asyncUpdate(1, builder -> {}), "Could not find a registered schema id");

      producer.close();
      assertWriteRejected(producer.asyncPut(1, "value"), "Producer is already closed");
      assertWriteRejected(producer.asyncDelete(1), "Producer is already closed");
      assertWriteRejected(producer.asyncUpdate(1, builder -> {}), "Producer is already closed");
      verifyNoInteractions(writerHook);
      verify(adapter, never()).sendMessage(any(), any(), any(), any(), any(), any());
    }
  }

  private VeniceException assertWriteRejected(CompletableFuture<DurableWrite> write, String message) {
    ExecutionException failure = expectThrows(ExecutionException.class, () -> write.get(5, TimeUnit.SECONDS));
    assertTrue(failure.getCause() instanceof VeniceException);
    VeniceException cause = (VeniceException) failure.getCause();
    assertTrue(cause.getMessage().contains(message), cause.getMessage());
    return cause;
  }

  private PubSubProducerAdapter createPubSubProducer(
      BiConsumer<KafkaMessageEnvelope, PubSubProducerCallback> onDataMessage) {
    PubSubProducerAdapter adapter = mock(PubSubProducerAdapter.class);
    doAnswer(invocation -> {
      KafkaKey key = invocation.getArgument(2);
      KafkaMessageEnvelope message = invocation.getArgument(3);
      PubSubProducerCallback callback = invocation.getArgument(5);
      if (key.isControlMessage()) {
        if (callback != null) {
          callback.onCompletion(null, null);
        }
      } else {
        onDataMessage.accept(message, callback);
      }
      return CompletableFuture.completedFuture(null);
    }).when(adapter).sendMessage(any(), any(), any(), any(), any(), any());
    return adapter;
  }

  private TestableVeniceProducer createRealWriterProducer(
      PubSubProducerAdapter adapter,
      int workerCount,
      int callbackThreadCount,
      VeniceWriterHook writerHook) {
    when(mockSchemaReader.getLatestUpdateSchema()).thenReturn(new DerivedSchemaEntry(1, 1, UPDATE_SCHEMA));
    TestableVeniceProducer producer = new TestableVeniceProducer(adapter);
    producer.configure(
        TEST_STORE,
        getProducerConfigs(workerCount, 10000, callbackThreadCount),
        null,
        mockSchemaReader,
        null,
        writerHook);
    return producer;
  }

  private VeniceProperties getProducerConfigs(int workerCount, int queueCapacity, int callbackThreadCount) {
    Properties props = new Properties();
    props.setProperty(CLIENT_PRODUCER_WORKER_COUNT, String.valueOf(workerCount));
    props.setProperty(CLIENT_PRODUCER_WORKER_QUEUE_CAPACITY, String.valueOf(queueCapacity));
    props.setProperty(CLIENT_PRODUCER_CALLBACK_THREAD_COUNT, String.valueOf(callbackThreadCount));
    return new VeniceProperties(props);
  }

  private TestableVeniceProducer createProducer(int workerCount, int queueCapacity) {
    TestableVeniceProducer producer = new TestableVeniceProducer(mockVeniceWriter);
    producer
        .configure(TEST_STORE, getProducerConfigs(workerCount, queueCapacity, 0), null, mockSchemaReader, null, null);
    return producer;
  }

  /**
   * Testable subclass of AbstractVeniceProducer that allows injecting mock dependencies.
   */
  private static class TestableVeniceProducer extends AbstractVeniceProducer<Integer, String> {
    private final VeniceWriter<byte[], byte[], byte[]> mockWriter;
    private final PubSubProducerAdapter adapter;
    private VeniceWriterOptions capturedWriterOptions;

    TestableVeniceProducer(VeniceWriter<byte[], byte[], byte[]> mockWriter) {
      this.mockWriter = mockWriter;
      this.adapter = null;
    }

    TestableVeniceProducer(PubSubProducerAdapter adapter) {
      this.mockWriter = null;
      this.adapter = adapter;
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
      capturedWriterOptions = writerOptions;
      if (adapter != null) {
        return new VeniceWriter<>(writerOptions, new VeniceProperties(properties), adapter);
      }
      return mockWriter;
    }
  }
}
