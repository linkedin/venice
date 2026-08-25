package com.linkedin.venice.samza;

import static com.linkedin.venice.ConfigKeys.VENICE_SYSTEM_PRODUCER_WORKER_COUNT;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.RouterBasedPushMonitor;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.mockito.InOrder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceSystemProducerDispatchTest {
  @Test
  public void testObjectSendReturnsAfterEnqueueWithoutWaitingForCoreWriter() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch writerEntered = new CountDownLatch(1);
    CountDownLatch releaseWriter = new CountDownLatch(1);
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      writerEntered.countDown();
      releaseWriter.await();
      return new CompletableFuture<>();
    });
    VeniceSystemProducer producer = buildStartedProducer(writer, 1, false, -1);
    try {
      CompletableFuture<CompletableFuture<Void>> invocation =
          CompletableFuture.supplyAsync(() -> producer.send((Object) "key", "value"));
      CompletableFuture<Void> durableFuture = invocation.get(2, TimeUnit.SECONDS);

      assertTrue(writerEntered.await(5, TimeUnit.SECONDS));
      assertFalse(durableFuture.isDone());
    } finally {
      releaseWriter.countDown();
      producer.stop();
    }
  }

  @Test
  public void testDirectPutWaitsForSubmissionButNotDurableAck() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch writerEntered = new CountDownLatch(1);
    CountDownLatch releaseWriter = new CountDownLatch(1);
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      writerEntered.countDown();
      releaseWriter.await();
      return new CompletableFuture<>();
    });
    VeniceSystemProducer producer = buildStartedProducer(writer, 1, false, -1);
    try {
      CompletableFuture<CompletableFuture<Void>> put =
          CompletableFuture.supplyAsync(() -> producer.put("key", "value"));
      assertTrue(writerEntered.await(5, TimeUnit.SECONDS));
      assertFalse(put.isDone());

      releaseWriter.countDown();
      CompletableFuture<Void> durableFuture = put.get(5, TimeUnit.SECONDS);
      assertFalse(durableFuture.isDone());
    } finally {
      releaseWriter.countDown();
      producer.stop();
    }
  }

  @Test
  public void testSamzaSendWaitsForSubmissionButNotDurableAck() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch writerEntered = new CountDownLatch(1);
    CountDownLatch releaseWriter = new CountDownLatch(1);
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      writerEntered.countDown();
      releaseWriter.await();
      return new CompletableFuture<>();
    });
    VeniceSystemProducer producer = buildStartedProducer(writer, 1, false, -1);
    OutgoingMessageEnvelope envelope =
        new OutgoingMessageEnvelope(new SystemStream("venice", "test_store"), "key", "value");
    try {
      CompletableFuture<Void> send = CompletableFuture.runAsync(() -> producer.send("source", envelope));
      assertTrue(writerEntered.await(5, TimeUnit.SECONDS));
      assertFalse(send.isDone());

      releaseWriter.countDown();
      send.get(5, TimeUnit.SECONDS);
    } finally {
      releaseWriter.countDown();
      producer.stop();
    }
  }

  @Test
  public void testSamzaSendSurfacesSynchronousWorkerFailure() {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    VeniceException writerFailure = new VeniceException("synchronous writer failure");
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenThrow(writerFailure);
    VeniceSystemProducer producer = buildStartedProducer(writer, 1, false, -1);
    OutgoingMessageEnvelope envelope =
        new OutgoingMessageEnvelope(new SystemStream("venice", "test_store"), "key", "value");

    assertThrows(VeniceException.class, () -> producer.send("source", envelope));
    assertThrows(VeniceException.class, producer::stop);
  }

  @Test
  public void testAsyncFailureIsStickyAcrossSendFlushAndStop() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    AtomicReference<PubSubProducerCallback> callback = new AtomicReference<>();
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      callback.set(invocation.getArgument(4));
      return new CompletableFuture<>();
    });
    VeniceSystemProducer producer = buildStartedProducer(writer, 1, false, -1);

    CompletableFuture<Void> durableFuture = producer.put("key", "value");
    verify(writer, timeout(5000)).put(any(), any(), eq(1), anyLong(), any());
    RuntimeException asyncFailure = new RuntimeException("async broker failure");
    callback.get().onCompletion(null, asyncFailure);
    assertThrows(ExecutionException.class, () -> durableFuture.get(5, TimeUnit.SECONDS));
    assertTrue(durableFuture.isCompletedExceptionally());

    assertThrows(VeniceException.class, () -> producer.put("later", "value"));
    assertThrows(VeniceException.class, () -> producer.flush("source"));
    assertThrows(VeniceException.class, producer::stop);
    verify(writer).flush();
    verify(writer).close();
  }

  @Test
  public void testSerializationAndLogicalTimestampAreCapturedBeforeEnqueue() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch firstDeleteEntered = new CountDownLatch(1);
    CountDownLatch releaseFirstDelete = new CountDownLatch(1);
    AtomicInteger deleteCount = new AtomicInteger();
    List<Long> deleteTimestamps = Collections.synchronizedList(new ArrayList<>());
    when(writer.delete(any(), anyLong(), any())).thenAnswer(invocation -> {
      deleteTimestamps.add(invocation.getArgument(1));
      if (deleteCount.getAndIncrement() == 0) {
        firstDeleteEntered.countDown();
        releaseFirstDelete.await();
      }
      return new CompletableFuture<>();
    });

    AtomicReference<byte[]> updateBytes = new AtomicReference<>();
    AtomicReference<Long> updateTimestamp = new AtomicReference<>();
    when(writer.update(any(), any(), eq(1), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      updateBytes.set(invocation.getArgument(1));
      updateTimestamp.set(invocation.getArgument(4));
      return new CompletableFuture<>();
    });

    VeniceSystemProducer producer = buildStartedProducer(writer, 1, true, 1);
    Schema schema = new Schema.Parser()
        .parse("{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");
    GenericRecord update = new GenericData.Record(schema);
    update.put("name", "before");
    RecordSerializer<Object> serializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(schema);
    byte[] expectedBytes = serializer.serialize(update);
    try {
      producer.send((Object) "blocker", null);
      assertTrue(firstDeleteEntered.await(5, TimeUnit.SECONDS));

      producer.send((Object) "update-key", new VeniceObjectWithTimestamp(update, 1234));
      producer.send((Object) "delete-key", new VeniceObjectWithTimestamp(null, 5678));
      update.put("name", "after");

      releaseFirstDelete.countDown();
      producer.flush("source");

      assertTrue(Arrays.equals(updateBytes.get(), expectedBytes));
      assertEquals(updateTimestamp.get().longValue(), 1234);
      assertTrue(deleteTimestamps.contains(5678L));
    } finally {
      releaseFirstDelete.countDown();
      producer.stop();
    }
  }

  @Test
  public void testDispatcherUsesCoreWriterPartition() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    AtomicReference<byte[]> routedKey = new AtomicReference<>();
    AtomicReference<byte[]> writtenKey = new AtomicReference<>();
    AtomicReference<String> workerThread = new AtomicReference<>();
    when(writer.getPartitionId(any())).thenAnswer(invocation -> {
      routedKey.set(invocation.getArgument(0));
      return 3;
    });
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      writtenKey.set(invocation.getArgument(0));
      workerThread.set(Thread.currentThread().getName());
      return new CompletableFuture<>();
    });
    VeniceSystemProducer producer = buildStartedProducer(writer, 2, false, -1);
    try {
      producer.put("key", "value");
      producer.flush("source");

      assertTrue(Arrays.equals(routedKey.get(), writtenKey.get()));
      assertTrue(workerThread.get().contains("venice-system-producer-worker-test_store-1"));
    } finally {
      producer.stop();
    }
  }

  @Test
  public void testFlushWaitsForPriorCoreInvocation() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch writerEntered = new CountDownLatch(1);
    CountDownLatch releaseWriter = new CountDownLatch(1);
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      writerEntered.countDown();
      releaseWriter.await();
      return new CompletableFuture<>();
    });
    VeniceSystemProducer producer = buildStartedProducer(writer, 1, false, -1);
    try {
      producer.send((Object) "key", "value");
      assertTrue(writerEntered.await(5, TimeUnit.SECONDS));

      CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> producer.flush("source"));
      Thread.sleep(200);
      verify(writer, never()).flush();

      releaseWriter.countDown();
      flush.get(5, TimeUnit.SECONDS);
      verify(writer).flush();
    } finally {
      releaseWriter.countDown();
      producer.stop();
    }
  }

  @Test
  public void testStopDrainsBeforeClosingWriter() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch writerEntered = new CountDownLatch(1);
    CountDownLatch releaseWriter = new CountDownLatch(1);
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      writerEntered.countDown();
      releaseWriter.await();
      return new CompletableFuture<>();
    });
    VeniceSystemProducer producer = buildStartedProducer(writer, 1, false, -1);
    producer.send((Object) "key", "value");
    assertTrue(writerEntered.await(5, TimeUnit.SECONDS));

    CompletableFuture<Void> stop = CompletableFuture.runAsync(producer::stop);
    Thread.sleep(200);
    verify(writer, never()).close();

    releaseWriter.countDown();
    stop.get(5, TimeUnit.SECONDS);
    InOrder shutdownOrder = inOrder(writer);
    shutdownOrder.verify(writer).flush();
    shutdownOrder.verify(writer).close();
  }

  @Test
  public void testInterruptedFlushFailsAndPreservesInterrupt() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    CountDownLatch writerEntered = new CountDownLatch(1);
    CountDownLatch releaseWriter = new CountDownLatch(1);
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      writerEntered.countDown();
      releaseWriter.await();
      return new CompletableFuture<>();
    });
    VeniceSystemProducer producer = buildStartedProducer(writer, 1, false, -1);
    producer.send((Object) "key", "value");
    assertTrue(writerEntered.await(5, TimeUnit.SECONDS));

    AtomicBoolean flushFailed = new AtomicBoolean(false);
    AtomicBoolean interruptPreserved = new AtomicBoolean(false);
    Thread flushThread = new Thread(() -> {
      try {
        producer.flush("source");
      } catch (VeniceException exception) {
        flushFailed.set(true);
        interruptPreserved.set(Thread.currentThread().isInterrupted());
      }
    });
    flushThread.start();
    Thread.sleep(200);
    flushThread.interrupt();
    flushThread.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(flushFailed.get());
    assertTrue(interruptPreserved.get());
    releaseWriter.countDown();
    producer.stop();
  }

  @Test
  public void testWorkerCountZeroRestoresInlineMode() {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    AtomicReference<Thread> writerThread = new AtomicReference<>();
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      writerThread.set(Thread.currentThread());
      return new CompletableFuture<>();
    });
    VeniceSystemProducer producer = buildStartedProducer(writer, 0, false, -1);
    try {
      producer.put("key", "value");
      assertEquals(writerThread.get(), Thread.currentThread());
    } finally {
      producer.stop();
    }
  }

  @Test(dataProvider = "inlinePushTypes")
  public void testBatchAndStreamReprocessingRemainInline(Version.PushType pushType) {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mockWriter();
    AtomicReference<Thread> writerThread = new AtomicReference<>();
    when(writer.put(any(), any(), eq(1), anyLong(), any())).thenAnswer(invocation -> {
      writerThread.set(Thread.currentThread());
      return new CompletableFuture<>();
    });
    VeniceSystemProducer producer = buildStartedProducer(writer, 4, false, -1, pushType);
    if (pushType == Version.PushType.STREAM_REPROCESSING) {
      RouterBasedPushMonitor pushMonitor = mock(RouterBasedPushMonitor.class);
      when(pushMonitor.getCurrentStatus()).thenReturn(ExecutionStatus.COMPLETED);
      producer.setPushMonitor(pushMonitor);
    }
    try {
      producer.put("key", "value");
      assertEquals(writerThread.get(), Thread.currentThread());
    } finally {
      producer.stop();
    }
  }

  @DataProvider(name = "inlinePushTypes")
  public Object[][] inlinePushTypes() {
    return new Object[][] { { Version.PushType.BATCH }, { Version.PushType.STREAM_REPROCESSING } };
  }

  private AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter() {
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mock(AbstractVeniceWriter.class);
    when(writer.getPartitionId(any())).thenReturn(0);
    return writer;
  }

  private VeniceSystemProducer buildStartedProducer(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      int workerCount,
      boolean writeComputationEnabled,
      int derivedSchemaId) {
    return buildStartedProducer(writer, workerCount, writeComputationEnabled, derivedSchemaId, Version.PushType.STREAM);
  }

  private VeniceSystemProducer buildStartedProducer(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      int workerCount,
      boolean writeComputationEnabled,
      int derivedSchemaId,
      Version.PushType pushType) {
    Map<String, String> configs = new HashMap<>();
    configs.put(VENICE_SYSTEM_PRODUCER_WORKER_COUNT, Integer.toString(workerCount));
    VeniceSystemProducer producer = new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName("test_store")
            .setPushType(pushType)
            .setSamzaJobId("job-id")
            .setRunningFabric("dc-0")
            .setFactory(mock(VeniceSystemFactory.class))
            .setDiscoveryUrl("discovery-url")
            .setSamzaConfig(new MapConfig(configs))
            .build());
    VeniceSystemProducer producerSpy = spy(producer);
    doNothing().when(producerSpy).setupClientsAndReInitProvider();
    doNothing().when(producerSpy).refreshSchemaCache();
    producerSpy.setControllerClient(buildController(writeComputationEnabled, derivedSchemaId, pushType));
    doReturn(writer).when(producerSpy).getVeniceWriter(any());
    producerSpy.start();
    return producerSpy;
  }

  private ControllerClient buildController(
      boolean writeComputationEnabled,
      int derivedSchemaId,
      Version.PushType pushType) {
    ControllerClient controller = mock(ControllerClient.class);
    SchemaResponse keySchema = new SchemaResponse();
    keySchema.setSchemaStr("\"string\"");
    when(controller.getKeySchema(anyString())).thenReturn(keySchema);

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setKafkaTopic(
        pushType == Version.PushType.BATCH
            ? "test_store_v1"
            : pushType == Version.PushType.STREAM_REPROCESSING ? "test_store_v1_sr" : "test_store_rt");
    versionCreationResponse.setKafkaBootstrapServers("kafka:9092");
    versionCreationResponse.setVersion(1);
    when(
        controller.requestTopicForWrites(
            anyString(),
            anyLong(),
            any(),
            anyString(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            any(),
            any(),
            any(),
            anyBoolean(),
            anyLong())).thenReturn(versionCreationResponse);

    StoreInfo storeInfo = new StoreInfo();
    List<Version> versions = new ArrayList<>();
    if (pushType.isBatchOrStreamReprocessing()) {
      versions.add(new VersionImpl("test_store", 1, "test_store_v1"));
    }
    storeInfo.setVersions(versions);
    storeInfo.setWriteComputationEnabled(writeComputationEnabled);
    StoreResponse storeResponse = new StoreResponse();
    storeResponse.setStore(storeInfo);
    when(controller.getStore(anyString())).thenReturn(storeResponse);

    SchemaResponse valueSchema = new SchemaResponse();
    valueSchema.setId(1);
    valueSchema.setDerivedSchemaId(derivedSchemaId);
    when(controller.getValueOrDerivedSchemaId(anyString(), anyString())).thenReturn(valueSchema);
    return controller;
  }
}
