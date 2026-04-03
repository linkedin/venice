package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.findMethod;
import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.freshPcs;
import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.pcsFromCheckpoint;
import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.setField;
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for unique key count: PCS field operations, OffsetRecord persistence, schema evolution,
 * header encoding/decoding, and production code path verification via doCallRealMethod/reflection.
 */
public class UniqueKeyCountTest {
  private static final int PARTITION = 1;
  private static final int USER_SCHEMA_ID = 5;
  private static final int CHUNK_SCHEMA_ID = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
  private static final int CHUNK_MANIFEST_SCHEMA_ID =
      AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
  private static final byte[] KEY_BYTES = "test-key".getBytes();
  private static final ByteBuffer VALUE_PAYLOAD = ByteBuffer.wrap("test-value".getBytes());
  private static final ByteBuffer EMPTY_RMD = ByteBuffer.allocate(0);

  private ActiveActiveStoreIngestionTask ingestionTask;
  private AbstractStorageEngine storageEngine;
  private PartitionConsumptionState pcs;
  private Map<Integer, PartitionConsumptionState> pcsMap;

  @BeforeMethod
  public void setUp() {
    ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    storageEngine = mock(AbstractStorageEngine.class);
    pcs = mock(PartitionConsumptionState.class);
    pcsMap = new VeniceConcurrentHashMap<>();
    pcsMap.put(PARTITION, pcs);
    doReturn(pcsMap).when(ingestionTask).getPartitionConsumptionStateMap();
    doReturn(storageEngine).when(ingestionTask).getStorageEngine();
    doCallRealMethod().when(ingestionTask).checkStorageOperationCommonInvalidPattern(any(), any());
    doCallRealMethod().when(ingestionTask).getStorageOperationTypeForPut(anyInt(), any());
    doCallRealMethod().when(ingestionTask).getStorageOperationTypeForDelete(anyInt(), any());
  }

  private Put createBatchPut(int schemaId) {
    Put put = new Put();
    put.putValue = VALUE_PAYLOAD.duplicate();
    put.schemaId = schemaId;
    put.replicationMetadataPayload = EMPTY_RMD.duplicate();
    return put;
  }

  private Delete createBatchDelete() {
    Delete delete = new Delete();
    delete.schemaId = -1;
    delete.replicationMetadataPayload = EMPTY_RMD.duplicate();
    return delete;
  }

  private void setupForStorageEngineTests(boolean addRmdEnabled) throws Exception {
    setField(ingestionTask, "storageEngine", storageEngine);
    setField(ingestionTask, "addRmdToBatchPushForHybridStores", addRmdEnabled);
    if (addRmdEnabled) {
      setField(ingestionTask, "defaultBatchRmdBytes", new byte[] { 0, 0, 0, 0 });
      setField(ingestionTask, "defaultBatchRmdWithSchemaIdPrefix", new byte[] { 0, 0, 0, 5, 0, 0, 0, 0 });
    }
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();
    doCallRealMethod().when(ingestionTask).putInStorageEngine(anyInt(), any(), any(Put.class));
    doCallRealMethod().when(ingestionTask).removeFromStorageEngine(anyInt(), any(), any(Delete.class));
  }

  private void setupForTrackUniqueKeyCount(
      boolean batchCountingEnabled,
      boolean hybridCountingEnabled,
      boolean isActiveActive) throws Exception {
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);
    doReturn(batchCountingEnabled).when(mockServerConfig).isUniqueKeyCountForAllBatchPushEnabled();
    doReturn(hybridCountingEnabled).when(mockServerConfig).isUniqueKeyCountForHybridStoreEnabled();
    setField(ingestionTask, "serverConfig", mockServerConfig);
    setField(ingestionTask, "isActiveActiveReplicationEnabled", isActiveActive);
  }

  private DefaultPubSubMessage createMockConsumerRecord(PubSubMessageHeaders headers) {
    DefaultPubSubMessage record = mock(DefaultPubSubMessage.class);
    doReturn(headers).when(record).getPubSubMessageHeaders();
    KafkaKey kafkaKey = mock(KafkaKey.class);
    doReturn(new byte[] { 0, 0, 0, 1 }).when(kafkaKey).getKey();
    doReturn(kafkaKey).when(record).getKey();
    return record;
  }

  private void invokeTrackUniqueKeyCount(
      DefaultPubSubMessage consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      MessageType messageType,
      int writerSchemaId) throws Exception {
    Method method = findMethod(
        StoreIngestionTask.class,
        "trackUniqueKeyCount",
        DefaultPubSubMessage.class,
        PartitionConsumptionState.class,
        LeaderProducedRecordContext.class,
        MessageType.class,
        int.class);
    method.setAccessible(true);
    method.invoke(
        ingestionTask,
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        messageType,
        writerSchemaId);
  }

  private void setupForProcessMessageTests(boolean uniqueKeyCountEnabled) throws Exception {
    doCallRealMethod().when(ingestionTask)
        .processMessageAndMaybeProduceToKafka(any(), any(), anyInt(), anyString(), anyInt(), anyLong(), anyLong());
    setField(ingestionTask, "uniqueKeyCountForHybridStoreEnabled", uniqueKeyCountEnabled);
    doReturn(true).when(ingestionTask).hasViewWriters();
  }

  private MergeConflictResultWrapper createMockMergeConflictResultWrapper(
      boolean isUpdateIgnored,
      boolean wasAlive,
      boolean isAlive) {
    MergeConflictResult mcResult = mock(MergeConflictResult.class);
    doReturn(isUpdateIgnored).when(mcResult).isUpdateIgnored();
    doReturn(isAlive ? ByteBuffer.wrap("new-value".getBytes()) : null).when(mcResult).getNewValue();
    doReturn(1).when(mcResult).getValueSchemaId();
    doReturn(false).when(mcResult).doesResultReuseInput();
    MergeConflictResultWrapper wrapper = mock(MergeConflictResultWrapper.class);
    doReturn(mcResult).when(wrapper).getMergeConflictResult();
    doReturn(wasAlive).when(wrapper).wasOldValueAlive();
    doReturn(Lazy.of(() -> wasAlive ? ByteBuffer.wrap("old-value".getBytes()) : null)).when(wrapper)
        .getOldValueByteBufferProvider();
    ByteBufferValueRecord<ByteBuffer> oldValueRecord =
        wasAlive ? new ByteBufferValueRecord<>(ByteBuffer.wrap("old".getBytes()), 1) : null;
    doReturn(Lazy.of(() -> oldValueRecord)).when(wrapper).getOldValueProvider();
    doReturn(Lazy.of(() -> (GenericRecord) null)).when(wrapper).getValueProvider();
    doReturn(isAlive ? ByteBuffer.wrap("updated".getBytes()) : null).when(wrapper).getUpdatedValueBytes();
    doReturn(ByteBuffer.wrap("rmd".getBytes())).when(wrapper).getUpdatedRmdBytes();
    return wrapper;
  }

  private PubSubMessageProcessedResultWrapper createWrapperWithResult(MergeConflictResultWrapper mcWrapper) {
    DefaultPubSubMessage consumerRecord = mock(DefaultPubSubMessage.class);
    KafkaKey kafkaKey = mock(KafkaKey.class);
    doReturn(new byte[] { 1, 2, 3 }).when(kafkaKey).getKey();
    doReturn(kafkaKey).when(consumerRecord).getKey();
    doReturn(mock(PubSubPosition.class)).when(consumerRecord).getPosition();
    PubSubMessageProcessedResultWrapper wrapper = new PubSubMessageProcessedResultWrapper(consumerRecord);
    wrapper.setProcessedResult(new PubSubMessageProcessedResult(mcWrapper));
    return wrapper;
  }

  private PartitionConsumptionState createMockPcsForTrack(boolean postEop, long uniqueKeyCount) {
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(postEop).when(mockPcs).isEndOfPushReceived();
    doReturn(uniqueKeyCount).when(mockPcs).getUniqueKeyCount();
    return mockPcs;
  }

  private PubSubMessageHeaders createSignalHeaders(byte signalValue) {
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(new PubSubMessageHeader(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { signalValue }));
    return headers;
  }

  private StoreIngestionTask setupProcessEndOfPush(boolean uniqueKeyCountEnabled) throws Exception {
    StoreIngestionTask sitMock = mock(StoreIngestionTask.class);
    doCallRealMethod().when(sitMock).processEndOfPush(any(), any(), any(), any());
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);
    doReturn(uniqueKeyCountEnabled).when(mockServerConfig).isUniqueKeyCountForAllBatchPushEnabled();
    setField(sitMock, "serverConfig", mockServerConfig);
    setField(sitMock, "storageEngine", mock(StorageEngine.class));
    setField(sitMock, "cacheBackend", Optional.empty());
    setField(sitMock, "isDataRecovery", false);
    setField(sitMock, "ingestionNotificationDispatcher", mock(IngestionNotificationDispatcher.class));
    setField(sitMock, "storageMetadataService", mock(StorageMetadataService.class));
    StoragePartitionConfig mockSpc = mock(StoragePartitionConfig.class);
    doReturn(false).when(mockSpc).isDeferredWrite();
    doReturn(mockSpc).when(sitMock).getStoragePartitionConfig(anyBoolean(), any());
    return sitMock;
  }

  private void setupPcsNullFallThrough() throws Exception {
    setField(ingestionTask, "storageEngine", storageEngine);
    setField(ingestionTask, "addRmdToBatchPushForHybridStores", true);
    setField(ingestionTask, "defaultBatchRmdBytes", new byte[] { 0, 0, 0, 0 });
    setField(ingestionTask, "defaultBatchRmdWithSchemaIdPrefix", new byte[] { 0, 0, 0, 5, 0, 0, 0, 0 });
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(new VeniceConcurrentHashMap<>()).when(ingestionTask).getPartitionConsumptionStateMap();
  }

  private void verifyNoCountChange(PartitionConsumptionState target) {
    verify(target, never()).incrementUniqueKeyCount();
    verify(target, never()).decrementUniqueKeyCount();
  }

  @DataProvider(name = "serializationValues")
  public Object[][] serializationValues() {
    return new Object[][] { { -1L }, { 0L }, { 100L }, { 54321L }, { 100_000_000L } };
  }

  @DataProvider(name = "batchPutSchemaIds")
  public Object[][] batchPutSchemaIds() {
    return new Object[][] { { USER_SCHEMA_ID, "Non-chunked PUT" }, { CHUNK_SCHEMA_ID, "Chunk fragment" },
        { CHUNK_MANIFEST_SCHEMA_ID, "Chunk manifest" } };
  }

  @DataProvider(name = "putRmdWithSchemaIds")
  public Object[][] putRmdWithSchemaIds() {
    return new Object[][] { { USER_SCHEMA_ID, "non-chunked PUT" }, { CHUNK_MANIFEST_SCHEMA_ID, "manifest PUT" } };
  }

  @DataProvider(name = "putFallThroughCases")
  public Object[][] putFallThroughCases() {
    return new Object[][] { { false, false, false, "RMD disabled" }, { true, false, true, "post-EOP" },
        { true, true, false, "DaVinci client" } };
  }

  @DataProvider(name = "deleteFallThroughCases")
  public Object[][] deleteFallThroughCases() {
    return new Object[][] { { false, false, false, "RMD disabled" }, { true, true, false, "DaVinci client" } };
  }

  @DataProvider(name = "batchCountingSkippedCases")
  public Object[][] batchCountingSkippedCases() {
    return new Object[][] { { false, false, MessageType.PUT, USER_SCHEMA_ID, "config disabled" },
        { true, false, MessageType.DELETE, USER_SCHEMA_ID, "DELETE message type" },
        { true, true, MessageType.PUT, USER_SCHEMA_ID, "post-EOP" } };
  }

  @DataProvider(name = "followerSignalSkippedCases")
  public Object[][] followerSignalSkippedCases() {
    return new Object[][] { { false, true, true, 5L, false, MessageType.PUT, USER_SCHEMA_ID, "config disabled" },
        { true, true, true, -1L, false, MessageType.PUT, USER_SCHEMA_ID, "baseline not established" },
        { true, false, true, 5L, false, MessageType.PUT, USER_SCHEMA_ID, "non-AA store" },
        { true, true, true, 5L, true, MessageType.PUT, USER_SCHEMA_ID, "leader context present" },
        { true, true, true, 5L, false, MessageType.UPDATE, USER_SCHEMA_ID, "UPDATE message type" } };
  }

  @Test
  public void testBatchKeyCountAndFinalize() {
    PartitionConsumptionState localPcs = freshPcs();
    assertEquals(localPcs.getUniqueKeyCount(), -1L);
    for (int i = 0; i < 5; i++) {
      localPcs.incrementUniqueKeyCountForBatchRecord(UniqueKeyCountTestUtils.sortedKeyBytes(i));
    }
    assertEquals(localPcs.getUniqueKeyCount(), 5L);
    localPcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(localPcs.getUniqueKeyCount(), 5L);
    // Empty batch finalize yields 0
    PartitionConsumptionState emptyPcs = freshPcs();
    emptyPcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(emptyPcs.getUniqueKeyCount(), 0L);
    // Verify increment/decrement works post-finalize (RT signals)
    localPcs.incrementUniqueKeyCount();
    assertEquals(localPcs.getUniqueKeyCount(), 6L);
    localPcs.decrementUniqueKeyCount();
    assertEquals(localPcs.getUniqueKeyCount(), 5L);
  }

  @Test
  public void testBatchDedupSkipsDuplicateKeys() {
    PartitionConsumptionState localPcs = freshPcs();
    for (int i = 0; i < 5; i++) {
      localPcs.incrementUniqueKeyCountForBatchRecord(UniqueKeyCountTestUtils.sortedKeyBytes(i));
    }
    assertEquals(localPcs.getUniqueKeyCount(), 5L);
    // Replay same keys (speculative execution) -- all should be skipped
    for (int i = 0; i < 5; i++) {
      localPcs.incrementUniqueKeyCountForBatchRecord(UniqueKeyCountTestUtils.sortedKeyBytes(i));
    }
    assertEquals(localPcs.getUniqueKeyCount(), 5L);
    // Same key inserted twice consecutively
    byte[] sameKey = UniqueKeyCountTestUtils.sortedKeyBytes(42);
    localPcs.incrementUniqueKeyCountForBatchRecord(sameKey);
    assertEquals(localPcs.getUniqueKeyCount(), 6L);
    localPcs.incrementUniqueKeyCountForBatchRecord(sameKey);
    assertEquals(localPcs.getUniqueKeyCount(), 6L);
  }

  @Test
  public void testFinalizeDoesNotOverwriteRTSignals() {
    PartitionConsumptionState localPcs = freshPcs();
    for (int i = 0; i < 10; i++) {
      localPcs.incrementUniqueKeyCountForBatchRecord(UniqueKeyCountTestUtils.sortedKeyBytes(i));
    }
    localPcs.finalizeUniqueKeyCountForBatchPush();
    localPcs.incrementUniqueKeyCount(); // RT adjustment -> 11
    assertEquals(localPcs.getUniqueKeyCount(), 11L);
    // Second finalize (e.g., from duplicate EOP) does NOT overwrite RT signals
    localPcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(localPcs.getUniqueKeyCount(), 11L);
  }

  @Test
  public void testConcurrency() throws InterruptedException {
    PartitionConsumptionState localPcs = freshPcs();
    localPcs.setUniqueKeyCount(0);
    int n = 10;
    int ops = 1000;
    Thread[] threads = new Thread[n * 2];
    for (int i = 0; i < n; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < ops; j++) {
          localPcs.incrementUniqueKeyCount();
        }
      });
      threads[i + n] = new Thread(() -> {
        for (int j = 0; j < ops; j++) {
          localPcs.decrementUniqueKeyCount();
        }
      });
    }
    for (Thread t: threads) {
      t.start();
    }
    for (Thread t: threads) {
      t.join();
    }
    assertEquals(localPcs.getUniqueKeyCount(), 0L);
  }

  // OffsetRecord persistence and schema evolution

  @Test(dataProvider = "serializationValues")
  public void testOffsetRecordSerializationRoundTrip(long value) {
    OffsetRecord orig = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    orig.setUniqueKeyCount(value);
    byte[] bytes = orig.toBytes();
    OffsetRecord restored = new OffsetRecord(
        bytes,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(restored.getUniqueKeyCount(), value);
  }

  @Test
  public void testSchemaEvolutionDoesNotCorruptOtherFields() {
    OffsetRecord record = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    record.setUniqueKeyCount(42L);
    record.setOffsetLag(77L);
    byte[] serialized = record.toBytes();
    OffsetRecord restored = new OffsetRecord(
        serialized,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(restored.getUniqueKeyCount(), 42L);
    assertEquals(restored.getOffsetLag(), 77L);
  }

  @Test
  public void testPcsRestoredFromCheckpoint() {
    for (long value: new long[] { 12345L, -1L, 0L }) {
      PartitionConsumptionState localPcs = pcsFromCheckpoint(value);
      assertEquals(localPcs.getUniqueKeyCount(), value);
    }
  }

  // "kcs" header encoding/decoding

  @Test
  public void testHeaderRoundTrip() {
    for (int delta: new int[] { 1, -1 }) {
      PubSubMessageHeaders headers = new PubSubMessageHeaders();
      headers.add(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { (byte) delta });
      PubSubMessageHeader h = headers.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER);
      Assert.assertNotNull(h);
      assertEquals((int) h.value()[0], delta);
    }
  }

  // putInStorageEngine

  @Test(dataProvider = "batchPutSchemaIds")
  public void testBatchPutReturnsValueOperationType(int schemaId, String desc) {
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();
    Put put = createBatchPut(schemaId);
    ActiveActiveStoreIngestionTask.StorageOperationType opType =
        ingestionTask.getStorageOperationTypeForPut(PARTITION, put);
    assertEquals(opType, ActiveActiveStoreIngestionTask.StorageOperationType.VALUE, desc);
  }

  @Test(dataProvider = "putRmdWithSchemaIds")
  public void testPutInStorageEngine_batchRmdEnabled_writesRmd(int schemaId, String desc) throws Exception {
    setupForStorageEngineTests(true);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, createBatchPut(schemaId));
    verify(storageEngine)
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
    verify(storageEngine, never()).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_chunkFragment_fallsThrough() throws Exception {
    setupForStorageEngineTests(true);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, createBatchPut(CHUNK_SCHEMA_ID));
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  @Test(dataProvider = "putFallThroughCases")
  public void testPutInStorageEngine_fallsThrough(
      boolean addRmdEnabled,
      boolean isDaVinci,
      boolean postEop,
      String desc) throws Exception {
    setupForStorageEngineTests(addRmdEnabled);
    doReturn(isDaVinci).when(ingestionTask).isDaVinciClient();
    doReturn(postEop).when(pcs).isEndOfPushReceived();
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, createBatchPut(USER_SCHEMA_ID));
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_pcsNull_fallsThrough() throws Exception {
    setupPcsNullFallThrough();
    doCallRealMethod().when(ingestionTask).putInStorageEngine(anyInt(), any(), any(Put.class));
    doReturn(ActiveActiveStoreIngestionTask.StorageOperationType.VALUE).when(ingestionTask)
        .getStorageOperationTypeForPut(anyInt(), any());
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, createBatchPut(USER_SCHEMA_ID));
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  // removeFromStorageEngine

  @Test
  public void testRemoveFromStorageEngine_batchRmdEnabled_preEop() throws Exception {
    setupForStorageEngineTests(true);
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, createBatchDelete());
    verify(storageEngine).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
    verify(storageEngine, never()).delete(anyInt(), any(byte[].class));
  }

  @Test(dataProvider = "deleteFallThroughCases")
  public void testRemoveFromStorageEngine_fallsThrough(
      boolean addRmdEnabled,
      boolean isDaVinci,
      boolean deferredWrite,
      String desc) throws Exception {
    setupForStorageEngineTests(addRmdEnabled);
    doReturn(isDaVinci).when(ingestionTask).isDaVinciClient();
    doReturn(deferredWrite).when(pcs).isDeferredWrite();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, createBatchDelete());
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_postEop_goesToValueAndRmd() throws Exception {
    setupForStorageEngineTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, createBatchDelete());
    verify(storageEngine).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
    verify(storageEngine, never()).delete(anyInt(), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_batchRmdEnabled_pcsNull_fallsThrough() throws Exception {
    setupPcsNullFallThrough();
    doCallRealMethod().when(ingestionTask).removeFromStorageEngine(anyInt(), any(), any(Delete.class));
    doReturn(ActiveActiveStoreIngestionTask.StorageOperationType.VALUE).when(ingestionTask)
        .getStorageOperationTypeForDelete(anyInt(), any());
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, createBatchDelete());
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_batchRmdEnabled_postEop_valueCase_fallsThrough() throws Exception {
    setupPcsNullFallThrough();
    doCallRealMethod().when(ingestionTask).removeFromStorageEngine(anyInt(), any(), any(Delete.class));
    doReturn(ActiveActiveStoreIngestionTask.StorageOperationType.VALUE).when(ingestionTask)
        .getStorageOperationTypeForDelete(anyInt(), any());
    doReturn(pcsMap).when(ingestionTask).getPartitionConsumptionStateMap();
    doReturn(true).when(pcs).isEndOfPushReceived();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, createBatchDelete());
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  // trackUniqueKeyCount

  @Test
  public void testTrackUniqueKeyCount_batchCounting_schemaFiltering() throws Exception {
    setupForTrackUniqueKeyCount(true, false, false);
    // Non-chunked PUT and manifest: counted
    for (int schemaId: new int[] { USER_SCHEMA_ID, CHUNK_MANIFEST_SCHEMA_ID }) {
      PartitionConsumptionState mockPcs = createMockPcsForTrack(false, -1L);
      invokeTrackUniqueKeyCount(
          createMockConsumerRecord(new PubSubMessageHeaders()),
          mockPcs,
          null,
          MessageType.PUT,
          schemaId);
      verify(mockPcs).incrementUniqueKeyCountForBatchRecord(any(byte[].class));
    }
    // Chunk fragment: skipped
    PartitionConsumptionState chunkPcs = createMockPcsForTrack(false, -1L);
    invokeTrackUniqueKeyCount(
        createMockConsumerRecord(new PubSubMessageHeaders()),
        chunkPcs,
        null,
        MessageType.PUT,
        CHUNK_SCHEMA_ID);
    verify(chunkPcs, never()).incrementUniqueKeyCountForBatchRecord(any(byte[].class));
  }

  @Test(dataProvider = "batchCountingSkippedCases")
  public void testTrackUniqueKeyCount_batchCounting_skipped(
      boolean batchEnabled,
      boolean postEop,
      MessageType messageType,
      int schemaId,
      String desc) throws Exception {
    setupForTrackUniqueKeyCount(batchEnabled, false, false);
    PartitionConsumptionState mockPcs = createMockPcsForTrack(postEop, -1L);
    invokeTrackUniqueKeyCount(
        createMockConsumerRecord(new PubSubMessageHeaders()),
        mockPcs,
        null,
        messageType,
        schemaId);
    verify(mockPcs, never()).incrementUniqueKeyCountForBatchRecord(any(byte[].class));
  }

  @Test
  public void testTrackUniqueKeyCount_batchAndFollower_bothEnabled() throws Exception {
    setupForTrackUniqueKeyCount(true, true, true);
    PartitionConsumptionState mockPcs = createMockPcsForTrack(false, -1L);
    invokeTrackUniqueKeyCount(
        createMockConsumerRecord(new PubSubMessageHeaders()),
        mockPcs,
        null,
        MessageType.PUT,
        USER_SCHEMA_ID);
    verify(mockPcs).incrementUniqueKeyCountForBatchRecord(any(byte[].class));
    verify(mockPcs, never()).incrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_createdAndDeleted() throws Exception {
    setupForTrackUniqueKeyCount(false, true, true);
    // Created signal
    PartitionConsumptionState mockPcs1 = createMockPcsForTrack(true, 5L);
    invokeTrackUniqueKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE)),
        mockPcs1,
        null,
        MessageType.PUT,
        USER_SCHEMA_ID);
    verify(mockPcs1).incrementUniqueKeyCount();
    verify(mockPcs1, never()).decrementUniqueKeyCount();
    // Deleted signal
    PartitionConsumptionState mockPcs2 = createMockPcsForTrack(true, 5L);
    invokeTrackUniqueKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_DELETED_SIGNAL_VALUE)),
        mockPcs2,
        null,
        MessageType.DELETE,
        USER_SCHEMA_ID);
    verify(mockPcs2).decrementUniqueKeyCount();
    verify(mockPcs2, never()).incrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_unexpectedValue() throws Exception {
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = createMockPcsForTrack(true, 5L);
    doReturn("store_v1-0").when(mockPcs).getReplicaId();
    invokeTrackUniqueKeyCount(
        createMockConsumerRecord(createSignalHeaders((byte) 99)),
        mockPcs,
        null,
        MessageType.PUT,
        USER_SCHEMA_ID);
    verifyNoCountChange(mockPcs);
  }

  @Test(dataProvider = "followerSignalSkippedCases")
  public void testTrackUniqueKeyCount_followerSignal_skipped(
      boolean hybridEnabled,
      boolean isAA,
      boolean postEop,
      long uniqueKeyCount,
      boolean hasLeaderCtx,
      MessageType messageType,
      int schemaId,
      String desc) throws Exception {
    setupForTrackUniqueKeyCount(false, hybridEnabled, isAA);
    PartitionConsumptionState mockPcs = createMockPcsForTrack(postEop, uniqueKeyCount);
    LeaderProducedRecordContext leaderCtx = hasLeaderCtx ? mock(LeaderProducedRecordContext.class) : null;
    invokeTrackUniqueKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE)),
        mockPcs,
        leaderCtx,
        messageType,
        schemaId);
    verifyNoCountChange(mockPcs);
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_chunkFiltering() throws Exception {
    setupForTrackUniqueKeyCount(false, true, true);
    // Manifest: signal applied
    PartitionConsumptionState manifestPcs = createMockPcsForTrack(true, 5L);
    invokeTrackUniqueKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE)),
        manifestPcs,
        null,
        MessageType.PUT,
        CHUNK_MANIFEST_SCHEMA_ID);
    verify(manifestPcs).incrementUniqueKeyCount();
    // Chunk fragment: skipped
    PartitionConsumptionState chunkPcs = createMockPcsForTrack(true, 5L);
    invokeTrackUniqueKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE)),
        chunkPcs,
        null,
        MessageType.PUT,
        CHUNK_SCHEMA_ID);
    verifyNoCountChange(chunkPcs);
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_headerAbsentOrInvalid() throws Exception {
    setupForTrackUniqueKeyCount(false, true, true);
    // Test missing header, null value header, and empty byte array header
    PubSubMessageHeaders[] headerCases =
        new PubSubMessageHeaders[] { new PubSubMessageHeaders(), new PubSubMessageHeaders() {
          {
            add(new PubSubMessageHeader(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, null));
          }
        }, new PubSubMessageHeaders() {
          {
            add(new PubSubMessageHeader(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[0]));
          }
        } };
    for (PubSubMessageHeaders headers: headerCases) {
      PartitionConsumptionState mockPcs = createMockPcsForTrack(true, 5L);
      invokeTrackUniqueKeyCount(createMockConsumerRecord(headers), mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);
      verifyNoCountChange(mockPcs);
    }
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_unexpectedValue_redundantFilterTrue() throws Exception {
    PartitionConsumptionState mockPcs = createMockPcsForTrack(true, 5L);
    doReturn("test-replica").when(mockPcs).getReplicaId();
    // Call twice -- the second call hits the filter=true branch
    for (int i = 0; i < 2; i++) {
      setupForTrackUniqueKeyCount(true, true, true);
      invokeTrackUniqueKeyCount(
          createMockConsumerRecord(createSignalHeaders((byte) 99)),
          mockPcs,
          null,
          MessageType.PUT,
          USER_SCHEMA_ID);
    }
    verifyNoCountChange(mockPcs);
  }

  // processMessageAndMaybeProduceToKafka

  @Test
  public void testProcessMessage_createdAndDeletedSignals() throws Exception {
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();
    // New key created: increments
    ingestionTask.processMessageAndMaybeProduceToKafka(
        createWrapperWithResult(createMockMergeConflictResultWrapper(false, false, true)),
        pcs,
        PARTITION,
        "url",
        0,
        0L,
        0L);
    verify(pcs).incrementUniqueKeyCount();
    verify(pcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_keyDeleted_decrementsCount() throws Exception {
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();
    ingestionTask.processMessageAndMaybeProduceToKafka(
        createWrapperWithResult(createMockMergeConflictResultWrapper(false, true, false)),
        pcs,
        PARTITION,
        "url",
        0,
        0L,
        0L);
    verify(pcs).decrementUniqueKeyCount();
    verify(pcs, never()).incrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_noCountChange() throws Exception {
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();
    // Update ignored: no change
    ingestionTask.processMessageAndMaybeProduceToKafka(
        createWrapperWithResult(createMockMergeConflictResultWrapper(true, false, false)),
        pcs,
        PARTITION,
        "url",
        0,
        0L,
        0L);
    // Existing key updated (alive -> alive): no change
    ingestionTask.processMessageAndMaybeProduceToKafka(
        createWrapperWithResult(createMockMergeConflictResultWrapper(false, true, true)),
        pcs,
        PARTITION,
        "url",
        0,
        0L,
        0L);
    verifyNoCountChange(pcs);
  }

  @Test
  public void testProcessMessage_signalSkipped_featureDisabled() throws Exception {
    setupForProcessMessageTests(false);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();
    ingestionTask.processMessageAndMaybeProduceToKafka(
        createWrapperWithResult(createMockMergeConflictResultWrapper(false, false, true)),
        pcs,
        PARTITION,
        "url",
        0,
        0L,
        0L);
    verifyNoCountChange(pcs);
  }

  // processEndOfPush

  @Test
  public void testProcessEndOfPush() throws Exception {
    for (boolean enabled: new boolean[] { true, false }) {
      StoreIngestionTask sitMock = setupProcessEndOfPush(enabled);
      PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
      OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
      doReturn(false).when(mockOffsetRecord).isEndOfPushReceived();
      doReturn(mockOffsetRecord).when(mockPcs).getOffsetRecord();
      doReturn("test-replica").when(mockPcs).getReplicaId();
      doReturn(10L).when(mockPcs).getUniqueKeyCount();
      KafkaMessageEnvelope kme = new KafkaMessageEnvelope();
      kme.producerMetadata = new ProducerMetadata();
      kme.producerMetadata.messageTimestamp = System.currentTimeMillis();
      sitMock.processEndOfPush(kme, mock(PubSubPosition.class), mockPcs, new EndOfPush());
      if (enabled) {
        verify(mockPcs).finalizeUniqueKeyCountForBatchPush();
      } else {
        verify(mockPcs, never()).finalizeUniqueKeyCountForBatchPush();
      }
    }
  }
}
