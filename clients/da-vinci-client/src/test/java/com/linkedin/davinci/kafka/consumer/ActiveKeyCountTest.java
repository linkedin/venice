package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.findMethod;
import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.freshPcs;
import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.pcsFromCheckpoint;
import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.setField;
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.venice.exceptions.VeniceException;
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
import com.linkedin.venice.schema.rmd.RmdConstants;
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
 * Unit tests for active key count: PCS field operations, OffsetRecord persistence, schema evolution,
 * and production code path verification via doCallRealMethod/reflection.
 */
public class ActiveKeyCountTest {
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
  public void setUp() throws Exception {
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
    setField(ingestionTask, "hostLevelIngestionStats", mock(HostLevelIngestionStats.class));
    doReturn(mock(HostLevelIngestionStats.class)).when(ingestionTask).getHostLevelIngestionStats();
    AggVersionedIngestionStats mockAggStats = mock(AggVersionedIngestionStats.class);
    setField(ingestionTask, "versionedIngestionStats", mockAggStats);
    setField(ingestionTask, "aggVersionedIngestionStats", mockAggStats);
    setField(ingestionTask, "storeName", "test-store");
    setField(ingestionTask, "versionNumber", 1);
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

  private void setupForTrackActiveKeyCount(
      boolean batchCountingEnabled,
      boolean hybridCountingEnabled,
      boolean isActiveActive) throws Exception {
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);
    doReturn(batchCountingEnabled).when(mockServerConfig).isActiveKeyCountForAllBatchPushEnabled();
    doReturn(hybridCountingEnabled).when(mockServerConfig).isActiveKeyCountForHybridStoreEnabled();
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

  private void invokeTrackActiveKeyCount(
      DefaultPubSubMessage consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      MessageType messageType,
      int writerSchemaId) throws Exception {
    Method method = findMethod(
        StoreIngestionTask.class,
        "trackActiveKeyCount",
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

  private void setupForProcessMessageTests(boolean activeKeyCountEnabled) throws Exception {
    doCallRealMethod().when(ingestionTask)
        .processMessageAndMaybeProduceToKafka(any(), any(), anyInt(), anyString(), anyInt(), anyLong(), anyLong());
    setField(ingestionTask, "activeKeyCountForHybridStoreEnabled", activeKeyCountEnabled);
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
    doReturn(5L).when(wrapper).getActiveKeyCountBeforeAliveCheck(); // valid count before alive check
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

  private PartitionConsumptionState createMockPcsForTrack(boolean postEop, long activeKeyCount) {
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(postEop).when(mockPcs).isEndOfPushReceived();
    doReturn(activeKeyCount).when(mockPcs).getActiveKeyCount();
    return mockPcs;
  }

  private PubSubMessageHeaders createSignalHeaders(byte signalValue) {
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(new PubSubMessageHeader(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { signalValue }));
    return headers;
  }

  private StoreIngestionTask setupProcessEndOfPush(boolean activeKeyCountEnabled) throws Exception {
    StoreIngestionTask sitMock = mock(StoreIngestionTask.class);
    doCallRealMethod().when(sitMock).processEndOfPush(any(), any(), any(), any());
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);
    doReturn(activeKeyCountEnabled).when(mockServerConfig).isActiveKeyCountForAllBatchPushEnabled();
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
    verify(target, never()).incrementActiveKeyCount();
    verify(target, never()).decrementActiveKeyCount();
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
    return new Object[][] { { false, false, "RMD disabled" }, { true, true, "post-EOP" } };
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
    assertEquals(localPcs.getActiveKeyCount(), -1L);
    for (int i = 0; i < 5; i++) {
      localPcs.incrementActiveKeyCountForBatchRecord(ActiveKeyCountTestUtils.sortedKeyBytes(i));
    }
    assertEquals(localPcs.getActiveKeyCount(), 5L);
    localPcs.finalizeActiveKeyCountForBatchPush();
    assertEquals(localPcs.getActiveKeyCount(), 5L);
    // Empty batch finalize yields 0
    PartitionConsumptionState emptyPcs = freshPcs();
    emptyPcs.finalizeActiveKeyCountForBatchPush();
    assertEquals(emptyPcs.getActiveKeyCount(), 0L);
    // Verify increment/decrement works post-finalize (RT signals)
    localPcs.incrementActiveKeyCount();
    assertEquals(localPcs.getActiveKeyCount(), 6L);
    localPcs.decrementActiveKeyCount();
    assertEquals(localPcs.getActiveKeyCount(), 5L);
  }

  @Test
  public void testBatchDedupSkipsDuplicateKeys() {
    PartitionConsumptionState localPcs = freshPcs();
    for (int i = 0; i < 5; i++) {
      localPcs.incrementActiveKeyCountForBatchRecord(ActiveKeyCountTestUtils.sortedKeyBytes(i));
    }
    assertEquals(localPcs.getActiveKeyCount(), 5L);
    // Replay same keys (speculative execution) -- all should be skipped
    for (int i = 0; i < 5; i++) {
      localPcs.incrementActiveKeyCountForBatchRecord(ActiveKeyCountTestUtils.sortedKeyBytes(i));
    }
    assertEquals(localPcs.getActiveKeyCount(), 5L);
    // Same key inserted twice consecutively
    byte[] sameKey = ActiveKeyCountTestUtils.sortedKeyBytes(42);
    localPcs.incrementActiveKeyCountForBatchRecord(sameKey);
    assertEquals(localPcs.getActiveKeyCount(), 6L);
    localPcs.incrementActiveKeyCountForBatchRecord(sameKey);
    assertEquals(localPcs.getActiveKeyCount(), 6L);
  }

  @Test
  public void testFinalizeDoesNotOverwriteRTSignals() {
    PartitionConsumptionState localPcs = freshPcs();
    for (int i = 0; i < 10; i++) {
      localPcs.incrementActiveKeyCountForBatchRecord(ActiveKeyCountTestUtils.sortedKeyBytes(i));
    }
    localPcs.finalizeActiveKeyCountForBatchPush();
    localPcs.incrementActiveKeyCount(); // RT adjustment -> 11
    assertEquals(localPcs.getActiveKeyCount(), 11L);
    // Second finalize (e.g., from duplicate EOP) does NOT overwrite RT signals
    localPcs.finalizeActiveKeyCountForBatchPush();
    assertEquals(localPcs.getActiveKeyCount(), 11L);
  }

  @Test
  public void testConcurrency() throws InterruptedException {
    PartitionConsumptionState localPcs = freshPcs();
    int n = 10;
    int ops = 1000;
    // Start high enough so decrements never hit the floor clamp during concurrent execution.
    // Worst case: all n*ops decrements race ahead of any increments → n*ops → 0.
    localPcs.setActiveKeyCount(n * ops);
    Thread[] threads = new Thread[n * 2];
    for (int i = 0; i < n; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < ops; j++) {
          localPcs.incrementActiveKeyCount();
        }
      });
      threads[i + n] = new Thread(() -> {
        for (int j = 0; j < ops; j++) {
          localPcs.decrementActiveKeyCount();
        }
      });
    }
    for (Thread t: threads) {
      t.start();
    }
    for (Thread t: threads) {
      t.join();
    }
    // n*ops (start) + n*ops (increments) - n*ops (decrements) = n*ops
    assertEquals(localPcs.getActiveKeyCount(), (long) (n * ops));
  }

  // OffsetRecord persistence and schema evolution

  @Test(dataProvider = "serializationValues")
  public void testOffsetRecordSerializationRoundTrip(long value) {
    OffsetRecord orig = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    orig.setActiveKeyCount(value);
    byte[] bytes = orig.toBytes();
    OffsetRecord restored = new OffsetRecord(
        bytes,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(restored.getActiveKeyCount(), value);
  }

  @Test
  public void testSchemaEvolutionDoesNotCorruptOtherFields() {
    OffsetRecord record = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    record.setActiveKeyCount(42L);
    record.setOffsetLag(77L);
    byte[] serialized = record.toBytes();
    OffsetRecord restored = new OffsetRecord(
        serialized,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(restored.getActiveKeyCount(), 42L);
    assertEquals(restored.getOffsetLag(), 77L);
  }

  @Test
  public void testPcsRestoredFromCheckpoint() {
    for (long value: new long[] { 12345L, -1L, 0L }) {
      PartitionConsumptionState localPcs = pcsFromCheckpoint(value);
      assertEquals(localPcs.getActiveKeyCount(), value);
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
  public void testPutInStorageEngine_fallsThrough(boolean addRmdEnabled, boolean postEop, String desc)
      throws Exception {
    setupForStorageEngineTests(addRmdEnabled);
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

  // removeFromStorageEngine — batch DELETEs always use plain delete (no RMD),
  // regardless of addRmdToBatchPushForHybridStores, so the ts=0 sentinel only applies to PUTs.

  @Test
  public void testRemoveFromStorageEngine_batchDeleteAlwaysUsesPlainDelete() throws Exception {
    setupForStorageEngineTests(true); // addRmdEnabled=true, but DELETE should still use plain delete
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, createBatchDelete());
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_postEop_goesToValueAndRmd() throws Exception {
    setupForStorageEngineTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, createBatchDelete());
    // Post-EOP DELETEs go to VALUE_AND_RMD case (normal A/A path), not the batch VALUE case
    verify(storageEngine).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
    verify(storageEngine, never()).delete(anyInt(), any(byte[].class));
  }

  // trackActiveKeyCount

  @Test
  public void testTrackActiveKeyCount_batchCounting_schemaFiltering() throws Exception {
    setupForTrackActiveKeyCount(true, false, false);
    // Non-chunked PUT and manifest: counted
    for (int schemaId: new int[] { USER_SCHEMA_ID, CHUNK_MANIFEST_SCHEMA_ID }) {
      PartitionConsumptionState mockPcs = createMockPcsForTrack(false, -1L);
      invokeTrackActiveKeyCount(
          createMockConsumerRecord(new PubSubMessageHeaders()),
          mockPcs,
          null,
          MessageType.PUT,
          schemaId);
      verify(mockPcs).incrementActiveKeyCountForBatchRecord(any(byte[].class));
    }
    // Chunk fragment: skipped
    PartitionConsumptionState chunkPcs = createMockPcsForTrack(false, -1L);
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(new PubSubMessageHeaders()),
        chunkPcs,
        null,
        MessageType.PUT,
        CHUNK_SCHEMA_ID);
    verify(chunkPcs, never()).incrementActiveKeyCountForBatchRecord(any(byte[].class));
  }

  @Test(dataProvider = "batchCountingSkippedCases")
  public void testTrackActiveKeyCount_batchCounting_skipped(
      boolean batchEnabled,
      boolean postEop,
      MessageType messageType,
      int schemaId,
      String desc) throws Exception {
    setupForTrackActiveKeyCount(batchEnabled, false, false);
    PartitionConsumptionState mockPcs = createMockPcsForTrack(postEop, -1L);
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(new PubSubMessageHeaders()),
        mockPcs,
        null,
        messageType,
        schemaId);
    verify(mockPcs, never()).incrementActiveKeyCountForBatchRecord(any(byte[].class));
  }

  @Test
  public void testTrackActiveKeyCount_batchAndFollower_bothEnabled() throws Exception {
    setupForTrackActiveKeyCount(true, true, true);
    PartitionConsumptionState mockPcs = createMockPcsForTrack(false, -1L);
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(new PubSubMessageHeaders()),
        mockPcs,
        null,
        MessageType.PUT,
        USER_SCHEMA_ID);
    verify(mockPcs).incrementActiveKeyCountForBatchRecord(any(byte[].class));
    verify(mockPcs, never()).incrementActiveKeyCount();
  }

  @Test
  public void testTrackActiveKeyCount_followerSignal_createdAndDeleted() throws Exception {
    setupForTrackActiveKeyCount(false, true, true);
    // Created signal
    PartitionConsumptionState mockPcs1 = createMockPcsForTrack(true, 5L);
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE)),
        mockPcs1,
        null,
        MessageType.PUT,
        USER_SCHEMA_ID);
    verify(mockPcs1).incrementActiveKeyCount();
    verify(mockPcs1, never()).decrementActiveKeyCount();
    // Deleted signal
    PartitionConsumptionState mockPcs2 = createMockPcsForTrack(true, 5L);
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_DELETED_SIGNAL_VALUE)),
        mockPcs2,
        null,
        MessageType.DELETE,
        USER_SCHEMA_ID);
    verify(mockPcs2).decrementActiveKeyCount();
    verify(mockPcs2, never()).incrementActiveKeyCount();
  }

  @Test
  public void testTrackActiveKeyCount_followerSignal_unexpectedValue_invalidatesCount() throws Exception {
    PartitionConsumptionState mockPcs = createMockPcsForTrack(true, 5L);
    doReturn("test-replica").when(mockPcs).getReplicaId();

    // Unexpected single-byte signal value: count is invalidated to -1
    setupForTrackActiveKeyCount(false, true, true);
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(createSignalHeaders((byte) 99)),
        mockPcs,
        null,
        MessageType.PUT,
        USER_SCHEMA_ID);
    verifyNoCountChange(mockPcs);
    verify(mockPcs).setActiveKeyCount(-1);
  }

  @Test
  public void testTrackActiveKeyCount_followerSignal_multiByteSignal_invalidatesCount() throws Exception {
    PartitionConsumptionState mockPcs = createMockPcsForTrack(true, 5L);
    doReturn("test-replica").when(mockPcs).getReplicaId();

    // Multi-byte signal (length > 1): count is invalidated to -1
    setupForTrackActiveKeyCount(false, true, true);
    PubSubMessageHeaders multiByteHeaders = new PubSubMessageHeaders();
    multiByteHeaders.add(new PubSubMessageHeader(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { 1, 2 }));
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(multiByteHeaders),
        mockPcs,
        null,
        MessageType.PUT,
        USER_SCHEMA_ID);
    verifyNoCountChange(mockPcs);
    verify(mockPcs).setActiveKeyCount(-1);
  }

  @Test
  public void testTrackActiveKeyCount_followerSignal_invalidateSignal() throws Exception {
    PartitionConsumptionState mockPcs = createMockPcsForTrack(true, 5L);

    // Invalidate signal (value=0) from leader: follower sets count to -1
    setupForTrackActiveKeyCount(false, true, true);
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_COUNT_INVALIDATE_SIGNAL_VALUE)),
        mockPcs,
        null,
        MessageType.PUT,
        USER_SCHEMA_ID);
    verifyNoCountChange(mockPcs);
    verify(mockPcs).setActiveKeyCount(-1);
  }

  @Test(dataProvider = "followerSignalSkippedCases")
  public void testTrackActiveKeyCount_followerSignal_skipped(
      boolean hybridEnabled,
      boolean isAA,
      boolean postEop,
      long activeKeyCount,
      boolean hasLeaderCtx,
      MessageType messageType,
      int schemaId,
      String desc) throws Exception {
    setupForTrackActiveKeyCount(false, hybridEnabled, isAA);
    PartitionConsumptionState mockPcs = createMockPcsForTrack(postEop, activeKeyCount);
    LeaderProducedRecordContext leaderCtx = hasLeaderCtx ? mock(LeaderProducedRecordContext.class) : null;
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE)),
        mockPcs,
        leaderCtx,
        messageType,
        schemaId);
    verifyNoCountChange(mockPcs);
  }

  @Test
  public void testTrackActiveKeyCount_followerSignal_chunkFiltering() throws Exception {
    setupForTrackActiveKeyCount(false, true, true);
    // Manifest: signal applied
    PartitionConsumptionState manifestPcs = createMockPcsForTrack(true, 5L);
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE)),
        manifestPcs,
        null,
        MessageType.PUT,
        CHUNK_MANIFEST_SCHEMA_ID);
    verify(manifestPcs).incrementActiveKeyCount();
    // Chunk fragment: skipped
    PartitionConsumptionState chunkPcs = createMockPcsForTrack(true, 5L);
    invokeTrackActiveKeyCount(
        createMockConsumerRecord(createSignalHeaders(ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE)),
        chunkPcs,
        null,
        MessageType.PUT,
        CHUNK_SCHEMA_ID);
    verifyNoCountChange(chunkPcs);
  }

  @Test
  public void testTrackActiveKeyCount_followerSignal_headerAbsentOrInvalid() throws Exception {
    setupForTrackActiveKeyCount(false, true, true);
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
      invokeTrackActiveKeyCount(createMockConsumerRecord(headers), mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);
      verifyNoCountChange(mockPcs);
    }
  }

  // processMessageAndMaybeProduceToKafka

  @Test
  public void testProcessMessage_signalIncrementAndDecrement() throws Exception {
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getActiveKeyCount();

    // New key created: increments
    ingestionTask.processMessageAndMaybeProduceToKafka(
        createWrapperWithResult(createMockMergeConflictResultWrapper(false, false, true)),
        pcs,
        PARTITION,
        "url",
        0,
        0L,
        0L);
    verify(pcs).incrementActiveKeyCount();
    verify(pcs, never()).decrementActiveKeyCount();

    // Key deleted: decrements (fresh mock to reset verify state)
    PartitionConsumptionState pcs2 = mock(PartitionConsumptionState.class);
    doReturn(true).when(pcs2).isEndOfPushReceived();
    doReturn(5L).when(pcs2).getActiveKeyCount();
    pcsMap.put(PARTITION, pcs2);

    ingestionTask.processMessageAndMaybeProduceToKafka(
        createWrapperWithResult(createMockMergeConflictResultWrapper(false, true, false)),
        pcs2,
        PARTITION,
        "url",
        0,
        0L,
        0L);
    verify(pcs2).decrementActiveKeyCount();
    verify(pcs2, never()).incrementActiveKeyCount();
  }

  @Test
  public void testProcessMessage_noCountChange() throws Exception {
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getActiveKeyCount();
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
    doReturn(5L).when(pcs).getActiveKeyCount();
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
      doReturn(10L).when(mockPcs).getActiveKeyCount();
      KafkaMessageEnvelope kme = new KafkaMessageEnvelope();
      kme.producerMetadata = new ProducerMetadata();
      kme.producerMetadata.messageTimestamp = System.currentTimeMillis();
      sitMock.processEndOfPush(kme, mock(PubSubPosition.class), mockPcs, new EndOfPush());
      if (enabled) {
        verify(mockPcs).finalizeActiveKeyCountForBatchPush();
      } else {
        verify(mockPcs, never()).finalizeActiveKeyCountForBatchPush();
      }
    }
  }

  // wasOldValueAlive — 4-branch RMD decision logic + isValuePresentForKey — 3-tier lookup

  private boolean invokeWasOldValueAlive(
      RmdWithValueSchemaId rmd,
      Lazy<ByteBuffer> provider,
      PartitionConsumptionState pcs,
      byte[] key) throws Exception {
    Method m = ActiveActiveStoreIngestionTask.class.getDeclaredMethod(
        "wasOldValueAlive",
        RmdWithValueSchemaId.class,
        Lazy.class,
        PartitionConsumptionState.class,
        byte[].class);
    m.setAccessible(true);
    return (boolean) m.invoke(ingestionTask, rmd, provider, pcs, key);
  }

  private boolean invokeIsValuePresentForKey(Lazy<ByteBuffer> provider, PartitionConsumptionState pcs, byte[] key)
      throws Exception {
    Method m = ActiveActiveStoreIngestionTask.class
        .getDeclaredMethod("isValuePresentForKey", Lazy.class, PartitionConsumptionState.class, byte[].class);
    m.setAccessible(true);
    return (boolean) m.invoke(ingestionTask, provider, pcs, key);
  }

  private RmdWithValueSchemaId rmdWithTimestamp(long ts) {
    RmdWithValueSchemaId rmd = mock(RmdWithValueSchemaId.class);
    GenericRecord rec = mock(GenericRecord.class);
    doReturn(rec).when(rmd).getRmdRecord();
    doReturn(ts).when(rec).get(RmdConstants.TIMESTAMP_FIELD_POS);
    return rmd;
  }

  private void setupForValueLookup(boolean keyExists) throws Exception {
    setField(ingestionTask, "storageEngine", storageEngine);
    doReturn(0).when(pcs).getPartition();
    doReturn(null).when(pcs).getTransientRecord(any());
    doReturn(keyExists).when(storageEngine).keyExists(anyInt(), any(byte[].class));
  }

  @Test
  public void testWasOldValueAlive_allBranches() throws Exception {
    // Feature disabled → always false
    setField(ingestionTask, "activeKeyCountForHybridStoreEnabled", false);
    Assert.assertFalse(invokeWasOldValueAlive(rmdWithTimestamp(100), Lazy.of(() -> VALUE_PAYLOAD), pcs, KEY_BYTES));

    setField(ingestionTask, "activeKeyCountForHybridStoreEnabled", true);

    // Branch 1: rmd==null + 2a ON → dead (new key, all batch PUTs have RMD)
    setField(ingestionTask, "addRmdToBatchPushForHybridStores", true);
    Assert.assertFalse(invokeWasOldValueAlive(null, Lazy.of(() -> VALUE_PAYLOAD), pcs, KEY_BYTES));

    // Branch 2: rmd==null + 2a OFF → delegates to isValuePresentForKey
    setField(ingestionTask, "addRmdToBatchPushForHybridStores", false);
    setupForValueLookup(true);
    Assert.assertTrue(invokeWasOldValueAlive(null, Lazy.of(() -> null), pcs, KEY_BYTES));
    setupForValueLookup(false);
    Assert.assertFalse(invokeWasOldValueAlive(null, Lazy.of(() -> null), pcs, KEY_BYTES));

    // Branch 3: ts=0 (batch sentinel) → alive
    Assert.assertTrue(
        invokeWasOldValueAlive(
            rmdWithTimestamp(RmdConstants.BATCH_RMD_SENTINEL_TIMESTAMP),
            Lazy.of(() -> null),
            pcs,
            KEY_BYTES));

    // Branch 4: ts>0 → delegates to isValuePresentForKey
    setupForValueLookup(true);
    Assert.assertTrue(invokeWasOldValueAlive(rmdWithTimestamp(1000L), Lazy.of(() -> null), pcs, KEY_BYTES));
    setupForValueLookup(false);
    Assert.assertFalse(invokeWasOldValueAlive(rmdWithTimestamp(1000L), Lazy.of(() -> null), pcs, KEY_BYTES));

    // Branch 4 variant: field-level ts (not Long) → falls through to isValuePresentForKey
    RmdWithValueSchemaId fieldLevelRmd = mock(RmdWithValueSchemaId.class);
    GenericRecord rec = mock(GenericRecord.class);
    doReturn(rec).when(fieldLevelRmd).getRmdRecord();
    doReturn(new java.util.ArrayList<>()).when(rec).get(RmdConstants.TIMESTAMP_FIELD_POS);
    setupForValueLookup(true);
    Assert.assertTrue(invokeWasOldValueAlive(fieldLevelRmd, Lazy.of(() -> null), pcs, KEY_BYTES));
  }

  @Test
  public void testIsValuePresentForKey_allTiers() throws Exception {
    setField(ingestionTask, "storageEngine", storageEngine);

    // Tier 1: Lazy already resolved by DCR → returns cached result
    Lazy<ByteBuffer> resolved = Lazy.of(() -> VALUE_PAYLOAD);
    resolved.get();
    Assert.assertTrue(invokeIsValuePresentForKey(resolved, pcs, KEY_BYTES));
    Lazy<ByteBuffer> resolvedNull = Lazy.of(() -> null);
    resolvedNull.get();
    Assert.assertFalse(invokeIsValuePresentForKey(resolvedNull, pcs, KEY_BYTES));

    // Tier 2: Lazy unresolved, transient cache hit
    PartitionConsumptionState.TransientRecord tr = mock(PartitionConsumptionState.TransientRecord.class);
    doReturn(tr).when(pcs).getTransientRecord(KEY_BYTES);
    doReturn(new byte[] { 1 }).when(tr).getValue();
    Assert.assertTrue(invokeIsValuePresentForKey(Lazy.of(() -> null), pcs, KEY_BYTES));
    doReturn(null).when(tr).getValue();
    Assert.assertFalse(invokeIsValuePresentForKey(Lazy.of(() -> null), pcs, KEY_BYTES));

    // Tier 3: Lazy unresolved, no transient record → storageEngine.keyExists
    doReturn(null).when(pcs).getTransientRecord(KEY_BYTES);
    doReturn(0).when(pcs).getPartition();
    doReturn(true).when(storageEngine).keyExists(0, KEY_BYTES);
    Assert.assertTrue(invokeIsValuePresentForKey(Lazy.of(() -> null), pcs, KEY_BYTES));
    doReturn(false).when(storageEngine).keyExists(0, KEY_BYTES);
    Assert.assertFalse(invokeIsValuePresentForKey(Lazy.of(() -> null), pcs, KEY_BYTES));
  }

  @Test
  public void testIsValuePresentForKey_tier3_chunkedStore_usesChunkingSuffix() throws Exception {
    setField(ingestionTask, "storageEngine", storageEngine);
    // Enable chunking on the ingestion task
    setField(ingestionTask, "isChunked", true);
    doReturn(true).when(ingestionTask).isChunked();
    doReturn(null).when(pcs).getTransientRecord(KEY_BYTES);
    doReturn(0).when(pcs).getPartition();

    // Compute the expected suffixed key (same as what RocksDB actually stores)
    byte[] suffixedKey = com.linkedin.davinci.storage.chunking.ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER
        .serializeNonChunkedKey(KEY_BYTES);

    // storageEngine.keyExists with raw key should NOT be called
    // storageEngine.keyExists with suffixed key should be called
    doReturn(false).when(storageEngine).keyExists(anyInt(), any(byte[].class));
    doReturn(true).when(storageEngine).keyExists(0, suffixedKey);

    // Tier 3 should use the suffixed key and find the value
    Assert.assertTrue(invokeIsValuePresentForKey(Lazy.of(() -> null), pcs, KEY_BYTES));

    // Verify the raw key was NOT used (would return false from the default stub)
    verify(storageEngine, never()).keyExists(0, KEY_BYTES);
    verify(storageEngine).keyExists(0, suffixedKey);
  }

  @Test
  public void testIsValuePresentForKey_tier3_nonChunkedStore_usesRawKey() throws Exception {
    setField(ingestionTask, "storageEngine", storageEngine);
    // Chunking disabled
    setField(ingestionTask, "isChunked", false);
    doReturn(false).when(ingestionTask).isChunked();
    doReturn(null).when(pcs).getTransientRecord(KEY_BYTES);
    doReturn(0).when(pcs).getPartition();

    doReturn(true).when(storageEngine).keyExists(0, KEY_BYTES);
    Assert.assertTrue(invokeIsValuePresentForKey(Lazy.of(() -> null), pcs, KEY_BYTES));

    // Verify raw key was used directly (no chunking suffix)
    verify(storageEngine).keyExists(0, KEY_BYTES);
  }

  @Test
  public void testIsValuePresentForKey_tier3_keyExistsThrows_invalidatesAndReturnsFalse() throws Exception {
    setField(ingestionTask, "storageEngine", storageEngine);
    setField(ingestionTask, "isChunked", false);
    doReturn(false).when(ingestionTask).isChunked();
    doReturn(null).when(pcs).getTransientRecord(KEY_BYTES);
    doReturn(0).when(pcs).getPartition();
    doReturn("test-replica").when(pcs).getReplicaId();

    // Tier 3: keyExists throws VeniceException (simulating transient RocksDB I/O failure)
    doThrow(new VeniceException("disk error")).when(storageEngine).keyExists(anyInt(), any(byte[].class));

    // Should return false (assume key absent) and NOT propagate the exception
    Assert.assertFalse(invokeIsValuePresentForKey(Lazy.of(() -> null), pcs, KEY_BYTES));

    // Count must be invalidated to -1
    verify(pcs).setActiveKeyCount(-1);
  }

  @Test
  public void testDecrementUnderflowInvalidatesAndPreservesInvalidState() {
    PartitionConsumptionState localPcs = freshPcs();

    // Normal decrement: 1 → 0, returns true
    localPcs.setActiveKeyCount(1);
    Assert.assertTrue(localPcs.decrementActiveKeyCount());
    assertEquals(localPcs.getActiveKeyCount(), 0);

    // Underflow at 0 → invalidates to -1 (drift detected), returns false
    Assert.assertFalse(localPcs.decrementActiveKeyCount());
    assertEquals(localPcs.getActiveKeyCount(), -1);

    // Already invalidated: stays -1, returns false
    Assert.assertFalse(localPcs.decrementActiveKeyCount());
    assertEquals(localPcs.getActiveKeyCount(), -1);
  }

  @Test
  public void testDecrementUnderflowAfterEmptyBatch() {
    PartitionConsumptionState localPcs = freshPcs();

    // Simulate empty batch push → finalize sets -1 to 0
    localPcs.finalizeActiveKeyCountForBatchPush();
    assertEquals(localPcs.getActiveKeyCount(), 0);

    // RT DELETE signal on empty batch: underflow invalidates to -1 (drift)
    Assert.assertFalse(localPcs.decrementActiveKeyCount());
    assertEquals(localPcs.getActiveKeyCount(), -1);
  }
}
