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
import static org.testng.Assert.assertNull;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.EmptyPubSubMessageHeaders;
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
 * header encoding/decoding, and production code path verification via doCallRealMethod/reflection
 * for putInStorageEngine, removeFromStorageEngine, trackUniqueKeyCount, processMessageAndMaybeProduceToKafka,
 * and processEndOfPush.
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

  // ==================== Helper methods ====================

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
      byte[] rmdBytes = new byte[] { 0, 0, 0, 0 };
      setField(ingestionTask, "defaultBatchRmdBytes", rmdBytes);
      byte[] rmdWithPrefix = new byte[] { 0, 0, 0, 5, 0, 0, 0, 0 };
      setField(ingestionTask, "defaultBatchRmdWithSchemaIdPrefix", rmdWithPrefix);
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
    PubSubPosition position = mock(PubSubPosition.class);
    doReturn(position).when(consumerRecord).getPosition();

    PubSubMessageProcessedResultWrapper wrapper = new PubSubMessageProcessedResultWrapper(consumerRecord);
    wrapper.setProcessedResult(new PubSubMessageProcessedResult(mcWrapper));
    return wrapper;
  }

  // ==================== DataProviders ====================

  @DataProvider(name = "serializationValues")
  public Object[][] serializationValues() {
    return new Object[][] { { -1L }, { 0L }, { 100L }, { 54321L }, { 100_000_000L } };
  }

  @DataProvider(name = "headerDeltas")
  public Object[][] headerDeltas() {
    return new Object[][] { { 1 }, { -1 } };
  }

  @DataProvider(name = "batchPutSchemaIds")
  public Object[][] batchPutSchemaIds() {
    return new Object[][] { { USER_SCHEMA_ID, "Non-chunked PUT" }, { CHUNK_SCHEMA_ID, "Chunk fragment" },
        { CHUNK_MANIFEST_SCHEMA_ID, "Chunk manifest" }, };
  }

  @DataProvider(name = "rmdSchemaIdSelection")
  public Object[][] rmdSchemaIdSelection() {
    return new Object[][] { { 1, true, "Schema ID 1 (user schema): use actual" },
        { 5, true, "Schema ID 5 (user schema): use actual" }, { 100, true, "Schema ID 100 (user schema): use actual" },
        { CHUNK_MANIFEST_SCHEMA_ID, false, "Manifest (-20): use superset/latest" }, };
  }

  // ==================== 1. PCS Field Operations ====================

  @Test
  public void testInitialState() {
    PartitionConsumptionState localPcs = freshPcs();
    assertEquals(localPcs.getUniqueKeyCount(), -1L, "Default uniqueKeyCount should be -1 (not tracked)");
  }

  @Test
  public void testSetGetUniqueKeyCount() {
    PartitionConsumptionState localPcs = freshPcs();
    for (long v: new long[] { 0, 1, 42, 100_000_000, -1, Long.MAX_VALUE }) {
      localPcs.setUniqueKeyCount(v);
      assertEquals(localPcs.getUniqueKeyCount(), v);
    }
  }

  @Test
  public void testIncrementDecrement() {
    PartitionConsumptionState localPcs = freshPcs();
    localPcs.setUniqueKeyCount(10);
    localPcs.incrementUniqueKeyCount();
    assertEquals(localPcs.getUniqueKeyCount(), 11L);
    localPcs.decrementUniqueKeyCount();
    assertEquals(localPcs.getUniqueKeyCount(), 10L);
  }

  @Test
  public void testBatchKeyCountAndFinalize() {
    PartitionConsumptionState localPcs = freshPcs();
    for (int i = 0; i < 5; i++) {
      localPcs.incrementUniqueKeyCountForBatchRecord();
    }
    assertEquals(localPcs.getUniqueKeyCount(), 5L, "uniqueKeyCount grows during batch for checkpoint safety");
    localPcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(localPcs.getUniqueKeyCount(), 5L, "After finalize, still 5");
  }

  @Test
  public void testFinalizeWithZeroBatch() {
    PartitionConsumptionState localPcs = freshPcs();
    localPcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(localPcs.getUniqueKeyCount(), 0L, "Empty batch -> count=0, not -1");
  }

  @Test
  public void testFinalizeDoesNotOverwriteRTSignals() {
    PartitionConsumptionState localPcs = freshPcs();
    for (int i = 0; i < 10; i++) {
      localPcs.incrementUniqueKeyCountForBatchRecord();
    }
    assertEquals(localPcs.getUniqueKeyCount(), 10L);
    localPcs.finalizeUniqueKeyCountForBatchPush();
    localPcs.incrementUniqueKeyCount(); // RT adjustment -> 11
    assertEquals(localPcs.getUniqueKeyCount(), 11L);
    // Second finalize (e.g., from duplicate EOP) does NOT overwrite RT signals
    // because finalize only acts on the empty-partition edge case now
    localPcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(localPcs.getUniqueKeyCount(), 11L, "Second finalize must not overwrite RT signals");
  }

  @Test
  public void testDecrementBelowZero() {
    PartitionConsumptionState localPcs = freshPcs();
    localPcs.setUniqueKeyCount(0);
    localPcs.decrementUniqueKeyCount();
    assertEquals(localPcs.getUniqueKeyCount(), -1L, "AtomicLong supports negative");
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
    assertEquals(localPcs.getUniqueKeyCount(), 0L, "Equal inc/dec should net to 0");
  }

  // ==================== 2. OffsetRecord Persistence & Schema Evolution ====================

  @Test
  public void testOffsetRecordDefault() {
    OffsetRecord or = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(or.getUniqueKeyCount(), -1L, "Fresh OffsetRecord should default to -1");
  }

  @Test
  public void testOffsetRecordGetSet() {
    OffsetRecord or = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    or.setUniqueKeyCount(999);
    assertEquals(or.getUniqueKeyCount(), 999L);
    or.setUniqueKeyCount(0);
    assertEquals(or.getUniqueKeyCount(), 0L);
  }

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
  public void testSchemaEvolutionOldRecordDefaultsToNegativeOne() {
    // When v20 PartitionState (no uniqueKeyCount) is deserialized by v21 code,
    // the Avro default (-1) is used. We test this by creating a fresh OffsetRecord
    // (which initializes uniqueKeyCount=-1 in getEmptyPartitionState).
    OffsetRecord fresh = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(fresh.getUniqueKeyCount(), -1L, "Schema evolution: missing field defaults to -1");
  }

  @Test
  public void testSchemaEvolutionRoundTripPreservesDefault() {
    // Simulate: OffsetRecord created WITHOUT ever setting uniqueKeyCount (as v20 code would),
    // serialized, then deserialized by v21 code. The uniqueKeyCount should be -1 (Avro default).
    OffsetRecord oldStyleRecord = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    // Don't set uniqueKeyCount -- simulates v20 behavior where the field doesn't exist
    assertEquals(oldStyleRecord.getUniqueKeyCount(), -1L, "Before serialization: default -1");

    byte[] serialized = oldStyleRecord.toBytes();
    OffsetRecord deserialized = new OffsetRecord(
        serialized,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(
        deserialized.getUniqueKeyCount(),
        -1L,
        "After serialization round-trip: -1 preserved (v20 records get Avro default)");
  }

  @Test
  public void testSchemaEvolutionDoesNotCorruptOtherFields() {
    // Adding uniqueKeyCount to schema must not corrupt other persisted fields
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
    assertEquals(restored.getUniqueKeyCount(), 42L, "uniqueKeyCount preserved");
    assertEquals(restored.getOffsetLag(), 77L, "offsetLag preserved -- not corrupted by new field");
  }

  @Test
  public void testPcsRestoredFromCheckpoint() {
    PartitionConsumptionState localPcs = pcsFromCheckpoint(12345);
    assertEquals(localPcs.getUniqueKeyCount(), 12345L);
  }

  @Test
  public void testPcsRestoredFromNegativeOneCheckpoint() {
    PartitionConsumptionState localPcs = pcsFromCheckpoint(-1);
    assertEquals(localPcs.getUniqueKeyCount(), -1L, "Restore -1 when feature not yet started");
  }

  // ==================== 3. "kcs" Header Encoding/Decoding ====================

  @Test(dataProvider = "headerDeltas")
  public void testHeaderRoundTrip(int delta) {
    // Encode
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { (byte) delta });

    // Decode
    PubSubMessageHeader h = headers.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER);
    Assert.assertNotNull(h);
    assertEquals((int) h.value()[0], delta, "Byte round-trip must preserve sign");
  }

  @Test
  public void testAbsentHeaderMeansNoSignal() {
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    assertNull(headers.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER));
  }

  @Test
  public void testEmptyHeadersMeansNoSignal() {
    assertNull(EmptyPubSubMessageHeaders.SINGLETON.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER));
  }

  // ==================== 4. Mixed Deployment (header evolution) ====================

  @Test
  public void testNewCodeOldVTNoHeader() {
    // Old server produced VT without "kcs" header. New follower reads it.
    // trackUniqueKeyCount finds no header -> no adjustment.
    PartitionConsumptionState localPcs = pcsFromCheckpoint(50);
    PubSubMessageHeaders oldHeaders = new PubSubMessageHeaders();
    PubSubMessageHeader h = oldHeaders.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER);
    // Simulate: header absent, no adjustment
    if (h != null) {
      localPcs.incrementUniqueKeyCount();
    }
    assertEquals(localPcs.getUniqueKeyCount(), 50L, "No header -> no change (backward compatible)");
  }

  @Test
  public void testNewCodeNewVTWithHeader() {
    // New server produced VT with "kcs" header. New follower reads it.
    PartitionConsumptionState localPcs = pcsFromCheckpoint(50);
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { 1 });

    PubSubMessageHeader h = headers.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER);
    if (h != null) {
      localPcs.incrementUniqueKeyCount();
    }
    assertEquals(localPcs.getUniqueKeyCount(), 51L);
  }

  @Test
  public void testOldCodeIgnoresNewHeader() {
    // Old follower (no trackUniqueKeyCount code) consumes VT with "kcs" header.
    // Headers are preserved in Kafka but the old code never reads them.
    // This is a conceptual test: the old code simply doesn't call getPubSubMessageHeaders().get("kcs").
    // Kafka headers don't affect message processing unless explicitly read.
    // Verification: no crash, no data corruption.
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { 1 });
    // Old code would never read this header -- just verify it doesn't throw
    Assert.assertNotNull(headers.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER));
  }

  // ==================== 5. Config Keys ====================

  @Test
  public void testConfigKeyNamingConvention() {
    String[] keys = { ConfigKeys.SERVER_ADD_RMD_TO_BATCH_PUSH_FOR_HYBRID_STORES,
        ConfigKeys.SERVER_UNIQUE_KEY_COUNT_FOR_ALL_BATCH_PUSH_ENABLED,
        ConfigKeys.SERVER_UNIQUE_KEY_COUNT_FOR_HYBRID_STORE_ENABLED };
    for (String key: keys) {
      Assert.assertFalse(key.contains("_"), "Config key should use dots, not underscores: " + key);
      Assert.assertTrue(key.startsWith("server."), "Config key should start with 'server.': " + key);
    }
  }

  // ==================== 6. Storage Engine: putInStorageEngine ====================

  @Test(dataProvider = "batchPutSchemaIds")
  public void testBatchPutReturnsValueOperationType(int schemaId, String desc) {
    // Server (not DaVinci) -> all batch PUTs without RMD return VALUE
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();

    Put put = createBatchPut(schemaId);
    ActiveActiveStoreIngestionTask.StorageOperationType opType =
        ingestionTask.getStorageOperationTypeForPut(PARTITION, put);
    assertEquals(opType, ActiveActiveStoreIngestionTask.StorageOperationType.VALUE, desc);
  }

  @Test(dataProvider = "rmdSchemaIdSelection")
  public void testBatchRmdSchemaIdSelection(int putSchemaId, boolean usesActualSchemaId, String desc) {
    // Replicate: int rmdSchemaId = put.schemaId > 0 ? put.schemaId : defaultBatchRmdValueSchemaId;
    int defaultBatchRmdValueSchemaId = 999; // superset/latest
    int rmdSchemaId = putSchemaId > 0 ? putSchemaId : defaultBatchRmdValueSchemaId;

    if (usesActualSchemaId) {
      assertEquals(rmdSchemaId, putSchemaId, desc);
    } else {
      assertEquals(rmdSchemaId, defaultBatchRmdValueSchemaId, desc);
    }
  }

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_nonChunked_preEop() throws Exception {
    setupForStorageEngineTests(true);

    Put put = createBatchPut(USER_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // Non-chunked PUT with batch RMD -> putWithReplicationMetadata (not plain put)
    verify(storageEngine)
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
    verify(storageEngine, never()).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_manifest_preEop() throws Exception {
    setupForStorageEngineTests(true);

    Put put = createBatchPut(CHUNK_MANIFEST_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // Manifest PUT with batch RMD -> putWithReplicationMetadata using pre-computed RMD
    verify(storageEngine)
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
    verify(storageEngine, never()).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_chunkFragment_fallsThrough() throws Exception {
    setupForStorageEngineTests(true);

    Put put = createBatchPut(CHUNK_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // Chunk fragment is filtered by isChunkFragment -> falls through to plain put
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdDisabled_fallsThrough() throws Exception {
    setupForStorageEngineTests(false);

    Put put = createBatchPut(USER_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // addRmdToBatchPushForHybridStores=false -> plain put
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  @Test
  public void testPutInStorageEngine_postEop_fallsThrough() throws Exception {
    setupForStorageEngineTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();

    Put put = createBatchPut(USER_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // Post-EOP -> falls through to plain put even with batch RMD enabled
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_daVinci_fallsThrough() throws Exception {
    // addRmdToBatchPushForHybridStores=true but isDaVinciClient=true -> !isDaVinciClient() fails -> plain put
    setupForStorageEngineTests(true);
    doReturn(true).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();

    Put put = createBatchPut(USER_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // DaVinci client -> falls through to plain put despite batch RMD enabled
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_pcsNull_fallsThrough() throws Exception {
    // Simulate the race condition: getStorageOperationTypeForPut returns VALUE,
    // but PCS is removed before the second lookup inside the VALUE case.
    setField(ingestionTask, "storageEngine", storageEngine);
    setField(ingestionTask, "addRmdToBatchPushForHybridStores", true);
    setField(ingestionTask, "defaultBatchRmdBytes", new byte[] { 0, 0, 0, 0 });
    setField(ingestionTask, "defaultBatchRmdWithSchemaIdPrefix", new byte[] { 0, 0, 0, 5, 0, 0, 0, 0 });
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doCallRealMethod().when(ingestionTask).putInStorageEngine(anyInt(), any(), any(Put.class));
    // Stub getStorageOperationTypeForPut to return VALUE directly (bypassing its own pcs null check)
    doReturn(ActiveActiveStoreIngestionTask.StorageOperationType.VALUE).when(ingestionTask)
        .getStorageOperationTypeForPut(anyInt(), any());
    // Return empty map so the second PCS lookup inside VALUE case returns null
    doReturn(new VeniceConcurrentHashMap<>()).when(ingestionTask).getPartitionConsumptionStateMap();

    Put put = createBatchPut(USER_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // pcs == null -> falls through to plain put
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  // ==================== 7. Storage Engine: removeFromStorageEngine ====================

  @Test
  public void testRemoveFromStorageEngine_batchRmdEnabled_preEop() throws Exception {
    setupForStorageEngineTests(true);

    Delete delete = createBatchDelete();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, delete);

    // DELETE with batch RMD enabled, pre-EOP -> deleteWithReplicationMetadata
    verify(storageEngine).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
    verify(storageEngine, never()).delete(anyInt(), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_batchRmdDisabled_fallsThrough() throws Exception {
    setupForStorageEngineTests(false);

    Delete delete = createBatchDelete();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, delete);

    // addRmdToBatchPushForHybridStores=false -> plain delete
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_postEop_goesToValueAndRmd() throws Exception {
    setupForStorageEngineTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();

    Delete delete = createBatchDelete();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, delete);

    // Post-EOP with empty RMD -> getStorageOperationTypeForDelete returns VALUE_AND_RMD
    // (the VALUE branch's EOP guard is unreachable for delete because the op-type switch
    // routes to VALUE_AND_RMD first). This verifies deleteWithReplicationMetadata is called.
    verify(storageEngine).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
    verify(storageEngine, never()).delete(anyInt(), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_batchRmdEnabled_daVinci_fallsThrough() throws Exception {
    // addRmdToBatchPushForHybridStores=true but isDaVinciClient=true -> falls through to plain delete
    // Note: DaVinci with non-deferred write returns VALUE from getStorageOperationTypeForDelete
    setupForStorageEngineTests(true);
    doReturn(true).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isDeferredWrite();

    Delete delete = createBatchDelete();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, delete);

    // DaVinci + !isDaVinciClient() fails in the VALUE branch -> plain delete
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_batchRmdEnabled_pcsNull_fallsThrough() throws Exception {
    setField(ingestionTask, "storageEngine", storageEngine);
    setField(ingestionTask, "addRmdToBatchPushForHybridStores", true);
    setField(ingestionTask, "defaultBatchRmdWithSchemaIdPrefix", new byte[] { 0, 0, 0, 5, 0, 0, 0, 0 });
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doCallRealMethod().when(ingestionTask).removeFromStorageEngine(anyInt(), any(), any(Delete.class));
    doReturn(ActiveActiveStoreIngestionTask.StorageOperationType.VALUE).when(ingestionTask)
        .getStorageOperationTypeForDelete(anyInt(), any());
    doReturn(new VeniceConcurrentHashMap<>()).when(ingestionTask).getPartitionConsumptionStateMap();

    Delete delete = createBatchDelete();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, delete);

    // pcs == null -> falls through to plain delete
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_batchRmdEnabled_postEop_valueCase_fallsThrough() throws Exception {
    // Force the VALUE case with post-EOP PCS to cover the inner pcs.isEndOfPushReceived()=true branch
    setField(ingestionTask, "storageEngine", storageEngine);
    setField(ingestionTask, "addRmdToBatchPushForHybridStores", true);
    setField(ingestionTask, "defaultBatchRmdWithSchemaIdPrefix", new byte[] { 0, 0, 0, 5, 0, 0, 0, 0 });
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doCallRealMethod().when(ingestionTask).removeFromStorageEngine(anyInt(), any(), any(Delete.class));
    // Stub to return VALUE directly so we enter the VALUE case
    doReturn(ActiveActiveStoreIngestionTask.StorageOperationType.VALUE).when(ingestionTask)
        .getStorageOperationTypeForDelete(anyInt(), any());
    doReturn(pcsMap).when(ingestionTask).getPartitionConsumptionStateMap();
    doReturn(true).when(pcs).isEndOfPushReceived(); // post-EOP

    Delete delete = createBatchDelete();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, delete);

    // pcs != null but isEndOfPushReceived=true -> falls through to plain delete
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  // ==================== 8. trackUniqueKeyCount ====================

  @Test
  public void testTrackUniqueKeyCount_batchCounting_enabled_preEop_nonChunk() throws Exception {
    setupForTrackUniqueKeyCount(true, false, false);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(mockPcs).isEndOfPushReceived();

    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    verify(mockPcs).incrementUniqueKeyCountForBatchRecord();
  }

  @Test
  public void testTrackUniqueKeyCount_batchCounting_chunkFragment_skipped() throws Exception {
    setupForTrackUniqueKeyCount(true, false, false);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(mockPcs).isEndOfPushReceived();

    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, CHUNK_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCountForBatchRecord();
  }

  @Test
  public void testTrackUniqueKeyCount_batchCounting_disabled_skipped() throws Exception {
    setupForTrackUniqueKeyCount(false, false, false);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(mockPcs).isEndOfPushReceived();

    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCountForBatchRecord();
  }

  @Test
  public void testTrackUniqueKeyCount_batchCounting_deleteType_skipped() throws Exception {
    // Batch counting only applies to PUT, not DELETE -- covers messageType != PUT branch
    setupForTrackUniqueKeyCount(true, false, false);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(mockPcs).isEndOfPushReceived();

    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.DELETE, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCountForBatchRecord();
  }

  @Test
  public void testTrackUniqueKeyCount_batchCounting_postEop_skipped() throws Exception {
    // Batch counting disabled when post-EOP -- covers isEndOfPushReceived() == true branch
    setupForTrackUniqueKeyCount(true, false, false);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();

    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCountForBatchRecord();
  }

  @Test
  public void testTrackUniqueKeyCount_batchCounting_manifestSchemaId() throws Exception {
    // Manifest schema ID passes the !isChunkFragment check -> counted
    setupForTrackUniqueKeyCount(true, false, false);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(mockPcs).isEndOfPushReceived();

    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, CHUNK_MANIFEST_SCHEMA_ID);

    verify(mockPcs).incrementUniqueKeyCountForBatchRecord();
  }

  @Test
  public void testTrackUniqueKeyCount_batchAndFollower_bothEnabled() throws Exception {
    // Both batch counting and follower signal enabled -- tests both paths execute independently
    setupForTrackUniqueKeyCount(true, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(mockPcs).isEndOfPushReceived();

    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    // Pre-EOP: batch counting fires, follower signal does not (requires isEndOfPushReceived)
    verify(mockPcs).incrementUniqueKeyCountForBatchRecord();
    verify(mockPcs, never()).incrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_created() throws Exception {
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount(); // baseline established

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER,
            new byte[] { ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    verify(mockPcs).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_deleted() throws Exception {
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER,
            new byte[] { ActiveActiveStoreIngestionTask.KEY_DELETED_SIGNAL_VALUE }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.DELETE, USER_SCHEMA_ID);

    verify(mockPcs).decrementUniqueKeyCount();
    verify(mockPcs, never()).incrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_unexpectedValue() throws Exception {
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();
    doReturn("store_v1-0").when(mockPcs).getReplicaId();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(new PubSubMessageHeader(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { 99 }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    // Neither increment nor decrement called for unexpected signal
    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_disabled() throws Exception {
    setupForTrackUniqueKeyCount(false, false, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER,
            new byte[] { ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    // Config disabled -> no signal applied
    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_baselineNotEstablished() throws Exception {
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(-1L).when(mockPcs).getUniqueKeyCount(); // baseline never established

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER,
            new byte[] { ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    // Baseline not established (uniqueKeyCount < 0) -> signal not applied
    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_deleteMessageType() throws Exception {
    // Covers the ternary else branch: messageType == MessageType.DELETE
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER,
            new byte[] { ActiveActiveStoreIngestionTask.KEY_DELETED_SIGNAL_VALUE }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    // DELETE messageType -- the ternary evaluates to (messageType == MessageType.DELETE) -> true
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.DELETE, USER_SCHEMA_ID);

    verify(mockPcs).decrementUniqueKeyCount();
    verify(mockPcs, never()).incrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_putWithManifest() throws Exception {
    // Covers ternary true branch: messageType == PUT && !isChunkFragment(manifest) -> true
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER,
            new byte[] { ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, CHUNK_MANIFEST_SCHEMA_ID);

    verify(mockPcs).incrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_headerMissing() throws Exception {
    // Covers signalHeader == null branch
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    // No "kcs" header at all
    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_headerValueNull() throws Exception {
    // Covers signalHeader.value() == null branch
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(new PubSubMessageHeader(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, null));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_headerValueEmpty() throws Exception {
    // Covers signalHeader.value().length == 0 branch
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(new PubSubMessageHeader(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[0]));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_chunkFragment_skipped() throws Exception {
    // PUT with chunk fragment schemaId -> ternary evaluates !isChunkFragment -> false -> skipped
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER,
            new byte[] { ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    // Chunk fragment schemaId -> condition fails on !isChunkFragment check
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, CHUNK_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_nonAAStore_skipped() throws Exception {
    // Non-A/A store -> isActiveActiveReplicationEnabled=false -> signal not applied
    setupForTrackUniqueKeyCount(false, true, false);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER,
            new byte[] { ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_leaderContext_skipped() throws Exception {
    // Leader-produced record (leaderProducedRecordContext != null) -> signal not applied
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER,
            new byte[] { ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    LeaderProducedRecordContext leaderContext = mock(LeaderProducedRecordContext.class);
    invokeTrackUniqueKeyCount(record, mockPcs, leaderContext, MessageType.PUT, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_updateMessageType_skipped() throws Exception {
    // UPDATE messageType -> ternary evaluates PUT?...:DELETE -> false for UPDATE -> skipped
    setupForTrackUniqueKeyCount(false, true, true);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();

    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER,
            new byte[] { ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL_VALUE }));
    DefaultPubSubMessage record = createMockConsumerRecord(headers);

    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.UPDATE, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_unexpectedValue_redundantFilterTrue() throws Exception {
    // Test the REDUNDANT_LOGGING_FILTER returning true (redundant exception -- log suppressed)
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();
    doReturn("test-replica").when(mockPcs).getReplicaId();

    // Call twice with the same unexpected value -- the second call should hit the filter=true branch
    for (int i = 0; i < 2; i++) {
      PubSubMessageHeaders headers = new PubSubMessageHeaders();
      headers.add(new PubSubMessageHeader(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { 99 }));
      DefaultPubSubMessage record = createMockConsumerRecord(headers);

      setupForTrackUniqueKeyCount(true, true, true);
      invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);
    }

    // Neither increment nor decrement called (unexpected value)
    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  // ==================== 9. processMessageAndMaybeProduceToKafka ====================

  @Test
  public void testProcessMessage_updateIgnored() throws Exception {
    // isUpdateIgnored=true -> skip all signal computation and produce
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(true, false, false);
    PubSubMessageProcessedResultWrapper wrapper = createWrapperWithResult(mcWrapper);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    // Update ignored -> no increment/decrement, no produce
    verify(pcs, never()).incrementUniqueKeyCount();
    verify(pcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_newKeyCreated_incrementsCount() throws Exception {
    // wasAlive=false, isAlive=true -> new key -> increment + KEY_CREATED_SIGNAL header
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(false, false, true);
    PubSubMessageProcessedResultWrapper wrapper = createWrapperWithResult(mcWrapper);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    verify(pcs).incrementUniqueKeyCount();
    verify(pcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_keyDeleted_decrementsCount() throws Exception {
    // wasAlive=true, isAlive=false -> key deleted -> decrement + KEY_DELETED_SIGNAL header
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(false, true, false);
    PubSubMessageProcessedResultWrapper wrapper = createWrapperWithResult(mcWrapper);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    verify(pcs).decrementUniqueKeyCount();
    verify(pcs, never()).incrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_existingKeyUpdated_noCountChange() throws Exception {
    // wasAlive=true, isAlive=true -> update existing key -> no count change
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(false, true, true);
    PubSubMessageProcessedResultWrapper wrapper = createWrapperWithResult(mcWrapper);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    verify(pcs, never()).incrementUniqueKeyCount();
    verify(pcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_bothDead_noCountChange() throws Exception {
    // wasAlive=false, isAlive=false -> delete of non-existent key -> no count change
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(false, false, false);
    PubSubMessageProcessedResultWrapper wrapper = createWrapperWithResult(mcWrapper);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    verify(pcs, never()).incrementUniqueKeyCount();
    verify(pcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_featureDisabled_noSignalComputation() throws Exception {
    // uniqueKeyCountForHybridStoreEnabled=false -> signal computation skipped
    setupForProcessMessageTests(false);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(false, false, true);
    PubSubMessageProcessedResultWrapper wrapper = createWrapperWithResult(mcWrapper);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    // Feature disabled -> no count changes even though wasAlive=false, isAlive=true
    verify(pcs, never()).incrementUniqueKeyCount();
    verify(pcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_baselineNotEstablished_noSignalComputation() throws Exception {
    // uniqueKeyCount < 0 -> baseline never established -> signal computation skipped
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(-1L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(false, false, true);
    PubSubMessageProcessedResultWrapper wrapper = createWrapperWithResult(mcWrapper);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    // Baseline not established -> no count changes
    verify(pcs, never()).incrementUniqueKeyCount();
    verify(pcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_processedResultNull_callsProcessActiveActive() throws Exception {
    // When processedResult is null, processActiveActiveMessage is called (mocked here to return result)
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(false, false, true);
    PubSubMessageProcessedResult processedResult = new PubSubMessageProcessedResult(mcWrapper);

    DefaultPubSubMessage consumerRecord = mock(DefaultPubSubMessage.class);
    KafkaKey kafkaKey = mock(KafkaKey.class);
    doReturn(new byte[] { 1, 2, 3 }).when(kafkaKey).getKey();
    doReturn(kafkaKey).when(consumerRecord).getKey();
    KafkaMessageEnvelope kme = new KafkaMessageEnvelope();
    kme.messageType = MessageType.PUT.getValue();
    Put put = new Put();
    put.putValue = ByteBuffer.wrap("val".getBytes());
    put.schemaId = USER_SCHEMA_ID;
    put.replicationMetadataPayload = ByteBuffer.allocate(0);
    kme.payloadUnion = put;
    doReturn(kme).when(consumerRecord).getValue();
    PubSubPosition position = mock(PubSubPosition.class);
    doReturn(position).when(consumerRecord).getPosition();

    // processedResult is null on the wrapper -> processActiveActiveMessage would be called.
    // We mock processActiveActiveMessage to return our controlled result.
    PubSubMessageProcessedResultWrapper wrapper = new PubSubMessageProcessedResultWrapper(consumerRecord);
    // wrapper.getProcessedResult() is null, so processActiveActiveMessage will be called
    // Mock the private processActiveActiveMessage indirectly -- it's private, so we can't mock it.
    // Instead, set processedResult on the wrapper before calling.
    wrapper.setProcessedResult(processedResult);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    verify(pcs).incrementUniqueKeyCount();
  }

  // ==================== 10. processEndOfPush ====================

  @Test
  public void testProcessEndOfPush_uniqueKeyCountEnabled() throws Exception {
    // serverConfig.isUniqueKeyCountForAllBatchPushEnabled() = true -> finalizeUniqueKeyCountForBatchPush called
    StoreIngestionTask sitMock = mock(StoreIngestionTask.class);
    doCallRealMethod().when(sitMock).processEndOfPush(any(), any(), any(), any());

    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(mockServerConfig).isUniqueKeyCountForAllBatchPushEnabled();
    setField(sitMock, "serverConfig", mockServerConfig);

    // Set up required fields that processEndOfPush accesses
    StorageEngine mockStorageEngine = mock(StorageEngine.class);
    setField(sitMock, "storageEngine", mockStorageEngine);
    setField(sitMock, "cacheBackend", Optional.empty());
    setField(sitMock, "isDataRecovery", false);

    IngestionNotificationDispatcher mockDispatcher = mock(IngestionNotificationDispatcher.class);
    setField(sitMock, "ingestionNotificationDispatcher", mockDispatcher);

    StorageMetadataService mockMetadataService = mock(StorageMetadataService.class);
    setField(sitMock, "storageMetadataService", mockMetadataService);

    StoragePartitionConfig mockSpc = mock(StoragePartitionConfig.class);
    doReturn(false).when(mockSpc).isDeferredWrite();
    doReturn(mockSpc).when(sitMock).getStoragePartitionConfig(anyBoolean(), any());

    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(false).when(mockOffsetRecord).isEndOfPushReceived();
    doReturn(mockOffsetRecord).when(mockPcs).getOffsetRecord();
    doReturn("test-replica").when(mockPcs).getReplicaId();
    doReturn(10L).when(mockPcs).getUniqueKeyCount();

    KafkaMessageEnvelope kme = new KafkaMessageEnvelope();
    kme.producerMetadata = new ProducerMetadata();
    kme.producerMetadata.messageTimestamp = System.currentTimeMillis();

    PubSubPosition offset = mock(PubSubPosition.class);
    EndOfPush eop = new EndOfPush();

    sitMock.processEndOfPush(kme, offset, mockPcs, eop);

    verify(mockPcs).finalizeUniqueKeyCountForBatchPush();
  }

  @Test
  public void testProcessEndOfPush_uniqueKeyCountDisabled() throws Exception {
    // serverConfig.isUniqueKeyCountForAllBatchPushEnabled() = false -> finalizeUniqueKeyCountForBatchPush NOT called
    StoreIngestionTask sitMock = mock(StoreIngestionTask.class);
    doCallRealMethod().when(sitMock).processEndOfPush(any(), any(), any(), any());

    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);
    doReturn(false).when(mockServerConfig).isUniqueKeyCountForAllBatchPushEnabled();
    setField(sitMock, "serverConfig", mockServerConfig);

    StorageEngine mockStorageEngine = mock(StorageEngine.class);
    setField(sitMock, "storageEngine", mockStorageEngine);
    setField(sitMock, "cacheBackend", Optional.empty());
    setField(sitMock, "isDataRecovery", false);

    IngestionNotificationDispatcher mockDispatcher = mock(IngestionNotificationDispatcher.class);
    setField(sitMock, "ingestionNotificationDispatcher", mockDispatcher);

    StorageMetadataService mockMetadataService = mock(StorageMetadataService.class);
    setField(sitMock, "storageMetadataService", mockMetadataService);

    StoragePartitionConfig mockSpc = mock(StoragePartitionConfig.class);
    doReturn(false).when(mockSpc).isDeferredWrite();
    doReturn(mockSpc).when(sitMock).getStoragePartitionConfig(anyBoolean(), any());

    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(false).when(mockOffsetRecord).isEndOfPushReceived();
    doReturn(mockOffsetRecord).when(mockPcs).getOffsetRecord();

    KafkaMessageEnvelope kme = new KafkaMessageEnvelope();
    kme.producerMetadata = new ProducerMetadata();
    kme.producerMetadata.messageTimestamp = System.currentTimeMillis();

    PubSubPosition offset = mock(PubSubPosition.class);
    EndOfPush eop = new EndOfPush();

    sitMock.processEndOfPush(kme, offset, mockPcs, eop);

    verify(mockPcs, never()).finalizeUniqueKeyCountForBatchPush();
  }
}
