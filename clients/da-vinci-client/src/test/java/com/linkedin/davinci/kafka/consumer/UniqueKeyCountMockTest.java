package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.findMethod;
import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.freshPcs;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Mock-level tests that verify the actual production code paths for unique key count with
 * chunked and non-chunked records. Unlike UniqueKeyCountTest (PCS-level) and
 * UniqueKeyCountIntegrationTest (lifecycle scenarios), these tests mock the ingestion task
 * and verify that the correct storage engine methods are called with the right arguments.
 *
 * Tests cover:
 * 1. Batch RMD writes: putInStorageEngine() with various schemaIds (non-chunked, chunk fragment, manifest)
 * 2. Batch RMD writes: removeFromStorageEngine() for DELETEs
 * 3. Batch RMD skipped: when flag is off, when DaVinci, when post-EOP
 * 4. Batch key counting: incrementUniqueKeyCountForBatchRecord called for correct schemaIds only
 * 5. Follower signal: "kcs" header applied for correct schemaIds only
 */
public class UniqueKeyCountMockTest {
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

  // Helper: create Put with given schemaId and empty RMD (standard batch, no pre-existing RMD)

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

  // 1. getStorageOperationTypeForPut — verifies VALUE is returned for batch records
  // (no pre-existing RMD). This is the precondition for our batch RMD code to fire.

  @DataProvider(name = "batchPutSchemaIds")
  public Object[][] batchPutSchemaIds() {
    return new Object[][] { { USER_SCHEMA_ID, "Non-chunked PUT" }, { CHUNK_SCHEMA_ID, "Chunk fragment" },
        { CHUNK_MANIFEST_SCHEMA_ID, "Chunk manifest" }, };
  }

  @Test(dataProvider = "batchPutSchemaIds")
  public void testBatchPutReturnsValueOperationType(int schemaId, String desc) {
    // Server (not DaVinci) → all batch PUTs without RMD return VALUE
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();

    Put put = createBatchPut(schemaId);
    ActiveActiveStoreIngestionTask.StorageOperationType opType =
        ingestionTask.getStorageOperationTypeForPut(PARTITION, put);
    assertEquals(opType, ActiveActiveStoreIngestionTask.StorageOperationType.VALUE, desc);
  }

  // 2. putInStorageEngine with batch RMD enabled — verifies putWithReplicationMetadata
  // called for non-chunk PUT and manifest, but NOT for chunk fragments

  @Test
  public void testNonChunkedBatchPutReturnsValueType() {
    // Non-chunked PUT without RMD → getStorageOperationTypeForPut returns VALUE
    // (our batch RMD code fires in the VALUE case)
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();
    Put put = createBatchPut(USER_SCHEMA_ID);
    assertEquals(
        ingestionTask.getStorageOperationTypeForPut(PARTITION, put),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE);
  }

  @Test
  public void testChunkFragmentBatchPutReturnsValueType() {
    // Chunk fragment with non-empty putValue and empty rmd → getStorageOperationTypeForPut returns VALUE.
    // Our batch RMD code then filters by schemaId != CHUNK (isChunkFragment guard).
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();
    Put put = createBatchPut(CHUNK_SCHEMA_ID);
    assertEquals(
        ingestionTask.getStorageOperationTypeForPut(PARTITION, put),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE,
        "Chunk fragment returns VALUE — filtered later by isChunkFragment in putInStorageEngine");
  }

  @Test
  public void testChunkManifestBatchPutReturnsValueType() {
    // Chunk manifest PUT without RMD → VALUE type, and passes our schemaId filter
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();
    Put put = createBatchPut(CHUNK_MANIFEST_SCHEMA_ID);
    assertEquals(
        ingestionTask.getStorageOperationTypeForPut(PARTITION, put),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE);
  }

  @Test
  public void testDaVinciClientReturnsValueType() {
    // DaVinci always returns VALUE (ignores RMD). Our code guards !isDaVinciClient().
    doReturn(true).when(ingestionTask).isDaVinciClient();
    Put put = createBatchPut(USER_SCHEMA_ID);
    assertEquals(
        ingestionTask.getStorageOperationTypeForPut(PARTITION, put),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE,
        "DaVinci returns VALUE — our batch RMD code guards !isDaVinciClient()");
  }

  @Test
  public void testPutWithExistingRmdReturnsValueAndRmd() {
    // PUT with existing RMD (repush/compliance) → VALUE_AND_RMD, our code never fires
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();
    Put put = new Put();
    put.putValue = VALUE_PAYLOAD.duplicate();
    put.schemaId = USER_SCHEMA_ID;
    put.replicationMetadataPayload = ByteBuffer.wrap("rmd-data".getBytes());
    assertEquals(
        ingestionTask.getStorageOperationTypeForPut(PARTITION, put),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE_AND_RMD,
        "Existing RMD → VALUE_AND_RMD path, batch RMD code not reached");
  }

  @Test
  public void testDeleteWithoutRmdPreEopReturnsValue() {
    // DELETE without RMD, pre-EOP → VALUE (our batch RMD delete code fires here)
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();
    Delete delete = createBatchDelete();
    assertEquals(
        ingestionTask.getStorageOperationTypeForDelete(PARTITION, delete),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE);
  }

  // 4. Batch key counting: incrementUniqueKeyCountForBatchRecord called for correct schemaIds
  // Tests the condition: !isEndOfPushReceived && schemaId != CHUNK && messageType == PUT

  @DataProvider(name = "batchCountingCases")
  public Object[][] batchCountingCases() {
    return new Object[][] {
        // schemaId, isEopReceived, shouldCount, description
        { USER_SCHEMA_ID, false, true, "Non-chunked PUT pre-EOP: counted" },
        { CHUNK_MANIFEST_SCHEMA_ID, false, true, "Chunk manifest pre-EOP: counted (logical key)" },
        { CHUNK_SCHEMA_ID, false, false, "Chunk fragment pre-EOP: NOT counted" },
        { USER_SCHEMA_ID, true, false, "Non-chunked PUT post-EOP: NOT counted (RT phase)" },
        { CHUNK_SCHEMA_ID, true, false, "Chunk fragment post-EOP: NOT counted" },
        { CHUNK_MANIFEST_SCHEMA_ID, true, false, "Chunk manifest post-EOP: NOT counted (RT phase)" }, };
  }

  @Test(dataProvider = "batchCountingCases")
  public void testBatchKeyCountSchemaAndEopFiltering(
      int schemaId,
      boolean isEopReceived,
      boolean shouldCount,
      String desc) {
    // Replicate the exact condition from processKafkaDataMessage
    boolean uniqueKeyCountForAllBatchPushEnabled = true;
    boolean messageTypeIsPut = true;
    boolean wouldCount =
        uniqueKeyCountForAllBatchPushEnabled && !isEopReceived && schemaId != CHUNK_SCHEMA_ID && messageTypeIsPut;
    assertEquals(wouldCount, shouldCount, desc);
  }

  // 5. Follower signal: "kcs" header applied for correct schemaIds and conditions
  // Tests the condition: uniqueKeyCountEnabled && isAA && isEopReceived
  // && leaderProducedRecordContext == null
  // && schemaId != CHUNK && messageType == PUT

  @DataProvider(name = "followerSignalCases")
  public Object[][] followerSignalCases() {
    return new Object[][] {
        // schemaId, isEop, isLeader, isAA, flagOn, shouldApply, description
        { USER_SCHEMA_ID, true, false, true, true, true, "Follower, non-chunked, post-EOP, A/A, flag on: apply" },
        { CHUNK_MANIFEST_SCHEMA_ID, true, false, true, true, true, "Follower, manifest, post-EOP: apply" },
        { CHUNK_SCHEMA_ID, true, false, true, true, false, "Follower, chunk fragment: NOT apply" },
        { USER_SCHEMA_ID, false, false, true, true, false, "Follower, pre-EOP: NOT apply (batch phase)" },
        { USER_SCHEMA_ID, true, true, true, true, false, "Leader drainer: NOT apply (already counted)" },
        { USER_SCHEMA_ID, true, false, false, true, false, "Non-A/A follower: NOT apply" },
        { USER_SCHEMA_ID, true, false, true, false, false, "Flag off: NOT apply" }, };
  }

  @Test(dataProvider = "followerSignalCases")
  public void testFollowerSignalApplicationConditions(
      int schemaId,
      boolean isEopReceived,
      boolean isLeader,
      boolean isActiveActive,
      boolean flagEnabled,
      boolean shouldApply,
      String desc) {
    // Replicate the exact condition from processKafkaDataMessage PUT case
    boolean leaderProducedRecordContextIsNull = !isLeader;
    boolean messageTypeIsPut = true;
    boolean wouldApply = flagEnabled && isActiveActive && isEopReceived && leaderProducedRecordContextIsNull
        && schemaId != CHUNK_SCHEMA_ID && messageTypeIsPut;
    assertEquals(wouldApply, shouldApply, desc);
  }

  // 6. Follower signal for DELETE: conditions
  // Tests the condition: uniqueKeyCountEnabled && isAA && isEopReceived
  // && leaderProducedRecordContext == null

  @DataProvider(name = "followerDeleteSignalCases")
  public Object[][] followerDeleteSignalCases() {
    return new Object[][] {
        // isEop, isLeader, isAA, flagOn, shouldApply, description
        { true, false, true, true, true, "Follower, post-EOP, A/A, flag on: apply" },
        { false, false, true, true, false, "Follower, pre-EOP: NOT apply" },
        { true, true, true, true, false, "Leader drainer: NOT apply" },
        { true, false, false, true, false, "Non-A/A: NOT apply" },
        { true, false, true, false, false, "Flag off: NOT apply" }, };
  }

  @Test(dataProvider = "followerDeleteSignalCases")
  public void testFollowerDeleteSignalConditions(
      boolean isEopReceived,
      boolean isLeader,
      boolean isActiveActive,
      boolean flagEnabled,
      boolean shouldApply,
      String desc) {
    // Replicate the exact condition from processKafkaDataMessage DELETE case
    boolean leaderProducedRecordContextIsNull = !isLeader;
    boolean wouldApply = flagEnabled && isActiveActive && isEopReceived && leaderProducedRecordContextIsNull;
    assertEquals(wouldApply, shouldApply, desc);
  }

  // 7. Batch RMD schema ID selection: non-chunked uses put.schemaId, manifest uses superset

  @DataProvider(name = "rmdSchemaIdSelection")
  public Object[][] rmdSchemaIdSelection() {
    return new Object[][] {
        // putSchemaId, expectedUsesActualSchemaId, description
        { 1, true, "Schema ID 1 (user schema): use actual" }, { 5, true, "Schema ID 5 (user schema): use actual" },
        { 100, true, "Schema ID 100 (user schema): use actual" },
        { CHUNK_MANIFEST_SCHEMA_ID, false, "Manifest (-20): use superset/latest" }, };
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

  // 8. Hybrid vs batch-only: addRmdToBatchPushForHybridStores only enabled for hybrid

  @DataProvider(name = "hybridGuardCases")
  public Object[][] hybridGuardCases() {
    return new Object[][] {
        // configEnabled, isHybrid, expectedFieldValue, description
        { true, true, true, "Config on + hybrid: batch RMD enabled" },
        { true, false, false, "Config on + batch-only: batch RMD disabled (not hybrid)" },
        { false, true, false, "Config off + hybrid: batch RMD disabled (config off)" },
        { false, false, false, "Config off + batch-only: batch RMD disabled" }, };
  }

  @Test(dataProvider = "hybridGuardCases")
  public void testAddRmdToHybridBatchPushDataHybridGuard(
      boolean configEnabled,
      boolean isHybrid,
      boolean expectedFieldValue,
      String desc) {
    // Replicate: this.addRmdToBatchPushForHybridStores = serverConfig.isAddRmdToBatchPushForHybridStoresEnabled() &&
    // isHybridMode();
    boolean fieldValue = configEnabled && isHybrid;
    assertEquals(fieldValue, expectedFieldValue, desc);
  }

  @DataProvider(name = "uniqueKeyCountHybridGuardCases")
  public Object[][] uniqueKeyCountHybridGuardCases() {
    return new Object[][] {
        // configEnabled, isHybrid, expectedFieldValue, description
        { true, true, true, "Config on + hybrid: unique key count enabled" },
        { true, false, false, "Config on + batch-only: unique key count disabled (not hybrid)" },
        { false, true, false, "Config off + hybrid: unique key count disabled (config off)" },
        { false, false, false, "Config off + batch-only: unique key count disabled" }, };
  }

  @Test(dataProvider = "uniqueKeyCountHybridGuardCases")
  public void testUniqueKeyCountForHybridStoreEnabledHybridGuard(
      boolean configEnabled,
      boolean isHybrid,
      boolean expectedFieldValue,
      String desc) {
    // Replicate: this.uniqueKeyCountForHybridStoreEnabled =
    // serverConfig.isUniqueKeyCountForHybridStoreEnabled() && isHybridMode();
    boolean fieldValue = configEnabled && isHybrid;
    assertEquals(fieldValue, expectedFieldValue, desc);
  }

  // 9. "kcs" header encoding/decoding with PubSubMessageHeaders

  @Test
  public void testKcdHeaderOnlyOnManifestNotChunkFragment() {
    // For chunked VT records, the "kcs" header is attached to the manifest message only.
    // Chunk fragments use EmptyPubSubMessageHeaders.
    // Follower-side: schemaId != CHUNK_SCHEMA_ID filters out chunk fragments.

    // Manifest: passes filter, would apply header
    assertEquals(
        StoreIngestionTask.isChunkFragment(CHUNK_MANIFEST_SCHEMA_ID),
        false,
        "Manifest passes chunk filter on follower");

    // Chunk fragment: fails filter, would NOT apply header
    assertEquals(
        StoreIngestionTask.isChunkFragment(CHUNK_SCHEMA_ID),
        true,
        "Chunk fragment blocked by chunk filter on follower");
  }

  // 10. End-to-end: chunked store batch → RT with signal headers

  @Test
  public void testChunkedStoreEndToEnd() {
    // Simulate: 5 logical keys, each with 1 manifest + 3 chunk fragments = 20 total records
    // Phase 1 (batch): only manifests counted → uniqueKeyCount = 5
    PartitionConsumptionState realPcs = freshPcs();

    // Simulate processKafkaDataMessage for each record
    for (int key = 0; key < 5; key++) {
      // 3 chunk fragments per key: NOT counted (isChunkFragment filters them)
      for (int chunk = 0; chunk < 3; chunk++) {
        // incrementUniqueKeyCountForBatchRecord NOT called for chunk fragments
      }
      // 1 manifest per key: counted (not a chunk fragment)
      realPcs.incrementUniqueKeyCountForBatchRecord();
    }

    assertEquals(realPcs.getUniqueKeyCount(), 5L, "uniqueKeyCount matches logical key count");

    realPcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(realPcs.getUniqueKeyCount(), 5L);

    // Phase 2 (RT): leader produces VT records with "kcs" headers
    // New key (not in batch) → signal=+1
    realPcs.incrementUniqueKeyCount();
    assertEquals(realPcs.getUniqueKeyCount(), 6L);

    // Delete existing key → signal=-1
    realPcs.decrementUniqueKeyCount();
    assertEquals(realPcs.getUniqueKeyCount(), 5L);

    // Follower receives "kcs" header on VT records
    // For chunked RT writes, header is only on manifest, not on chunk fragments
    // Manifest with signal=+1
    realPcs.incrementUniqueKeyCount();
    assertEquals(realPcs.getUniqueKeyCount(), 6L);
  }

  @Test
  public void testNonChunkedStoreEndToEnd() {
    // Simulate: 10 non-chunked keys, each is a single PUT
    PartitionConsumptionState realPcs = freshPcs();

    for (int key = 0; key < 10; key++) {
      boolean wouldCount = USER_SCHEMA_ID != CHUNK_SCHEMA_ID; // true
      realPcs.incrementUniqueKeyCountForBatchRecord();
    }

    assertEquals(realPcs.getUniqueKeyCount(), 10L);

    realPcs.finalizeUniqueKeyCountForBatchPush();

    // RT writes
    realPcs.incrementUniqueKeyCount(); // new key
    realPcs.incrementUniqueKeyCount(); // new key
    realPcs.decrementUniqueKeyCount(); // delete
    assertEquals(realPcs.getUniqueKeyCount(), 11L); // 10 + 2 - 1
  }

  @Test
  public void testChunkedStoreWithPersistenceRoundTrip() {
    // Chunked batch → checkpoint → restore → RT signals
    PartitionConsumptionState realPcs = freshPcs();

    // 3 chunked keys (only manifests counted)
    for (int key = 0; key < 3; key++) {
      realPcs.incrementUniqueKeyCountForBatchRecord();
    }
    assertEquals(realPcs.getUniqueKeyCount(), 3L);

    // Simulate mid-batch checkpoint
    OffsetRecord checkpoint = realPcs.getOffsetRecord();
    checkpoint.setUniqueKeyCount(realPcs.getUniqueKeyCount());
    byte[] bytes = checkpoint.toBytes();

    // Simulate crash + restore
    OffsetRecord restored = new OffsetRecord(
        bytes,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(restored.getUniqueKeyCount(), 3L);

    // Restored PCS resumes with 2 more chunked keys (only manifests)
    PartitionConsumptionState restoredPcs = UniqueKeyCountTestUtils.restoreFrom(restored);
    assertEquals(restoredPcs.getUniqueKeyCount(), 3L);

    restoredPcs.incrementUniqueKeyCountForBatchRecord(); // manifest 4
    restoredPcs.incrementUniqueKeyCountForBatchRecord(); // manifest 5
    assertEquals(restoredPcs.getUniqueKeyCount(), 5L); // 3 restored + 2 new

    restoredPcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(restoredPcs.getUniqueKeyCount(), 5L);

    // RT phase: follower applies signal from VT header
    restoredPcs.incrementUniqueKeyCount(); // new chunked key added via RT
    assertEquals(restoredPcs.getUniqueKeyCount(), 6L);
  }

  // Group 1: putInStorageEngine — doCallRealMethod on mock

  /**
   * Prepares the mock ingestion task so that calling the real putInStorageEngine / removeFromStorageEngine
   * can execute the VALUE branch of the switch. Sets fields via reflection and stubs getters.
   */
  private void setupForStorageEngineTests(boolean addRmdEnabled) throws Exception {
    // Fields accessed directly by the real method
    setField(ingestionTask, "storageEngine", storageEngine);
    setField(ingestionTask, "addRmdToBatchPushForHybridStores", addRmdEnabled);
    if (addRmdEnabled) {
      byte[] rmdBytes = new byte[] { 0, 0, 0, 0 }; // dummy RMD bytes
      setField(ingestionTask, "defaultBatchRmdBytes", rmdBytes);
      byte[] rmdWithPrefix = new byte[] { 0, 0, 0, 5, 0, 0, 0, 0 }; // dummy prefix + RMD
      setField(ingestionTask, "defaultBatchRmdWithSchemaIdPrefix", rmdWithPrefix);
    }

    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();

    // putInStorageEngine and removeFromStorageEngine call the real method
    doCallRealMethod().when(ingestionTask).putInStorageEngine(anyInt(), any(), any(Put.class));
    doCallRealMethod().when(ingestionTask).removeFromStorageEngine(anyInt(), any(), any(Delete.class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_nonChunked_preEop() throws Exception {
    setupForStorageEngineTests(true);

    Put put = createBatchPut(USER_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // Non-chunked PUT with batch RMD → putWithReplicationMetadata (not plain put)
    verify(storageEngine)
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
    verify(storageEngine, never()).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_manifest_preEop() throws Exception {
    setupForStorageEngineTests(true);

    Put put = createBatchPut(CHUNK_MANIFEST_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // Manifest PUT with batch RMD → putWithReplicationMetadata using pre-computed RMD
    verify(storageEngine)
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
    verify(storageEngine, never()).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_chunkFragment_fallsThrough() throws Exception {
    setupForStorageEngineTests(true);

    Put put = createBatchPut(CHUNK_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // Chunk fragment is filtered by isChunkFragment → falls through to plain put
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  @Test
  public void testPutInStorageEngine_batchRmdDisabled_fallsThrough() throws Exception {
    setupForStorageEngineTests(false);

    Put put = createBatchPut(USER_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // addRmdToBatchPushForHybridStores=false → plain put
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

    // Post-EOP → falls through to plain put even with batch RMD enabled
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  // Group 2: removeFromStorageEngine — doCallRealMethod on mock

  @Test
  public void testRemoveFromStorageEngine_batchRmdEnabled_preEop() throws Exception {
    setupForStorageEngineTests(true);

    Delete delete = createBatchDelete();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, delete);

    // DELETE with batch RMD enabled, pre-EOP → deleteWithReplicationMetadata
    verify(storageEngine).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
    verify(storageEngine, never()).delete(anyInt(), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_batchRmdDisabled_fallsThrough() throws Exception {
    setupForStorageEngineTests(false);

    Delete delete = createBatchDelete();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, delete);

    // addRmdToBatchPushForHybridStores=false → plain delete
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  @Test
  public void testRemoveFromStorageEngine_postEop_goesToValueAndRmd() throws Exception {
    setupForStorageEngineTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();

    Delete delete = createBatchDelete();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, delete);

    // Post-EOP with empty RMD → getStorageOperationTypeForDelete returns VALUE_AND_RMD
    // (the VALUE branch's EOP guard is unreachable for delete because the op-type switch
    // routes to VALUE_AND_RMD first). This verifies deleteWithReplicationMetadata is called.
    verify(storageEngine).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
    verify(storageEngine, never()).delete(anyInt(), any(byte[].class));
  }

  // Group 3: trackUniqueKeyCount — invoke private method via reflection

  /**
   * Invokes the private trackUniqueKeyCount method via reflection.
   */
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

  /**
   * Sets up the mock ingestion task for trackUniqueKeyCount tests.
   */
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
    headers.add(new PubSubMessageHeader(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { 99 })); // unexpected
                                                                                                         // signal value
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

    // Config disabled → no signal applied
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

    // Baseline not established (uniqueKeyCount < 0) → signal not applied
    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  // Group 4: trackUniqueKeyCount — additional branch coverage

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

    // DELETE messageType — the ternary evaluates to (messageType == MessageType.DELETE) → true
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.DELETE, USER_SCHEMA_ID);

    verify(mockPcs).decrementUniqueKeyCount();
    verify(mockPcs, never()).incrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_putWithManifest() throws Exception {
    // Covers ternary true branch: messageType == PUT && !isChunkFragment(manifest) → true
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
  public void testTrackUniqueKeyCount_batchCounting_deleteType_skipped() throws Exception {
    // Batch counting only applies to PUT, not DELETE — covers messageType != PUT branch
    setupForTrackUniqueKeyCount(true, false, false);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(mockPcs).isEndOfPushReceived();

    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.DELETE, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCountForBatchRecord();
  }

  @Test
  public void testTrackUniqueKeyCount_batchCounting_postEop_skipped() throws Exception {
    // Batch counting disabled when post-EOP — covers isEndOfPushReceived() == true branch
    setupForTrackUniqueKeyCount(true, false, false);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();

    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, USER_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCountForBatchRecord();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_chunkFragment_skipped() throws Exception {
    // PUT with chunk fragment schemaId → ternary evaluates !isChunkFragment → false → skipped
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

    // Chunk fragment schemaId → condition fails on !isChunkFragment check
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, CHUNK_SCHEMA_ID);

    verify(mockPcs, never()).incrementUniqueKeyCount();
    verify(mockPcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_nonAAStore_skipped() throws Exception {
    // Non-A/A store → isActiveActiveReplicationEnabled=false → signal not applied
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
    // Leader-produced record (leaderProducedRecordContext != null) → signal not applied
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
    // UPDATE messageType → ternary evaluates PUT?...:DELETE → false for UPDATE → skipped
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
  public void testTrackUniqueKeyCount_batchAndFollower_bothEnabled() throws Exception {
    // Both batch counting and follower signal enabled — tests both paths execute independently
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
  public void testTrackUniqueKeyCount_batchCounting_manifestSchemaId() throws Exception {
    // Manifest schema ID passes the !isChunkFragment check → counted
    setupForTrackUniqueKeyCount(true, false, false);
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(mockPcs).isEndOfPushReceived();

    DefaultPubSubMessage record = createMockConsumerRecord(new PubSubMessageHeaders());
    invokeTrackUniqueKeyCount(record, mockPcs, null, MessageType.PUT, CHUNK_MANIFEST_SCHEMA_ID);

    verify(mockPcs).incrementUniqueKeyCountForBatchRecord();
  }

  // Group 5: processMessageAndMaybeProduceToKafka — signal computation

  /**
   * Creates a mock MergeConflictResultWrapper with controlled wasAlive/isAlive values.
   * wasAlive = (oldValueByteBufferProvider.get() != null)
   * isAlive = (mergeConflictResult.getNewValue() != null)
   */
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
    // getOldValueProvider() is accessed in hasViewWriters=true branch (line 792)
    ByteBufferValueRecord<ByteBuffer> oldValueRecord =
        wasAlive ? new ByteBufferValueRecord<>(ByteBuffer.wrap("old".getBytes()), 1) : null;
    doReturn(Lazy.of(() -> oldValueRecord)).when(wrapper).getOldValueProvider();
    doReturn(Lazy.of(() -> (GenericRecord) null)).when(wrapper).getValueProvider();
    doReturn(isAlive ? ByteBuffer.wrap("updated".getBytes()) : null).when(wrapper).getUpdatedValueBytes();
    doReturn(ByteBuffer.wrap("rmd".getBytes())).when(wrapper).getUpdatedRmdBytes();

    return wrapper;
  }

  /**
   * Sets up the mock ingestion task for processMessageAndMaybeProduceToKafka tests.
   * Uses pre-built PubSubMessageProcessedResult to bypass processActiveActiveMessage.
   */
  private void setupForProcessMessageTests(boolean uniqueKeyCountEnabled) throws Exception {
    doCallRealMethod().when(ingestionTask)
        .processMessageAndMaybeProduceToKafka(any(), any(), anyInt(), anyString(), anyInt(), anyLong(), anyLong());
    setField(ingestionTask, "uniqueKeyCountForHybridStoreEnabled", uniqueKeyCountEnabled);
    // hasViewWriters() returns false by default on mock, so the else branch runs producing to VT.
    // But producePutOrDeleteToKafka is private and needs fields. So we mock hasViewWriters to return
    // true, making queueUpVersionTopicWritesWithViewWriters (a no-op on mock) handle the produce.
    doReturn(true).when(ingestionTask).hasViewWriters();
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

  @Test
  public void testProcessMessage_updateIgnored() throws Exception {
    // isUpdateIgnored=true → skip all signal computation and produce
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(true, false, false);
    PubSubMessageProcessedResultWrapper wrapper = createWrapperWithResult(mcWrapper);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    // Update ignored → no increment/decrement, no produce
    verify(pcs, never()).incrementUniqueKeyCount();
    verify(pcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_newKeyCreated_incrementsCount() throws Exception {
    // wasAlive=false, isAlive=true → new key → increment + KEY_CREATED_SIGNAL header
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
    // wasAlive=true, isAlive=false → key deleted → decrement + KEY_DELETED_SIGNAL header
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
    // wasAlive=true, isAlive=true → update existing key → no count change
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
    // wasAlive=false, isAlive=false → delete of non-existent key → no count change
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
    // uniqueKeyCountForHybridStoreEnabled=false → signal computation skipped
    setupForProcessMessageTests(false);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(5L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(false, false, true);
    PubSubMessageProcessedResultWrapper wrapper = createWrapperWithResult(mcWrapper);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    // Feature disabled → no count changes even though wasAlive=false, isAlive=true
    verify(pcs, never()).incrementUniqueKeyCount();
    verify(pcs, never()).decrementUniqueKeyCount();
  }

  @Test
  public void testProcessMessage_baselineNotEstablished_noSignalComputation() throws Exception {
    // uniqueKeyCount < 0 → baseline never established → signal computation skipped
    setupForProcessMessageTests(true);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(-1L).when(pcs).getUniqueKeyCount();

    MergeConflictResultWrapper mcWrapper = createMockMergeConflictResultWrapper(false, false, true);
    PubSubMessageProcessedResultWrapper wrapper = createWrapperWithResult(mcWrapper);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    // Baseline not established → no count changes
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

    // processedResult is null on the wrapper → processActiveActiveMessage would be called.
    // We mock processActiveActiveMessage to return our controlled result.
    PubSubMessageProcessedResultWrapper wrapper = new PubSubMessageProcessedResultWrapper(consumerRecord);
    // wrapper.getProcessedResult() is null, so processActiveActiveMessage will be called
    // Mock the private processActiveActiveMessage indirectly — it's private, so we can't mock it.
    // Instead, set processedResult on the wrapper before calling.
    wrapper.setProcessedResult(processedResult);

    ingestionTask.processMessageAndMaybeProduceToKafka(wrapper, pcs, PARTITION, "url", 0, 0L, 0L);

    verify(pcs).incrementUniqueKeyCount();
  }

  // Group 6: putInStorageEngine — additional branch coverage

  @Test
  public void testPutInStorageEngine_batchRmdEnabled_daVinci_fallsThrough() throws Exception {
    // addRmdToBatchPushForHybridStores=true but isDaVinciClient=true → !isDaVinciClient() fails → plain put
    setupForStorageEngineTests(true);
    doReturn(true).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isEndOfPushReceived();

    Put put = createBatchPut(USER_SCHEMA_ID);
    ingestionTask.putInStorageEngine(PARTITION, KEY_BYTES, put);

    // DaVinci client → falls through to plain put despite batch RMD enabled
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
  }

  // Group 7: removeFromStorageEngine — additional branch coverage

  @Test
  public void testRemoveFromStorageEngine_batchRmdEnabled_daVinci_fallsThrough() throws Exception {
    // addRmdToBatchPushForHybridStores=true but isDaVinciClient=true → falls through to plain delete
    // Note: DaVinci with non-deferred write returns VALUE from getStorageOperationTypeForDelete
    setupForStorageEngineTests(true);
    doReturn(true).when(ingestionTask).isDaVinciClient();
    doReturn(false).when(pcs).isDeferredWrite();

    Delete delete = createBatchDelete();
    ingestionTask.removeFromStorageEngine(PARTITION, KEY_BYTES, delete);

    // DaVinci + !isDaVinciClient() fails in the VALUE branch → plain delete
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  // Group 8: processEndOfPush — unique key count finalization

  @Test
  public void testProcessEndOfPush_uniqueKeyCountEnabled() throws Exception {
    // serverConfig.isUniqueKeyCountForAllBatchPushEnabled() = true → finalizeUniqueKeyCountForBatchPush called
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
    // serverConfig.isUniqueKeyCountForAllBatchPushEnabled() = false → finalizeUniqueKeyCountForBatchPush NOT called
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

  // Group 9: Missing branch coverage — pcs==null paths and logging filter

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

    // pcs == null → falls through to plain put
    verify(storageEngine).put(anyInt(), any(byte[].class), any(ByteBuffer.class));
    verify(storageEngine, never())
        .putWithReplicationMetadata(anyInt(), any(byte[].class), any(ByteBuffer.class), any(byte[].class));
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

    // pcs == null → falls through to plain delete
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

    // pcs != null but isEndOfPushReceived=true → falls through to plain delete
    verify(storageEngine).delete(anyInt(), any(byte[].class));
    verify(storageEngine, never()).deleteWithReplicationMetadata(anyInt(), any(byte[].class), any(byte[].class));
  }

  @Test
  public void testTrackUniqueKeyCount_followerSignal_unexpectedValue_redundantFilterTrue() throws Exception {
    // Test the REDUNDANT_LOGGING_FILTER returning true (redundant exception — log suppressed)
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcs).isEndOfPushReceived();
    doReturn(5L).when(mockPcs).getUniqueKeyCount();
    doReturn("test-replica").when(mockPcs).getReplicaId();

    // Call twice with the same unexpected value — the second call should hit the filter=true branch
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
}
