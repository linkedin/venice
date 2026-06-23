package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatKey;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.stats.dimensions.VeniceChunkingStatus;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.dimensions.VeniceStoreWriteType;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.WriterChunkingHelper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PartitionConsumptionStateTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final PubSubTopicPartition TOPIC_PARTITION =
      new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("topic1_v1"), 0);
  private PubSubContext pubSubContext;

  @BeforeMethod
  public void setUp() {
    pubSubContext = DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
  }

  @Test
  public void testUpdateChecksum() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);
    pcs.initializeExpectedChecksum();
    byte[] rmdPayload = new byte[] { 127 };
    byte[] key1 = new byte[] { 1 };
    byte[] key2 = new byte[] { 2 };
    byte[] key3 = new byte[] { 3 };
    byte[] key4 = new byte[] { 4 };
    byte[] valuePayload1 = new byte[] { 10 };
    byte[] valuePayload3 = new byte[] { 11 };
    byte[] valuePayload4 = new byte[] { 12 };

    Put put = new Put();
    // Try to update a value chunk (should only update value payload)
    put.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    put.putValue = ByteBuffer.wrap(valuePayload1);
    put.replicationMetadataPayload = WriterChunkingHelper.EMPTY_BYTE_BUFFER;
    pcs.maybeUpdateExpectedChecksum(key1, put);
    // Try to update a RMD chunk (should not be updated)
    put.putValue = WriterChunkingHelper.EMPTY_BYTE_BUFFER;
    put.replicationMetadataPayload = ByteBuffer.wrap(rmdPayload);
    pcs.maybeUpdateExpectedChecksum(key2, put);
    // Try to update a regular value with RMD (should only update value payload)
    put.schemaId = 1;
    put.putValue = ByteBuffer.wrap(valuePayload3);
    put.replicationMetadataPayload = ByteBuffer.wrap(rmdPayload);
    pcs.maybeUpdateExpectedChecksum(key3, put);
    // Try to update a manifest (should only update value payload)
    put.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    put.putValue = ByteBuffer.wrap(valuePayload4);
    put.replicationMetadataPayload = ByteBuffer.wrap(rmdPayload);
    pcs.maybeUpdateExpectedChecksum(key4, put);

    byte[] checksum = pcs.getExpectedChecksum();
    Assert.assertNotNull(checksum);

    // Calculate expected checksum.
    CheckSum expectedChecksum = CheckSum.getInstance(CheckSumType.MD5);
    expectedChecksum.update(key1);
    expectedChecksum.update(AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion());
    expectedChecksum.update(valuePayload1, 0, valuePayload1.length);
    expectedChecksum.update(key3);
    expectedChecksum.update(1);
    expectedChecksum.update(valuePayload3, 0, valuePayload3.length);
    expectedChecksum.update(key4);
    expectedChecksum.update(AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
    expectedChecksum.update(valuePayload4, 0, valuePayload4.length);

    Assert.assertEquals(expectedChecksum.getCheckSum(), checksum);
  }

  /**
   * Test the different transientRecordMap operations.
   */
  @Test
  public void testTransientRecordMap() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);
    assertEquals(pcs.getPubSubContext(), pubSubContext);
    PubSubPosition consumedPosition1Mock = mock(PubSubPosition.class);
    PubSubPosition consumedPosition2Mock = mock(PubSubPosition.class);
    PubSubPosition consumedPosition3Mock = mock(PubSubPosition.class);

    byte[] key1 = new byte[] { 65, 66, 67, 68 };
    byte[] key2 = new byte[] { 65, 66, 67, 68 };
    byte[] key3 = new byte[] { 65, 66, 67, 69 };
    byte[] value1 = new byte[] { 97, 98, 99 };
    byte[] value2 = new byte[] { 97, 98, 99, 100 };

    String schema = "\"string\"";
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(schema, 1);
    GenericRecord record = new GenericData.Record(aaSchema);
    // Test removal succeeds if the key is specified with same kafkaConsumedOffset
    pcs.setTransientRecord(-1, consumedPosition1Mock, key1, 5, record);
    PartitionConsumptionState.TransientRecord tr1 = pcs.getTransientRecord(key2);
    Assert.assertEquals(tr1.getValue(), null);
    Assert.assertEquals(tr1.getValueLen(), -1);
    Assert.assertEquals(tr1.getValueOffset(), -1);
    Assert.assertEquals(tr1.getValueSchemaId(), 5);
    // Assert.assertEquals(tr1.getReplicationMetadata(), replicationMetadataKey1_1);

    Assert.assertEquals(pcs.getTransientRecordMapSize(), 1);
    PartitionConsumptionState.TransientRecord tr2 = pcs.mayRemoveTransientRecord(-1, consumedPosition1Mock, key1);
    Assert.assertNull(tr2);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 0);

    // Test removal fails if the key is specified with same kafkaConsumedOffset
    pcs.setTransientRecord(-1, consumedPosition1Mock, key1, value1, 100, value1.length, 5, null);
    pcs.setTransientRecord(-1, consumedPosition2Mock, key3, 5, null);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 2);
    pcs.setTransientRecord(-1, consumedPosition3Mock, key1, value2, 100, value2.length, 5, null);

    tr2 = pcs.mayRemoveTransientRecord(-1, consumedPosition1Mock, key1);
    Assert.assertNotNull(tr2);
    Assert.assertEquals(tr2.getValue(), value2);
    Assert.assertEquals(tr2.getValueLen(), value2.length);
    Assert.assertEquals(tr2.getValueOffset(), 100);
    Assert.assertEquals(tr2.getValueSchemaId(), 5);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 2);

    tr2 = pcs.mayRemoveTransientRecord(-1, consumedPosition3Mock, key1);
    Assert.assertNull(tr2);
    Assert.assertEquals(pcs.getTransientRecordMapSize(), 1);
  }

  @Test
  public void testIsLeaderCompleted() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);
    // default is LEADER_NOT_COMPLETED
    assertEquals(pcs.getLeaderCompleteState(), LeaderCompleteState.LEADER_NOT_COMPLETED);
    assertFalse(pcs.isLeaderCompleted());

    // test with LEADER_COMPLETED
    pcs.setLeaderCompleteState(LeaderCompleteState.LEADER_COMPLETED);
    assertTrue(pcs.isLeaderCompleted());
  }

  @Test
  public void testAddIncPushVersionToPendingReportList() {
    List<String> pendingReportIncrementalPush = new ArrayList<>();
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(pendingReportIncrementalPush).when(offsetRecord).getPendingReportIncPushVersionList();
    PartitionConsumptionState pcs =
        new PartitionConsumptionState(TOPIC_PARTITION, offsetRecord, pubSubContext, false, false, false, null);
    pcs.addIncPushVersionToPendingReportList("a");
    Assert.assertEquals(pcs.getPendingReportIncPushVersionList().size(), 1);
    for (int i = 0; i < 50; i++) {
      pcs.addIncPushVersionToPendingReportList("v_" + i);
    }
    Assert.assertEquals(pcs.getPendingReportIncPushVersionList().size(), 50);
    Assert.assertEquals(pcs.getPendingReportIncPushVersionList().get(0), "v_0");
  }

  @Test
  public void testDolStateOperations() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);

    // Initially, DoL state should be null
    assertNull(pcs.getDolState());

    // Test setDolState
    DolStamp dolStamp = new DolStamp(42L, "test-host-123");
    pcs.setDolState(dolStamp);
    assertNotNull(pcs.getDolState());
    assertEquals(pcs.getDolState(), dolStamp);
    assertEquals(pcs.getDolState().getLeadershipTerm(), 42L);
    assertEquals(pcs.getDolState().getHostId(), "test-host-123");

    // Test clearDolState
    pcs.clearDolState();
    assertNull(pcs.getDolState());
  }

  @Test
  public void testHighestLeadershipTermOperations() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);

    // Initially, highest leadership term should be -1 (default uninitialized value)
    assertEquals(pcs.getHighestLeadershipTerm(), -1L);

    // Test setHighestLeadershipTerm
    pcs.setHighestLeadershipTerm(100L);
    assertEquals(pcs.getHighestLeadershipTerm(), 100L);

    // Test updating to a higher term
    pcs.setHighestLeadershipTerm(200L);
    assertEquals(pcs.getHighestLeadershipTerm(), 200L);

    // Test setting to a lower term (should be allowed)
    pcs.setHighestLeadershipTerm(50L);
    assertEquals(pcs.getHighestLeadershipTerm(), 50L);
  }

  @Test
  public void testDolStateWithMultipleUpdates() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);

    // Set first DolStamp
    DolStamp dolStamp1 = new DolStamp(1L, "host-1");
    pcs.setDolState(dolStamp1);
    assertEquals(pcs.getDolState().getLeadershipTerm(), 1L);

    // Replace with second DolStamp
    DolStamp dolStamp2 = new DolStamp(2L, "host-2");
    pcs.setDolState(dolStamp2);
    assertEquals(pcs.getDolState().getLeadershipTerm(), 2L);
    assertEquals(pcs.getDolState().getHostId(), "host-2");

    // Clear and verify
    pcs.clearDolState();
    assertNull(pcs.getDolState());
  }

  @Test
  public void testHllTrackingBasic() {
    PartitionConsumptionState pcs = createPcsWithHll();

    pcs.trackKeyIngested("key1".getBytes());
    pcs.trackKeyIngested("key2".getBytes());
    pcs.trackKeyIngested("key3".getBytes());

    assertEquals(pcs.getEstimatedUniqueIngestedKeyCount(), 3);
    assertTrue(pcs.hasUniqueIngestedKeyCountHll());
  }

  @Test
  public void testHllDeduplication() {
    PartitionConsumptionState pcs = createPcsWithHll();

    int expectedDuplicates = 10;
    for (int i = 0; i < 100; i++) {
      String key = "key" + i % expectedDuplicates;
      pcs.trackKeyIngested(key.getBytes());
    }

    assertEquals(pcs.getEstimatedUniqueIngestedKeyCount(), expectedDuplicates);
  }

  @Test
  public void testHllInitBranchingLogic() {
    // Case 1: New subscription — createFresh, HLL is initialized
    PartitionConsumptionState freshPcs = createPcsWithHll();
    assertTrue(freshPcs.hasUniqueIngestedKeyCountHll());
    assertEquals(freshPcs.getEstimatedUniqueIngestedKeyCount(), 0);

    // Case 2: Restore from checkpoint — restoreUniqueKeyCountHll, HLL matches original
    freshPcs.trackKeyIngested("key1".getBytes());
    freshPcs.trackKeyIngested("key2".getBytes());
    byte[] serialized = freshPcs.serializeUniqueIngestedKeyCountHll();

    OffsetRecord restoredRecord = mock(OffsetRecord.class);
    doReturn(ByteBuffer.wrap(serialized)).when(restoredRecord).getUniqueIngestedKeyCountHllSketch();
    doReturn(null).when(restoredRecord).getLeaderTopic();
    PartitionConsumptionState restoredPcs =
        new PartitionConsumptionState(TOPIC_PARTITION, restoredRecord, pubSubContext, false, false, false, null);
    restoredPcs.restoreUniqueKeyCountHll();
    assertTrue(restoredPcs.hasUniqueIngestedKeyCountHll());
    assertEquals(restoredPcs.getEstimatedUniqueIngestedKeyCount(), 2);

    // Case 3: Pre-deployment version — don't call any init, HLL stays null
    PartitionConsumptionState preDeployPcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);
    preDeployPcs.trackKeyIngested("key1".getBytes());
    assertFalse(preDeployPcs.hasUniqueIngestedKeyCountHll());
    assertEquals(preDeployPcs.getEstimatedUniqueIngestedKeyCount(), 0);
    assertNull(preDeployPcs.serializeUniqueIngestedKeyCountHll());
  }

  @Test
  public void testHllAccuracyAtScale() {
    PartitionConsumptionState pcs = createPcsWithHll();

    int uniqueKeys = 1_000_000;
    for (int i = 0; i < uniqueKeys; i++) {
      pcs.trackKeyIngested(("key_" + i).getBytes());
    }

    long estimate = pcs.getEstimatedUniqueIngestedKeyCount();
    double errorRate = Math.abs(estimate - uniqueKeys) / (double) uniqueKeys;

    // At lgK=13, error should be < 2% (well within 1.15% * 3 sigma = ~3.45%)
    assertTrue(errorRate < 0.02, "Error rate " + errorRate + " exceeds 2%");
  }

  @Test
  public void testHllConfigurableLgK() {
    // Test with lgK=10 (smaller sketch, less accurate)
    PartitionConsumptionState pcs = createPcsWithHll(10);

    for (int i = 0; i < 10000; i++) {
      pcs.trackKeyIngested(("key_" + i).getBytes());
    }

    long estimate = pcs.getEstimatedUniqueIngestedKeyCount();
    double errorRate = Math.abs(estimate - 10000) / 10000.0;
    // lgK=10 has ~3.25% error, allow up to 5%
    assertTrue(errorRate < 0.05, "Error rate " + errorRate + " exceeds 5% for lgK=10");

    // Serialized size should be smaller than lgK=13
    byte[] serialized = pcs.serializeUniqueIngestedKeyCountHll();
    assertTrue(serialized.length < 3000); // ~1KB for lgK=10
  }

  @Test
  public void testHllAvroFieldNullByDefault() {
    // A fresh OffsetRecord (no HLL set) should return null
    OffsetRecord offsetRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer(), pubSubContext);
    assertNull(offsetRecord.getUniqueIngestedKeyCountHllSketch());

    // Round-trip through Avro should preserve null
    byte[] avroBytes = offsetRecord.toBytes();
    OffsetRecord restored =
        new OffsetRecord(avroBytes, AvroProtocolDefinition.PARTITION_STATE.getSerializer(), pubSubContext);
    assertNull(restored.getUniqueIngestedKeyCountHllSketch());
  }

  @Test
  public void testLeaderFollowerStateFilter() {
    // Create three PCS objects simulating partitions with different roles
    PartitionConsumptionState leaderPcs = createPcsWithHll();
    leaderPcs.setLeaderFollowerState(LeaderFollowerStateType.LEADER);
    for (int i = 0; i < 100; i++) {
      leaderPcs.trackKeyIngested(("leader-key-" + i).getBytes());
    }

    PartitionConsumptionState followerPcs1 = createPcsWithHll();
    followerPcs1.setLeaderFollowerState(LeaderFollowerStateType.STANDBY);
    for (int i = 0; i < 50; i++) {
      followerPcs1.trackKeyIngested(("follower1-key-" + i).getBytes());
    }

    PartitionConsumptionState followerPcs2 = createPcsWithHll();
    followerPcs2.setLeaderFollowerState(LeaderFollowerStateType.STANDBY);
    for (int i = 0; i < 30; i++) {
      followerPcs2.trackKeyIngested(("follower2-key-" + i).getBytes());
    }

    // Simulate the SIT filtering logic: null = all, LEADER = leader only, STANDBY = followers only
    List<PartitionConsumptionState> allPcs = Arrays.asList(leaderPcs, followerPcs1, followerPcs2);

    // null filter: sum all
    long allTotal = allPcs.stream().mapToLong(PartitionConsumptionState::getEstimatedUniqueIngestedKeyCount).sum();
    assertEquals(allTotal, 180L);

    // LEADER filter: only leader partitions
    long leaderTotal = allPcs.stream()
        .filter(pcs -> pcs.getLeaderFollowerState() == LeaderFollowerStateType.LEADER)
        .mapToLong(PartitionConsumptionState::getEstimatedUniqueIngestedKeyCount)
        .sum();
    assertEquals(leaderTotal, 100L);

    // STANDBY filter: only follower partitions
    long followerTotal = allPcs.stream()
        .filter(pcs -> pcs.getLeaderFollowerState() == LeaderFollowerStateType.STANDBY)
        .mapToLong(PartitionConsumptionState::getEstimatedUniqueIngestedKeyCount)
        .sum();
    assertEquals(followerTotal, 80L);
  }

  /**
   * Simulates the syncOffset() HLL persistence path:
   * 1. Track keys into a PCS with HLL enabled
   * 2. Serialize HLL and set on a real OffsetRecord (mirrors syncOffset logic)
   * 3. Serialize OffsetRecord to Avro bytes
   * 4. Deserialize into a new OffsetRecord
   * 5. Create a new PCS from the restored OffsetRecord
   * 6. Verify the HLL estimate matches
   *
   * Also verifies that when HLL is disabled, no bytes are written to the OffsetRecord.
   */
  @Test
  public void testSyncOffsetHllPersistencePath() {
    // --- HLL enabled path ---
    PartitionConsumptionState pcs = createPcsWithHll();
    for (int i = 0; i < 5000; i++) {
      pcs.trackKeyIngested(("key-" + i).getBytes());
    }
    long originalEstimate = pcs.getEstimatedUniqueIngestedKeyCount();
    assertTrue(originalEstimate > 0);

    // Simulate syncOffset: serialize HLL and set on OffsetRecord
    OffsetRecord offsetRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer(), pubSubContext);
    assertTrue(pcs.hasUniqueIngestedKeyCountHll());
    byte[] hllBytes = pcs.serializeUniqueIngestedKeyCountHll();
    assertNotNull(hllBytes);
    offsetRecord.setUniqueIngestedKeyCountHllSketch(ByteBuffer.wrap(hllBytes));

    // Serialize OffsetRecord to Avro bytes and restore
    byte[] avroBytes = offsetRecord.toBytes();
    OffsetRecord restored =
        new OffsetRecord(avroBytes, AvroProtocolDefinition.PARTITION_STATE.getSerializer(), pubSubContext);
    assertNotNull(restored.getUniqueIngestedKeyCountHllSketch());

    // Create new PCS from restored OffsetRecord and verify estimate
    OffsetRecord restoredForPcs = mock(OffsetRecord.class);
    doReturn(restored.getUniqueIngestedKeyCountHllSketch()).when(restoredForPcs).getUniqueIngestedKeyCountHllSketch();
    doReturn(null).when(restoredForPcs).getLeaderTopic();
    PartitionConsumptionState restoredPcs =
        new PartitionConsumptionState(TOPIC_PARTITION, restoredForPcs, pubSubContext, false, false, false, null);
    restoredPcs.restoreUniqueKeyCountHll();
    assertEquals(restoredPcs.getEstimatedUniqueIngestedKeyCount(), originalEstimate);

    // --- HLL disabled path: no bytes should be set ---
    PartitionConsumptionState disabledPcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);
    // Don't init HLL
    assertFalse(disabledPcs.hasUniqueIngestedKeyCountHll());
    assertNull(disabledPcs.serializeUniqueIngestedKeyCountHll());
  }

  /**
   * getLeaderPosition VT branch:
   *   - consumeRemotely=true + useCheckpointedDivRtPosition=true + LCVP non-EARLIEST → return LCVP
   *   - consumeRemotely=true + useCheckpointedDivRtPosition=true + LCVP=EARLIEST       → fall back to latestProcessedRemoteVtPosition
   *   - consumeRemotely=true + useCheckpointedDivRtPosition=false                      → return latestProcessedRemoteVtPosition
   *   - consumeRemotely=false                                                           → return latestProcessedVtPosition
   */
  @Test
  public void testGetLeaderPositionVtBranch() {
    PubSubPosition lcvp = mock(PubSubPosition.class);
    PubSubPosition remoteVtPos = mock(PubSubPosition.class);
    PubSubPosition localVtPos = mock(PubSubPosition.class);

    // Use a mock OffsetRecord to avoid InMemoryPubSubPosition wire-format serialization issues
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(TOPIC_REPOSITORY.getTopic("topic1_v1")).when(offsetRecord)
        .getLeaderTopic(pubSubContext.getPubSubTopicRepository());
    doReturn(lcvp).when(offsetRecord).getLatestConsumedVtPosition();

    PartitionConsumptionState pcs =
        new PartitionConsumptionState(TOPIC_PARTITION, offsetRecord, pubSubContext, false, false, false, null);
    pcs.setLatestProcessedRemoteVtPosition(remoteVtPos);
    pcs.setLatestProcessedVtPosition(localVtPos);

    // consumeRemotely=true + Global RT DIV on (useCheckpointedDivRtPosition=true) + LCVP non-EARLIEST → LCVP
    pcs.setConsumeRemotely(true);
    assertEquals(
        pcs.getLeaderPosition("broker", true),
        lcvp,
        "Global RT DIV + consumeRemotely + non-EARLIEST LCVP must return LCVP");

    // consumeRemotely=true + Global RT DIV on + LCVP=EARLIEST → latestProcessedRemoteVtPosition
    doReturn(PubSubSymbolicPosition.EARLIEST).when(offsetRecord).getLatestConsumedVtPosition();
    assertEquals(
        pcs.getLeaderPosition("broker", true),
        remoteVtPos,
        "Global RT DIV + consumeRemotely + EARLIEST LCVP must fall back to latestProcessedRemoteVtPosition");

    // consumeRemotely=true + Global RT DIV off (useCheckpointedDivRtPosition=false) → latestProcessedRemoteVtPosition
    doReturn(lcvp).when(offsetRecord).getLatestConsumedVtPosition(); // restore non-EARLIEST LCVP
    assertEquals(
        pcs.getLeaderPosition("broker", false),
        remoteVtPos,
        "Global RT DIV off must use latestProcessedRemoteVtPosition regardless of LCVP");

    // consumeRemotely=false → latestProcessedVtPosition
    pcs.setConsumeRemotely(false);
    assertEquals(
        pcs.getLeaderPosition("broker", true),
        localVtPos,
        "consumeRemotely=false must use latestProcessedVtPosition");
  }

  private PartitionConsumptionState createPcsWithHll() {
    return createPcsWithHll(PartitionConsumptionState.HLL_DEFAULT_LOG_K);
  }

  private PartitionConsumptionState createPcsWithHll(int lgK) {
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(null).when(offsetRecord).getUniqueIngestedKeyCountHllSketch();
    doReturn(null).when(offsetRecord).getLeaderTopic();
    PartitionConsumptionState pcs =
        new PartitionConsumptionState(TOPIC_PARTITION, offsetRecord, pubSubContext, false, false, false, null);
    pcs.initializeUniqueKeyCountHll(lgK);
    return pcs;
  }

  @Test
  public void testStoreLevelPausedDefaultsToFalse() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);
    assertFalse(pcs.isStoreLevelPaused());
  }

  @Test
  public void testSetAndGetStoreLevelPaused() {
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);
    pcs.setStoreLevelPaused(true);
    assertTrue(pcs.isStoreLevelPaused());
    pcs.setStoreLevelPaused(false);
    assertFalse(pcs.isStoreLevelPaused());
  }

  @Test
  public void testGetOrCreateCachedHeartbeatKeyCarriesResolvedSloLabels() {
    PubSubContext pubSubContext = DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        true,
        true,
        "us-west");

    HeartbeatKey local = pcs.getOrCreateCachedHeartbeatKey("us-west");
    assertEquals(local.getWriteType(), VeniceStoreWriteType.WRITE_COMPUTE);
    assertEquals(local.getChunkingStatus(), VeniceChunkingStatus.CHUNKED);
    assertEquals(local.getLocality(), VeniceRegionLocality.LOCAL);

    HeartbeatKey remote = pcs.getOrCreateCachedHeartbeatKey("us-east");
    assertEquals(remote.getWriteType(), VeniceStoreWriteType.WRITE_COMPUTE);
    assertEquals(remote.getChunkingStatus(), VeniceChunkingStatus.CHUNKED);
    assertEquals(remote.getLocality(), VeniceRegionLocality.REMOTE);

    // Subsequent call for the same region returns the cached instance.
    assertTrue(
        local == pcs.getOrCreateCachedHeartbeatKey("us-west"),
        "Same-region calls must return the cached HeartbeatKey instance");
  }

  @Test
  public void testGetOrCreateCachedHeartbeatKeyLeavesLocalityNullWhenLocalRegionUnset() {
    PubSubContext pubSubContext = DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
    // Empty local region — observed in unconfigured deployments — must NOT default to REMOTE.
    PartitionConsumptionState empty = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        "");
    HeartbeatKey k1 = empty.getOrCreateCachedHeartbeatKey("us-west");
    assertNull(k1.getLocality(), "Empty localRegionName must leave locality null, not REMOTE");

    PartitionConsumptionState nullRegion = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        null);
    HeartbeatKey k2 = nullRegion.getOrCreateCachedHeartbeatKey("us-west");
    assertNull(k2.getLocality(), "Null localRegionName must leave locality null");
  }

  @Test
  public void testGetOrCreateCachedHeartbeatKeyResolvesRegularUnchunked() {
    PubSubContext pubSubContext = DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
    PartitionConsumptionState pcs = new PartitionConsumptionState(
        TOPIC_PARTITION,
        mock(OffsetRecord.class),
        pubSubContext,
        false,
        false,
        false,
        "us-west");
    HeartbeatKey k = pcs.getOrCreateCachedHeartbeatKey("us-west");
    assertEquals(k.getWriteType(), VeniceStoreWriteType.REGULAR);
    assertEquals(k.getChunkingStatus(), VeniceChunkingStatus.UNCHUNKED);
    assertEquals(k.getLocality(), VeniceRegionLocality.LOCAL);
  }

  @DataProvider(name = "batchPushRecordCountCases")
  public static Object[][] batchPushRecordCountCases() {
    // { description, priorCountInOffsetRecord, incrementsAfterConstruction, expectedFinalCount }
    return new Object[][] { { "default count is 0", 0L, 0, 0L }, { "increments are monotonic", 0L, 10, 10L },
        { "count restored from OffsetRecord on construction (restart safety)", 57L, 0, 57L },
        { "restored count continues to accumulate further increments", 57L, 5, 62L } };
  }

  @Test(dataProvider = "batchPushRecordCountCases")
  public void testBatchPushRecordCountLifecycle(
      String description,
      long priorCountInOffsetRecord,
      int incrementsAfterConstruction,
      long expectedFinalCount) {
    OffsetRecord offsetRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer(), pubSubContext);
    if (priorCountInOffsetRecord != 0L) {
      offsetRecord.setBatchPushRecordCount(priorCountInOffsetRecord);
    }
    PartitionConsumptionState pcs =
        new PartitionConsumptionState(TOPIC_PARTITION, offsetRecord, pubSubContext, false, false, false, null);
    for (int i = 0; i < incrementsAfterConstruction; i++) {
      pcs.incrementBatchPushRecordCount();
    }
    assertEquals(pcs.getBatchPushRecordCount(), expectedFinalCount, description);
  }
}
