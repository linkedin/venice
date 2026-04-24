package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SITFastReadyToServeTest {
  @Test
  public void testReadyToServeSerialization() {
    // Create test data
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartitionImpl topicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test_v1"), 0);
    PubSubContext pubSubContext = new PubSubContext.Builder().setPubSubTopicRepository(pubSubTopicRepository).build();
    OffsetRecord offsetRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer(), pubSubContext);

    // Create PartitionConsumptionState
    PartitionConsumptionState pcs = new PartitionConsumptionState(topicPartition, offsetRecord, pubSubContext, false);

    // Verify initial state
    Assert.assertFalse(pcs.getReadyToServeInOffsetRecord(), "Should not be ready to serve initially");
    // Set ready to serve
    pcs.recordReadyToServeInOffsetRecord();
    // Verify after setting
    Assert.assertTrue(pcs.getReadyToServeInOffsetRecord(), "Should be ready to serve after setting");

    // Serialize and deserialize the offset record
    InternalAvroSpecificSerializer<PartitionState> serializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    byte[] serialized = offsetRecord.toBytes();
    OffsetRecord deserialized = new OffsetRecord(serialized, serializer, null);

    // Create a new PCS with the deserialized offset record
    PartitionConsumptionState newPcs =
        new PartitionConsumptionState(topicPartition, deserialized, pubSubContext, false);

    // Verify after deserialization
    Assert.assertTrue(newPcs.getReadyToServeInOffsetRecord(), "Should still be ready to serve after deserialization");
  }

  @Test
  public void testReadyToServeWithMessageTimeLag() {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(5).when(serverConfig).getTimeLagThresholdForFastOnlineTransitionInRestartMinutes();
    doReturn(serverConfig).when(storeIngestionTask).getServerConfig();
    OffsetRecord record = mock(OffsetRecord.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(record).when(pcs).getOffsetRecord();
    doReturn("test_v1-1").when(pcs).getReplicaId();
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeWithPreviousTimeLag(any());

    // Case 1: Prev HB timestamp is not preserved.
    storeIngestionTask.checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(pcs, never()).lagHasCaughtUp();

    // Case 2: Growth is within bound
    long currentTimestamp = System.currentTimeMillis();
    doReturn(currentTimestamp - TimeUnit.MINUTES.toMillis(1)).when(record).getLastCheckpointTimestamp();
    doReturn(currentTimestamp - TimeUnit.MINUTES.toMillis(10)).when(record).getHeartbeatTimestamp();
    storeIngestionTask.checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(pcs, times(1)).lagHasCaughtUp();

    // Case 3: Growth is out of bound
    doReturn(currentTimestamp - TimeUnit.MINUTES.toMillis(7)).when(record).getLastCheckpointTimestamp();
    storeIngestionTask.checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(pcs, times(1)).lagHasCaughtUp();
    verify(pcs, times(1)).setReadyToServeTimeLagThresholdInMs(eq(TimeUnit.MINUTES.toMillis(8)));
    // Flag clearing is the outer checkFastReadyToServeForReplica's responsibility now; see the
    // testStale* tests that drive behavior via the outer method.
    verify(pcs, never()).clearPreviouslyReadyToServeInOffsetRecord();
  }

  @Test
  public void testReadyToServeWithOffsetLag() {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(2).when(serverConfig).getOffsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart();
    doReturn(serverConfig).when(storeIngestionTask).getServerConfig();
    OffsetRecord record = mock(OffsetRecord.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(record).when(pcs).getOffsetRecord();
    doReturn("test_v1-1").when(pcs).getReplicaId();
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeWithPreviousOffsetLag(any());
    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(100L, -100L, -1L, BufferReplayPolicy.REWIND_FROM_SOP);
    doReturn(Optional.of(hybridStoreConfig)).when(storeIngestionTask).getHybridStoreConfig();
    doReturn(1).when(storeIngestionTask).getPartitionCount();

    // Case 1: Prev HB timestamp is not preserved.
    storeIngestionTask.checkFastReadyToServeWithPreviousOffsetLag(pcs);
    verify(pcs, never()).lagHasCaughtUp();

    // Case 2: Growth is within bound
    hybridStoreConfig.setOffsetLagThresholdToGoOnline(100);
    doReturn(200L).when(storeIngestionTask).measureHybridOffsetLag(pcs, true);
    doReturn(100L).when(record).getOffsetLag();
    storeIngestionTask.checkFastReadyToServeWithPreviousOffsetLag(pcs);
    verify(pcs, times(1)).lagHasCaughtUp();

    // Case 3: Growth is out of bound
    doReturn(400L).when(storeIngestionTask).measureHybridOffsetLag(pcs, true);
    storeIngestionTask.checkFastReadyToServeWithPreviousOffsetLag(pcs);
    verify(pcs, times(1)).lagHasCaughtUp();
    // Flag clearing is the outer checkFastReadyToServeForReplica's responsibility now.
    verify(pcs, never()).clearPreviouslyReadyToServeInOffsetRecord();
  }

  /**
   * The outer {@link StoreIngestionTask#checkFastReadyToServeForReplica} is the single site that clears the stale
   * {@code previouslyReadyToServe} flag on every decline path. The tests below drive the outer method and cover:
   *
   * <ul>
   *   <li>Time-lag inner check declines → flag cleared.</li>
   *   <li>Offset-lag inner check declines → flag cleared.</li>
   *   <li>Outer gate passes (hybrid present, flag=true) but no inner branch matches the current config combo
   *       → flag cleared. This is the config-fall-through case that was uncovered before the refactor.</li>
   *   <li>Fast path succeeds → flag preserved.</li>
   *   <li>Outer gate blocks the fast path (flag already false, or no hybrid config) → no-op; flag untouched.</li>
   * </ul>
   */
  @Test
  public void testStalePreviouslyReadyToServeFlagIsClearedOnTimeLagDecline() {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(5).when(serverConfig).getTimeLagThresholdForFastOnlineTransitionInRestartMinutes();
    doReturn(serverConfig).when(storeIngestionTask).getServerConfig();
    OffsetRecord record = mock(OffsetRecord.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(record).when(pcs).getOffsetRecord();
    doReturn("test_v1-1").when(pcs).getReplicaId();
    doReturn(true).when(pcs).getReadyToServeInOffsetRecord();
    doReturn(Optional.of(mock(HybridStoreConfig.class))).when(storeIngestionTask).getHybridStoreConfig();
    doReturn(true).when(storeIngestionTask).isTimeLagRelaxEnabled();
    doReturn(true).when(serverConfig).isUseHeartbeatLagForReadyToServeCheckEnabled();
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeForReplica(any());
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeWithPreviousTimeLag(any());

    long now = System.currentTimeMillis();
    // Previous heartbeat is 10min behind and the last checkpoint was 7min ago: previous lag is 3min, but
    // the lag growth (currentLag - previousLag) is 7min, which exceeds the 5min threshold, so decline.
    doReturn(now - TimeUnit.MINUTES.toMillis(7)).when(record).getLastCheckpointTimestamp();
    doReturn(now - TimeUnit.MINUTES.toMillis(10)).when(record).getHeartbeatTimestamp();

    Assert.assertFalse(storeIngestionTask.checkFastReadyToServeForReplica(pcs));
    verify(pcs, never()).lagHasCaughtUp();
    verify(pcs, times(1)).clearPreviouslyReadyToServeInOffsetRecord();
  }

  @Test
  public void testStalePreviouslyReadyToServeFlagIsClearedOnOffsetLagDecline() {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(2).when(serverConfig).getOffsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart();
    doReturn(serverConfig).when(storeIngestionTask).getServerConfig();
    OffsetRecord record = mock(OffsetRecord.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(record).when(pcs).getOffsetRecord();
    doReturn("test_v1-1").when(pcs).getReplicaId();
    doReturn(true).when(pcs).getReadyToServeInOffsetRecord();
    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(100L, -100L, -1L, BufferReplayPolicy.REWIND_FROM_SOP);
    hybridStoreConfig.setOffsetLagThresholdToGoOnline(100);
    doReturn(Optional.of(hybridStoreConfig)).when(storeIngestionTask).getHybridStoreConfig();
    doReturn(1).when(storeIngestionTask).getPartitionCount();
    doReturn(false).when(storeIngestionTask).isTimeLagRelaxEnabled();
    doReturn(true).when(storeIngestionTask).isOffsetLagDeltaRelaxEnabled();
    doReturn(false).when(serverConfig).isUseHeartbeatLagForReadyToServeCheckEnabled();
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeForReplica(any());
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeWithPreviousOffsetLag(any());

    // Offset lag grew from 100 to 400 => delta 300 > 2 * 100 threshold, decline.
    doReturn(400L).when(storeIngestionTask).measureHybridOffsetLag(pcs, true);
    doReturn(100L).when(record).getOffsetLag();

    Assert.assertFalse(storeIngestionTask.checkFastReadyToServeForReplica(pcs));
    verify(pcs, never()).lagHasCaughtUp();
    verify(pcs, times(1)).clearPreviouslyReadyToServeInOffsetRecord();
  }

  /**
   * Site A from the review: the outer gate passes (hybrid present, flag=true) but the current config combo
   * matches neither inner branch. The outer method returns false without entering either lag-check method.
   * The flag must still be cleared here because syncOffset() during the ensuing regular catch-up pollutes the
   * on-disk lag fields; if the admin later flips a config that makes a lag-check method reachable, a restart
   * could then pass the delta check with flag=true still set.
   */
  @Test
  public void testStalePreviouslyReadyToServeFlagIsClearedWhenConfigDoesNotMatchAnyInnerBranch() {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(serverConfig).when(storeIngestionTask).getServerConfig();
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn("test_v1-1").when(pcs).getReplicaId();
    doReturn(true).when(pcs).getReadyToServeInOffsetRecord();
    doReturn(Optional.of(mock(HybridStoreConfig.class))).when(storeIngestionTask).getHybridStoreConfig();
    // Config combo that matches neither inner branch: time-lag needs heartbeat=true, offset-lag needs
    // heartbeat=false; flipping just isTimeLagRelaxEnabled without the matching heartbeat setting skips
    // the time-lag branch, and leaving isOffsetLagDeltaRelaxEnabled=false skips the offset-lag branch.
    doReturn(true).when(storeIngestionTask).isTimeLagRelaxEnabled();
    doReturn(false).when(serverConfig).isUseHeartbeatLagForReadyToServeCheckEnabled();
    doReturn(false).when(storeIngestionTask).isOffsetLagDeltaRelaxEnabled();
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeForReplica(any());

    Assert.assertFalse(storeIngestionTask.checkFastReadyToServeForReplica(pcs));
    verify(storeIngestionTask, never()).checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(storeIngestionTask, never()).checkFastReadyToServeWithPreviousOffsetLag(pcs);
    verify(pcs, times(1)).clearPreviouslyReadyToServeInOffsetRecord();
  }

  /**
   * When the fast path succeeds, the flag must stay set — the replica is genuinely ready-to-serve and a clean
   * shutdown should allow the next restart to take the fast path again.
   */
  @Test
  public void testPreviouslyReadyToServeFlagPreservedWhenFastPathSucceeds() {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(5).when(serverConfig).getTimeLagThresholdForFastOnlineTransitionInRestartMinutes();
    doReturn(serverConfig).when(storeIngestionTask).getServerConfig();
    OffsetRecord record = mock(OffsetRecord.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(record).when(pcs).getOffsetRecord();
    doReturn("test_v1-1").when(pcs).getReplicaId();
    doReturn(true).when(pcs).getReadyToServeInOffsetRecord();
    doReturn(Optional.of(mock(HybridStoreConfig.class))).when(storeIngestionTask).getHybridStoreConfig();
    doReturn(true).when(storeIngestionTask).isTimeLagRelaxEnabled();
    doReturn(true).when(serverConfig).isUseHeartbeatLagForReadyToServeCheckEnabled();
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeForReplica(any());
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeWithPreviousTimeLag(any());

    long now = System.currentTimeMillis();
    // Growth is 1min, within the 5min threshold -> fast path passes.
    doReturn(now - TimeUnit.MINUTES.toMillis(1)).when(record).getLastCheckpointTimestamp();
    doReturn(now - TimeUnit.MINUTES.toMillis(10)).when(record).getHeartbeatTimestamp();

    Assert.assertTrue(storeIngestionTask.checkFastReadyToServeForReplica(pcs));
    verify(pcs, times(1)).lagHasCaughtUp();
    verify(pcs, never()).clearPreviouslyReadyToServeInOffsetRecord();
  }

  /**
   * When the outer gate blocks (flag already false, or no hybrid config), the outer method must not touch the
   * flag — clearing a flag that was never set is a harmless no-op but verifying it stays untouched guards
   * against an accidental side-effect if the routing logic is ever rewritten.
   */
  @Test
  public void testPreviouslyReadyToServeFlagNotTouchedWhenOuterGateBlocks() {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn("test_v1-1").when(pcs).getReplicaId();
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeForReplica(any());

    // Case 1: no hybrid config -> outer gate fails.
    doReturn(Optional.empty()).when(storeIngestionTask).getHybridStoreConfig();
    doReturn(true).when(pcs).getReadyToServeInOffsetRecord();
    Assert.assertFalse(storeIngestionTask.checkFastReadyToServeForReplica(pcs));

    // Case 2: flag already false -> outer gate fails.
    doReturn(Optional.of(mock(HybridStoreConfig.class))).when(storeIngestionTask).getHybridStoreConfig();
    doReturn(false).when(pcs).getReadyToServeInOffsetRecord();
    Assert.assertFalse(storeIngestionTask.checkFastReadyToServeForReplica(pcs));

    verify(pcs, never()).clearPreviouslyReadyToServeInOffsetRecord();
  }

  @Test
  public void testReadyToServeConfigBranch() {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(2).when(serverConfig).getOffsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart();
    doReturn(serverConfig).when(storeIngestionTask).getServerConfig();
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn("test_v1-1").when(pcs).getReplicaId();
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeForReplica(any());

    // Case 1: Hybrid config invalid
    doReturn(Optional.empty()).when(storeIngestionTask).getHybridStoreConfig();
    storeIngestionTask.checkFastReadyToServeForReplica(pcs);
    verify(storeIngestionTask, times(0)).checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(storeIngestionTask, times(0)).checkFastReadyToServeWithPreviousOffsetLag(pcs);

    // Case 2: Previous not ready
    doReturn(Optional.of(mock(HybridStoreConfig.class))).when(storeIngestionTask).getHybridStoreConfig();
    doReturn(false).when(pcs).getReadyToServeInOffsetRecord();
    storeIngestionTask.checkFastReadyToServeForReplica(pcs);
    verify(storeIngestionTask, times(0)).checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(storeIngestionTask, times(0)).checkFastReadyToServeWithPreviousOffsetLag(pcs);

    // Case 3: No fast-restart config enabled
    doReturn(true).when(pcs).getReadyToServeInOffsetRecord();
    doReturn(false).when(storeIngestionTask).isTimeLagRelaxEnabled();
    doReturn(false).when(serverConfig).isUseHeartbeatLagForReadyToServeCheckEnabled();
    doReturn(false).when(storeIngestionTask).isOffsetLagDeltaRelaxEnabled();
    storeIngestionTask.checkFastReadyToServeForReplica(pcs);
    verify(storeIngestionTask, times(0)).checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(storeIngestionTask, times(0)).checkFastReadyToServeWithPreviousOffsetLag(pcs);

    // Case 4: Lag fast-start config enabled
    doReturn(true).when(storeIngestionTask).isTimeLagRelaxEnabled();
    doReturn(false).when(serverConfig).isUseHeartbeatLagForReadyToServeCheckEnabled();
    doReturn(true).when(storeIngestionTask).isOffsetLagDeltaRelaxEnabled();
    storeIngestionTask.checkFastReadyToServeForReplica(pcs);
    verify(storeIngestionTask, times(0)).checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(storeIngestionTask, times(1)).checkFastReadyToServeWithPreviousOffsetLag(pcs);

    // Case 5: Lag fast-start config enabled (cont.)
    doReturn(false).when(storeIngestionTask).isTimeLagRelaxEnabled();
    doReturn(true).when(serverConfig).isUseHeartbeatLagForReadyToServeCheckEnabled();
    storeIngestionTask.checkFastReadyToServeForReplica(pcs);
    verify(storeIngestionTask, times(0)).checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(storeIngestionTask, times(1)).checkFastReadyToServeWithPreviousOffsetLag(pcs);

    // Case 5: HB fast-start config enabled
    doReturn(true).when(storeIngestionTask).isTimeLagRelaxEnabled();
    doReturn(true).when(serverConfig).isUseHeartbeatLagForReadyToServeCheckEnabled();
    storeIngestionTask.checkFastReadyToServeForReplica(pcs);
    verify(storeIngestionTask, times(1)).checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(storeIngestionTask, times(1)).checkFastReadyToServeWithPreviousOffsetLag(pcs);
  }
}
