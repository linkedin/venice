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
    // The stale previouslyReadyToServe flag must be cleared on the decline path so that a crash during catch-up
    // cannot trick a later restart into passing the fast path via a small checkpoint delta.
    // Called twice above: Case 1 (default-0 timestamps produce a huge delta) and Case 3 (growth out of bound).
    verify(pcs, times(2)).clearPreviouslyReadyToServeInOffsetRecord();
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
    // The stale previouslyReadyToServe flag must be cleared on the decline path so that a crash during catch-up
    // cannot trick a later restart into passing the fast path via a small checkpoint delta.
    verify(pcs, times(1)).clearPreviouslyReadyToServeInOffsetRecord();
  }

  /**
   * Regression test for the stale previouslyReadyToServe flag bug.
   *
   * Scenario:
   *   Run 1: replica genuinely reaches ready-to-serve; previouslyReadyToServe=true is persisted.
   *   Server is down for a long time (lag exceeds threshold on next restart).
   *   Run 2: restart. Fast RTS lag check declines (lag too high). Replica starts regular catch-up.
   *          syncOffset() refreshes heartbeatTimestamp / offsetLag / lastCheckpointTimestamp to fresh,
   *          still-behind values before the replica ever reaches RTS.
   *          Server crashes mid-catch-up.
   *   Run 3: restart. Without the fix, flag is still true, and the checkpoint-vs-heartbeat delta on disk is
   *          small (because both were written moments apart during Run 2 catch-up), so the fast RTS path
   *          incorrectly marks the replica ready-to-serve while it is still substantially behind.
   *
   * Fix: clear the flag in the decline branch of both lag-check methods so the flag is only ever true when
   * the replica was actually caught up at last shutdown. This test verifies that behavior directly.
   */
  @Test
  public void testStalePreviouslyReadyToServeFlagIsClearedOnDeclineThenCannotPassOnNextRestart() {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(5).when(serverConfig).getTimeLagThresholdForFastOnlineTransitionInRestartMinutes();
    doReturn(serverConfig).when(storeIngestionTask).getServerConfig();
    OffsetRecord record = mock(OffsetRecord.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(record).when(pcs).getOffsetRecord();
    doReturn("test_v1-1").when(pcs).getReplicaId();
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeWithPreviousTimeLag(any());

    long currentTimestamp = System.currentTimeMillis();
    // Run 2 restart: previous heartbeat is 10min behind, checkpoint was 7min ago => growth = 3min > 5min-2min
    // threshold, lag check declines.
    doReturn(currentTimestamp - TimeUnit.MINUTES.toMillis(7)).when(record).getLastCheckpointTimestamp();
    doReturn(currentTimestamp - TimeUnit.MINUTES.toMillis(10)).when(record).getHeartbeatTimestamp();
    doReturn(true).when(pcs).getReadyToServeInOffsetRecord();

    Assert.assertFalse(
        storeIngestionTask.checkFastReadyToServeWithPreviousTimeLag(pcs),
        "Fast RTS should decline when lag growth exceeds threshold");
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
    doCallRealMethod().when(storeIngestionTask).checkFastReadyToServeWithPreviousOffsetLag(any());
    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(100L, -100L, -1L, BufferReplayPolicy.REWIND_FROM_SOP);
    hybridStoreConfig.setOffsetLagThresholdToGoOnline(100);
    doReturn(Optional.of(hybridStoreConfig)).when(storeIngestionTask).getHybridStoreConfig();
    doReturn(1).when(storeIngestionTask).getPartitionCount();

    // Offset lag grew from 100 to 400 => delta 300 > 2 * 100 threshold, decline.
    doReturn(400L).when(storeIngestionTask).measureHybridOffsetLag(pcs, true);
    doReturn(100L).when(record).getOffsetLag();
    doReturn(true).when(pcs).getReadyToServeInOffsetRecord();

    Assert.assertFalse(
        storeIngestionTask.checkFastReadyToServeWithPreviousOffsetLag(pcs),
        "Fast RTS should decline when offset lag growth exceeds threshold");
    verify(pcs, never()).lagHasCaughtUp();
    verify(pcs, times(1)).clearPreviouslyReadyToServeInOffsetRecord();
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
