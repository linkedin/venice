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
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.offsets.OffsetRecord;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class SITFastReadyToServeTest {
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
    verify(storeIngestionTask, times(2)).checkFastReadyToServeWithPreviousOffsetLag(pcs);

    // Case 5: HB fast-start config enabled
    doReturn(true).when(storeIngestionTask).isTimeLagRelaxEnabled();
    doReturn(true).when(serverConfig).isUseHeartbeatLagForReadyToServeCheckEnabled();
    storeIngestionTask.checkFastReadyToServeForReplica(pcs);
    verify(storeIngestionTask, times(1)).checkFastReadyToServeWithPreviousTimeLag(pcs);
    verify(storeIngestionTask, times(2)).checkFastReadyToServeWithPreviousOffsetLag(pcs);
  }
}
