package com.linkedin.davinci.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.venice.meta.Store;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;


public class VeniceLeaderFollowerStateModelTest extends
    AbstractVenicePartitionStateModelTest<LeaderFollowerPartitionStateModel, LeaderFollowerIngestionProgressNotifier> {
  @Override
  protected LeaderFollowerPartitionStateModel getParticipantStateModel() {
    return new LeaderFollowerPartitionStateModel(
        mockIngestionBackend,
        mockStoreConfig,
        testPartition,
        mockNotifier,
        mockReadOnlyStoreRepository,
        CompletableFuture.completedFuture(mockPushStatusAccessor),
        null,
        mockParticipantStateTransitionStats);
  }

  @Override
  protected LeaderFollowerIngestionProgressNotifier getNotifier() {
    return mock(LeaderFollowerIngestionProgressNotifier.class);
  }

  @Test
  public void testOnBecomeFollowerFromOffline() throws Exception {
    // if the resource is not the current serving version, latch is not placed.
    when(mockStore.getCurrentVersion()).thenReturn(2);
    testStateModel.onBecomeStandbyFromOffline(mockMessage, mockContext);
    verify(mockNotifier, never()).waitConsumptionCompleted(
        mockMessage.getResourceName(),
        testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS,
        mockStoreIngestionService);

    when(mockStore.getCurrentVersion()).thenReturn(1);
    testStateModel.onBecomeStandbyFromOffline(mockMessage, mockContext);
    verify(mockNotifier).startConsumption(mockMessage.getResourceName(), testPartition);
    verify(mockNotifier).waitConsumptionCompleted(
        mockMessage.getResourceName(),
        testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS,
        mockStoreIngestionService);
  }

  @Test
  public void testGracefulDropForCurrentVersionResource() {
    // if the resource is not the current serving version, state transition thread will not be blocked.
    when(mockStore.getCurrentVersion()).thenReturn(2);
    testStateModel.onBecomeDroppedFromOffline(mockMessage, mockContext);
    verify(mockParticipantStateTransitionStats, never()).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(mockParticipantStateTransitionStats, never()).decrementThreadBlockedOnOfflineToDroppedTransitionCount();

    // if the resource is the current serving version, state transition thread will be blocked.
    when(mockStore.getCurrentVersion()).thenReturn(1);
    testStateModel.onBecomeDroppedFromOffline(mockMessage, mockContext);
    verify(mockParticipantStateTransitionStats, times(1)).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(mockParticipantStateTransitionStats, times(1)).decrementThreadBlockedOnOfflineToDroppedTransitionCount();
  }

  @Test
  public void testRemoveCVStateWhenBecomeOfflineFromStandby() {
    when(mockStore.getCurrentVersion()).thenReturn(2);
    when(mockIngestionBackend.stopConsumption(any(VeniceStoreVersionConfig.class), eq(testPartition)))
        .thenReturn(CompletableFuture.completedFuture(null));

    testStateModel.onBecomeOfflineFromStandby(mockMessage, mockContext);

    verify(mockIngestionBackend).stopConsumption(any(VeniceStoreVersionConfig.class), eq(testPartition));
    verify(mockPushStatusAccessor).deleteReplicaStatus(resourceName, testPartition);
  }
}
