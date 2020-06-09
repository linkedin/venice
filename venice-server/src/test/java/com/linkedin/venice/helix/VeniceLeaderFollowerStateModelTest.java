package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class VeniceLeaderFollowerStateModelTest
    extends AbstractVenicePartitionStateModelTest<LeaderFollowerParticipantModel, LeaderFollowerStateModelNotifier> {
  @Override
  protected LeaderFollowerParticipantModel getParticipantStateModel() {
    return new LeaderFollowerParticipantModel(mockStoreIngestionService, mockStorageService, mockStoreConfig,
        testPartition, mockNotifier, mockReadOnlyStoreRepository);
  }

  @Override
  protected LeaderFollowerStateModelNotifier getNotifier() {
    return mock(LeaderFollowerStateModelNotifier.class);
  }

  @Test
  public void testOnBecomeFollowerFromOffline() throws Exception {
    //if the resource is not the current serving version, latch is not placed.
    when(mockStore.getCurrentVersion()).thenReturn(2);
    testStateModel.onBecomeStandbyFromOffline(mockMessage, mockContext);
    verify(mockNotifier, never()).waitConsumptionCompleted(mockMessage.getResourceName(), testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS, mockAggStoreIngestionStats, mockAggVersionedStorageIngestionStats);

    when(mockStore.getCurrentVersion()).thenReturn(1);
    testStateModel.onBecomeStandbyFromOffline(mockMessage, mockContext);
    verify(mockNotifier, only()).waitConsumptionCompleted(mockMessage.getResourceName(), testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS, mockAggStoreIngestionStats, mockAggVersionedStorageIngestionStats);
  }
}
