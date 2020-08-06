package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;
import java.util.Optional;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class VeniceLeaderFollowerStateModelTest
    extends AbstractVenicePartitionStateModelTest<LeaderFollowerParticipantModel, LeaderFollowerStateModelNotifier> {
  @Override
  protected LeaderFollowerParticipantModel getParticipantStateModel() {
    return new LeaderFollowerParticipantModel(mockStoreIngestionService, mockStorageService, mockStoreConfig,
        testPartition, mockNotifier, mockReadOnlyStoreRepository, Optional.empty(), null);
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
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS, mockStoreIngestionService);

    when(mockStore.getCurrentVersion()).thenReturn(1);
    testStateModel.onBecomeStandbyFromOffline(mockMessage, mockContext);
    verify(mockNotifier).startConsumption(mockMessage.getResourceName(), testPartition);
    verify(mockNotifier).waitConsumptionCompleted(mockMessage.getResourceName(), testPartition,
        Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS, mockStoreIngestionService);
  }
}
