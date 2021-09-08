package com.linkedin.davinci.helix;

import com.linkedin.venice.meta.Store;
import java.util.Optional;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class VeniceLeaderFollowerStateModelTest
    extends AbstractVenicePartitionStateModelTest<LeaderFollowerPartitionStateModel, LeaderFollowerIngestionProgressNotifier> {
  @Override
  protected LeaderFollowerPartitionStateModel getParticipantStateModel() {
    return new LeaderFollowerPartitionStateModel(mockIngestionBackend, mockStoreConfig,
        testPartition, mockNotifier, mockReadOnlyStoreRepository, Optional.empty(), null);
  }

  @Override
  protected LeaderFollowerIngestionProgressNotifier getNotifier() {
    return mock(LeaderFollowerIngestionProgressNotifier.class);
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
