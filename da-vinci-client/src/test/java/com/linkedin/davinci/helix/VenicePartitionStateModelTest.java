package com.linkedin.davinci.helix;

import com.linkedin.venice.meta.Store;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

/**
 * Unit tests to verify the State model takes the appropriate decisions on transitions.
 */
public class VenicePartitionStateModelTest
    extends AbstractVenicePartitionStateModelTest<VenicePartitionStateModel, OnlineOfflineStateModelNotifier> {

  @Override
  protected VenicePartitionStateModel getParticipantStateModel() {
    return new VenicePartitionStateModel(mockStoreIngestionService, mockStorageService, mockStoreConfig, testPartition,
        mockNotifier, mockReadOnlyStoreRepository, Optional.of(CompletableFuture.completedFuture(mockPushStatusAccessor)),
        instanceName);
  }

  @Override
  protected OnlineOfflineStateModelNotifier getNotifier() {
    return mock(OnlineOfflineStateModelNotifier.class);
  }

  /**
   * Verifies the following:
   *  1. Kafka Partition consumption turned on.
   *  2. Partition is added to local storage engine if required.
   *    2.1 KafkaConsumption offset is reset if new local partition created.
   *  3. Notifier knows the consumption is started.
   */
  @Test
  public void testOnBecomeBootstrapFromOffline() {
    testStateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);
    verify(mockStoreIngestionService, times(1))
        .startConsumption(mockStoreConfig, testPartition);
    verify(mockStorageService, times(1)).openStoreForNewPartition(mockStoreConfig, testPartition);
    verify(mockNotifier, times(1)).startConsumption(mockMessage.getResourceName(), testPartition);
  }

  /**
   * Verify wait on notifier is processed.
   * @throws Exception
   */
  @Test
  public void testOnBecomeOnlineFromBootstrap()
      throws Exception {
    testStateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext);
    verify(mockNotifier, times(1))
        .waitConsumptionCompleted(mockMessage.getResourceName(), testPartition,
            Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS, mockStoreIngestionService);
  }

  /**
   * Test a state model transit from offline to bootstrap then from bootstrap to online.
   */
  @Test
  public void testOfflineToBootstrapToOnline() {
    OnlineOfflineStateModelNotifier notifier = new OnlineOfflineStateModelNotifier();
    testStateModel =
        new VenicePartitionStateModel(mockStoreIngestionService, mockStorageService, mockStoreConfig, testPartition,
            notifier, mockReadOnlyStoreRepository, Optional.empty(), null);
    testStateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);
    CountDownLatch latch = notifier.getLatch(mockMessage.getResourceName(), testPartition);
    Assert.assertEquals(latch.getCount(), 1);
    new Thread(() -> {
      try {
        Thread.sleep(1000l);
        // Notify that consumption is completed.
        notifier.completed(mockMessage.getResourceName(), testPartition, 0);
      } catch (InterruptedException e) {
        Assert.fail(e.getMessage());
      }
    }).run();

    testStateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext);
    latch = notifier.getLatch(mockMessage.getResourceName(), testPartition);
    Assert.assertNull(latch);
  }

  /**
   * Verifies the following:
   *  1. Kafka Partition consumption is turned off.
   */
  @Test
  public void testOnBecomeOfflineFromOnline() {
    testStateModel.onBecomeOfflineFromOnline(mockMessage, mockContext);
    verify(mockStoreIngestionService, atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
  }

  /**
   * Verifies the following:
   *  1. Kafka Partition consumption is turned off.
   */
  @Test
  public void testOnBecomeOfflineFromBootstrap() {
    testStateModel.onBecomeOfflineFromBootstrap(mockMessage, mockContext);
    verify(mockStoreIngestionService, atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
  }

  /**
   * Verifies the following:
   *  1. Information in the local engine is cleared for the dropped partition.
   */
  @Test
  public void testOnBecomeDroppedFromOffline() {
    testStateModel.setupNewStorePartition();
    doAnswer(invocation -> {return null;}).when(mockPushStatusAccessor).deleteReplicaStatus(any(), anyInt());
    testStateModel.onBecomeDroppedFromOffline(mockMessage, mockContext);
    verify(mockStorageService, atLeastOnce()).dropStorePartition(mockStoreConfig , testPartition);
    verify(mockStoreIngestionService, atLeastOnce()).resetConsumptionOffset(mockStoreConfig, testPartition);
    verify(mockPushStatusAccessor, atLeastOnce()).deleteReplicaStatus(any(), anyInt());
  }

  /**
   * Verifies the following:
   *  1. Kafka Consumption is stopped.
   */
  @Test
  public void testOnBecomeOfflineFromError(){
    testStateModel.onBecomeOfflineFromError(mockMessage, mockContext);
    verify(mockStoreIngestionService, atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
  }

  /**
   * Verifies the following:
   *  1. Kafka Consumption is stopped.
   *  2. Information from the local storage engine is deleted for the dropped partition.
   */
  @Test
  public void testOnBecomeDroppedFromError() {
    testStateModel.onBecomeDroppedFromError(mockMessage, mockContext);
    verify(mockStoreIngestionService, atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
    verify(mockStorageService, atLeastOnce()).dropStorePartition(mockStoreConfig, testPartition);
    verify(mockStoreIngestionService, atLeastOnce()).resetConsumptionOffset(mockStoreConfig, testPartition);
  }

  @Test
  public void testRollbackOnError() {
    StateTransitionError mockError = mock(StateTransitionError.class);
    testStateModel.rollbackOnError(mockMessage, mockContext, mockError);
    verify(mockStoreIngestionService, atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
  }
}