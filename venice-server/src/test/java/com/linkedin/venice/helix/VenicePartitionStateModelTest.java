package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedStorageIngestionStats;
import com.linkedin.venice.storage.StorageService;

import java.util.concurrent.CountDownLatch;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests to verify the State model takes the appropriate decisions on transitions.
 */
public class VenicePartitionStateModelTest {

  private StoreIngestionService mockStoreIngestionService;
  private StorageService mockStorageService;
  private VeniceStoreConfig mockStoreConfig;
  private int testPartition = 0;

  private Message mockMessage;
  private NotificationContext mockContext;

  private VenicePartitionStateModel testStateModel;

  private VeniceStateModelFactory.StateModelNotifier mockNotifier;
  private ReadOnlyStoreRepository mockReadOnlyStoreRepository;
  private Store mockStore;
  private AggStoreIngestionStats mockAggStoreIngestionStats;
  private AggVersionedStorageIngestionStats mockAggVersionedStorageIngestionStats;
  private final String resourceName = "test_v1";

  @BeforeMethod
  public void setUp() throws Exception {
    mockStoreIngestionService = Mockito.mock(StoreIngestionService.class);
    mockStorageService = Mockito.mock(StorageService.class);
    mockStoreConfig = Mockito.mock(VeniceStoreConfig.class);

    mockMessage = Mockito.mock(Message.class);
    mockContext = Mockito.mock(NotificationContext.class);

    mockNotifier = Mockito.mock(VeniceStateModelFactory.StateModelNotifier.class);
    mockReadOnlyStoreRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    mockStore = Mockito.mock(Store.class);
    mockAggStoreIngestionStats = Mockito.mock(AggStoreIngestionStats.class);
    mockAggVersionedStorageIngestionStats = Mockito.mock(AggVersionedStorageIngestionStats.class);

    Mockito.when(mockMessage.getResourceName()).thenReturn(resourceName);
    Mockito.when(mockReadOnlyStoreRepository.getStore(Version.parseStoreFromKafkaTopicName(resourceName)))
        .thenReturn(mockStore);
    Mockito.when(mockStore.getBootstrapToOnlineTimeoutInHours()).thenReturn(Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);
    Mockito.when(mockStoreIngestionService.getAggStoreIngestionStats()).thenReturn(mockAggStoreIngestionStats);
    Mockito.when(mockStoreIngestionService.getAggVersionedStorageIngestionStats()).thenReturn(mockAggVersionedStorageIngestionStats);

    testStateModel = new VenicePartitionStateModel(mockStoreIngestionService, mockStorageService, mockStoreConfig,
        testPartition, mockNotifier, mockReadOnlyStoreRepository);
  }

  /**
   * Verifies the following:
   *  1. Kafka Partition consumption turned on.
   *  2. Partition is added to local storage engine if required.
   *    2.1 KafkaConsumption offset is reset if new local partition created.
   *  3. Notifier knows the consumption is started.
   */
  @Test
  public void testOnBecomeBootstrapFromOffline() throws Exception {
    testStateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);
    Mockito.verify(mockStoreIngestionService, Mockito.times(1)).startConsumption(mockStoreConfig, testPartition, false);
    Mockito.verify(mockStorageService, Mockito.times(1)).openStoreForNewPartition(mockStoreConfig, testPartition);
    Mockito.verify(mockNotifier, Mockito.times(1)).startConsumption(mockMessage.getResourceName(), testPartition);
  }

  /**
   * Verify wait on notifier is processed.
   * @throws Exception
   */
  @Test
  public void testOnBecomeOnlineFromBootstrap()
      throws Exception {
    testStateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext);
    Mockito.verify(mockNotifier, Mockito.times(1))
        .waitConsumptionCompleted(mockMessage.getResourceName(), testPartition,
            Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS, mockAggStoreIngestionStats, mockAggVersionedStorageIngestionStats);
  }

  /**
   * Test a state model transit from offline to bootstrap then from bootstrap to online.
   */
  @Test
  public void testOfflineToBootstrapToOnline() {
    VeniceStateModelFactory.StateModelNotifier notifier = new VeniceStateModelFactory.StateModelNotifier();
    testStateModel =
        new VenicePartitionStateModel(mockStoreIngestionService, mockStorageService, mockStoreConfig, testPartition,
            notifier, mockReadOnlyStoreRepository);
    testStateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);
    CountDownLatch latch = notifier.getLatch(mockMessage.getResourceName(), testPartition);
    Assert.assertEquals(latch.getCount(), 1);
    Thread comumptionThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000l);
          // Notify that consumption is completed.
          notifier.completed(mockMessage.getResourceName(), testPartition, 0);
        } catch (InterruptedException e) {
          Assert.fail(e.getMessage());
        }
      }
    });
    comumptionThread.start();
    testStateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext);
    latch = notifier.getLatch(mockMessage.getResourceName(), testPartition);
    Assert.assertNull(latch);
  }

  /**
   * Verifies the following:
   *  1. Kafka Partition consumption is turned off.
   */
  @Test
  public void testOnBecomeOfflineFromOnline() throws Exception {
    testStateModel.onBecomeOfflineFromOnline(mockMessage, mockContext);
    Mockito.verify(mockStoreIngestionService, Mockito.atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
  }

  /**
   * Verifies the following:
   *  1. Kafka Partition consumption is turned off.
   */
  @Test
  public void testOnBecomeOfflineFromBootstrap() throws Exception {
    testStateModel.onBecomeOfflineFromBootstrap(mockMessage, mockContext);
    Mockito.verify(mockStoreIngestionService, Mockito.atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
  }

  /**
   * Verifies the following:
   *  1. Information in the local engine is cleared for the dropped partition.
   */
  @Test
  public void testOnBecomeDroppedFromOffline() throws Exception {
    testStateModel.onBecomeDroppedFromOffline(mockMessage, mockContext);
    Mockito.verify(mockStorageService, Mockito.atLeastOnce()).dropStorePartition(mockStoreConfig , testPartition);
    Mockito.verify(mockStoreIngestionService, Mockito.atLeastOnce()).resetConsumptionOffset(mockStoreConfig, testPartition);
  }

  /**
   * Verifies the following:
   *  1. Kafka Consumption is stopped.
   */
  @Test
  public void testOnBecomeOfflineFromError() throws Exception {
    testStateModel.onBecomeOfflineFromError(mockMessage, mockContext);
    Mockito.verify(mockStoreIngestionService, Mockito.atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
  }

  /**
   * Verifies the following:
   *  1. Kafka Consumption is stopped.
   *  2. Information from the local storage engine is deleted for the dropped partition.
   */
  @Test
  public void testOnBecomeDroppedFromError() throws Exception {
    testStateModel.onBecomeDroppedFromError(mockMessage, mockContext);
    Mockito.verify(mockStoreIngestionService, Mockito.atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
    Mockito.verify(mockStorageService, Mockito.atLeastOnce()).dropStorePartition(mockStoreConfig, testPartition);
    Mockito.verify(mockStoreIngestionService, Mockito.atLeastOnce()).resetConsumptionOffset(mockStoreConfig, testPartition);
  }

  @Test
  public void testRollbackOnError()
      throws Exception {
    StateTransitionError mockError = Mockito.mock(StateTransitionError.class);
    testStateModel.rollbackOnError(mockMessage, mockContext, mockError);
    Mockito.verify(mockStoreIngestionService, Mockito.atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
  }
}