package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests to verify the State model takes the appropriate decisions on transitions.
 */
public class VenicePartitionStateModelTest {

  private KafkaConsumerService mockKafkaConsumerService;
  private StoreRepository mockStoreRepository;
  private VeniceStoreConfig mockStoreConfig;
  private AbstractStorageEngine mockAbstractStorageEngine;
  private int testPartition = 0;

  private Message mockMessage;
  private NotificationContext mockContext;

  private VenicePartitionStateModel testStateModel;

  @BeforeSuite
  public void setUp() throws Exception {
    mockKafkaConsumerService = PowerMockito.mock(KafkaConsumerService.class);
    mockStoreRepository = PowerMockito.mock(StoreRepository.class);
    mockStoreConfig = PowerMockito.mock(VeniceStoreConfig.class);

    mockAbstractStorageEngine = PowerMockito.mock(AbstractStorageEngine.class);
    PowerMockito.when(mockStoreRepository.getLocalStorageEngine(Mockito.anyString()))
        .thenReturn(mockAbstractStorageEngine);
    PowerMockito.when(mockStoreRepository.getOrCreateLocalStorageEngine(Mockito.any(), Mockito.anyInt()))
        .thenReturn(mockAbstractStorageEngine);

    mockMessage = PowerMockito.mock(Message.class);
    mockContext = PowerMockito.mock(NotificationContext.class);

    testStateModel = new VenicePartitionStateModel(mockKafkaConsumerService, mockStoreRepository, mockStoreConfig,
        testPartition);
  }

  /**
   * Verifies the following:
   *  1. Kafka Partition consumption turned on.
   *  2. Partition is added to local storage engine if required.
   *    2.1 KafkaConsumption offset is reset if new local partition created.
   */
  @Test
  public void testOnBecomeOnlineFromOffline() throws Exception {
    // Check that only KafkaConsumption is started when corresponding partition information exists on local storage.
    PowerMockito.when(mockAbstractStorageEngine.containsPartition(testPartition)).thenReturn(true);
    testStateModel.onBecomeOnlineFromOffline(mockMessage, mockContext);
    Mockito.verify(mockKafkaConsumerService, Mockito.atLeastOnce()).startConsumption(mockStoreConfig, testPartition);
    Mockito.verify(mockAbstractStorageEngine, Mockito.never()).addStoragePartition(testPartition);

    /*
     * When partition information does not exists on local storage, Check that KafkaConsumption is started,
     * local partition is created and kafka offset is reset.
     */
    PowerMockito.when(mockAbstractStorageEngine.containsPartition(testPartition)).thenReturn(false);
    testStateModel.onBecomeOnlineFromOffline(mockMessage, mockContext);
    Mockito.verify(mockKafkaConsumerService, Mockito.atLeastOnce()).startConsumption(mockStoreConfig, testPartition);
    Mockito.verify(mockAbstractStorageEngine, Mockito.atLeastOnce()).addStoragePartition(testPartition);
    Mockito.verify(mockKafkaConsumerService, Mockito.atLeastOnce()).resetConsumptionOffset(mockStoreConfig, testPartition);
  }

  /**
   * Verifies the following:
   *  1. Kafka Partition consumption is turned off.
   */
  @Test
  public void testOnBecomeOfflineFromOnline() throws Exception {
    testStateModel.onBecomeOfflineFromOnline(mockMessage, mockContext);
    Mockito.verify(mockKafkaConsumerService, Mockito.atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
  }

  /**
   * Verifies the following:
   *  1. Information in the local engine is cleared for the dropped partition.
   */
  @Test
  public void testOnBecomeDroppedFromOffline() throws Exception {
    testStateModel.onBecomeDroppedFromOffline(mockMessage, mockContext);
    Mockito.verify(mockAbstractStorageEngine, Mockito.atLeastOnce()).removePartition(testPartition);
  }

  /**
   * Verifies the following:
   *  1. Kafka Consumption is stopped.
   */
  @Test
  public void testOnBecomeOfflineFromError() throws Exception {
    testStateModel.onBecomeOfflineFromError(mockMessage, mockContext);
    Mockito.verify(mockKafkaConsumerService, Mockito.atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
  }

  /**
   * Verifies the following:
   *  1. Kafka Consumption is stopped.
   *  2. Information from the local storage engine is deleted for the dropped partition.
   */
  @Test
  public void testOnBecomeDroppedFromError() throws Exception {
    testStateModel.onBecomeDroppedFromError(mockMessage, mockContext);
    Mockito.verify(mockKafkaConsumerService, Mockito.atLeastOnce()).stopConsumption(mockStoreConfig, testPartition);
    Mockito.verify(mockAbstractStorageEngine, Mockito.atLeastOnce()).removePartition(testPartition);
  }
}