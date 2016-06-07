package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.mockito.Mockito;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests to verify the State model takes the appropriate decisions on transitions.
 */
public class VenicePartitionStateModelTest {

  private KafkaConsumerService mockKafkaConsumerService;
  private StorageService mockStorageService;
  private VeniceStoreConfig mockStoreConfig;
  private int testPartition = 0;

  private Message mockMessage;
  private NotificationContext mockContext;

  private VenicePartitionStateModel testStateModel;

  @BeforeSuite
  public void setUp() throws Exception {
    mockKafkaConsumerService = Mockito.mock(KafkaConsumerService.class);
    mockStorageService = Mockito.mock(StorageService.class);
    mockStoreConfig = Mockito.mock(VeniceStoreConfig.class);

    mockMessage = Mockito.mock(Message.class);
    mockContext = Mockito.mock(NotificationContext.class);

    testStateModel = new VenicePartitionStateModel(mockKafkaConsumerService, mockStorageService, mockStoreConfig,
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
    testStateModel.onBecomeOnlineFromOffline(mockMessage, mockContext);
    Mockito.verify(mockKafkaConsumerService, Mockito.times(1)).startConsumption(mockStoreConfig, testPartition);
    Mockito.verify(mockStorageService, Mockito.times(1)).openStoreForNewPartition(mockStoreConfig, testPartition);
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
    Mockito.verify(mockStorageService, Mockito.atLeastOnce()).dropStorePartition(mockStoreConfig , testPartition);
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
    Mockito.verify(mockStorageService, Mockito.atLeastOnce()).dropStorePartition(mockStoreConfig, testPartition);
  }
}