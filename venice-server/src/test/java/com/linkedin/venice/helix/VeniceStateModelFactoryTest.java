package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.StorageService;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceStateModelFactoryTest {
  private KafkaConsumerService mockKafkaConsumerService;
  private StorageService mockStorageService;
  private VeniceConfigLoader mockConfigLoader;
  private VeniceServerConfig mockServerConfig;
  private VeniceStoreConfig mockStoreConfig;
  private int testPartition = 0;
  private String resourceName = "test";

  private Message mockMessage;
  private NotificationContext mockContext;

  private VeniceStateModelFactory factory;
  private VenicePartitionStateModel stateModel;

  @BeforeMethod
  public void setup() {
    mockKafkaConsumerService = Mockito.mock(KafkaConsumerService.class);
    mockStorageService = Mockito.mock(StorageService.class);
    mockConfigLoader = Mockito.mock(VeniceConfigLoader.class);
    mockServerConfig = Mockito.mock(VeniceServerConfig.class);
    mockStoreConfig = Mockito.mock(VeniceStoreConfig.class);
    Mockito.when(mockConfigLoader.getVeniceServerConfig()).thenReturn(mockServerConfig);
    Mockito.when(mockConfigLoader.getStoreConfig(resourceName)).thenReturn(mockStoreConfig);

    mockMessage = Mockito.mock(Message.class);
    Mockito.when(mockMessage.getResourceName()).thenReturn(resourceName);

    mockContext = Mockito.mock(NotificationContext.class);

    factory = new VeniceStateModelFactory(mockKafkaConsumerService, mockStorageService, mockConfigLoader);
    stateModel = factory.createNewStateModel(resourceName, resourceName + "_" + testPartition);
  }

  @Test
  public void testStartConsumption() {
    stateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);

    Assert.assertEquals(factory.getNotifier().getLatch(resourceName, testPartition).getCount(), 1,
        "After becoming bootstrap, latch should be set to 1.");
  }

  @Test(timeOut = 3000)
  public void testConsumptionComplete() {
    stateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);
    Thread consumeThread = new Thread(new Runnable() {
      @Override
      public void run() {
        //Mock consume delay.
        try {
          Thread.sleep(1000);
          factory.getNotifier().completed(resourceName, testPartition, 0);
        } catch (InterruptedException e) {
          Assert.fail("Test if interrupted.");
        }
      }
    });
    consumeThread.start();
    stateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext);
    Assert.assertNull(factory.getNotifier().getLatch(resourceName, testPartition));
  }

  @Test(timeOut = 3000, expectedExceptions = VeniceException.class)
  public void testConsumptionFail() {
    stateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);
    Thread consumeThread = new Thread(new Runnable() {
      @Override
      public void run() {
        //Mock consume delay.
        try {
          Thread.sleep(1000);
          factory.getNotifier().error(resourceName, testPartition, "error", new Exception());
        } catch (InterruptedException e) {
          Assert.fail("Test if interrupted.");
        }
      }
    });
    consumeThread.start();
    stateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext);
  }

  @Test
  public void testWrongWaiting() {
    stateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);
    factory.getNotifier().removeLatch(resourceName, testPartition);
    try {
      stateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext);
      Assert.fail("Latch was deleted, should throw exception before becoming online.");
    } catch (VeniceException e) {
      //expected
    }
  }
}
