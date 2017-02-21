package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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

    factory = new VeniceStateModelFactory(mockKafkaConsumerService, mockStorageService, mockConfigLoader,
        Executors.newCachedThreadPool(new DaemonThreadFactory("venice-unittest")));
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

  @Test
  public void testNumberOfStateTransitionsExceedThreadPoolSize()
      throws ExecutionException, InterruptedException {
    int threadPoolSize = 1;
    LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ExecutorService executor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 300L, TimeUnit.SECONDS, queue,
        new DaemonThreadFactory("venice-state-transition"));
    factory = new VeniceStateModelFactory(mockKafkaConsumerService, mockStorageService, mockConfigLoader, executor);
    ExecutorService testExecutor = factory.getExecutorService("");
    Assert.assertEquals(testExecutor, executor);

    BlockTask blockedTask = new BlockTask();
    testExecutor.submit(blockedTask);
    int taskCount = 10;
    for (int i = 0; i < taskCount; i++) {
      testExecutor.submit(() -> {
      });
    }
    Assert.assertEquals(queue.size(), taskCount);
    blockedTask.resume();

    // After resume the blocking task, it's executed and return true.
    TestUtils.waitForNonDeterministicCompletion(1000, TimeUnit.MILLISECONDS, () -> blockedTask.result);
    // eventually, the queue would be empty because all of task had been executed.
    TestUtils.waitForNonDeterministicCompletion(1000, TimeUnit.MILLISECONDS, () -> queue.isEmpty());
  }

  private class BlockTask implements Runnable {
    private Lock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private volatile boolean result = false;

    public void resume() {
      lock.lock();
      try {
        condition.signal();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void run() {
      lock.lock();
      try {
        condition.await();
        result = true;
      } catch (InterruptedException e) {
        result = false;
      } finally {
        lock.unlock();
      }
    }
  }
}
