package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.TestUtils;
import java.util.Optional;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceStateModelFactoryTest {
  private VeniceIngestionBackend mockIngestionBackend;
  private VeniceConfigLoader mockConfigLoader;
  private VeniceServerConfig mockServerConfig;
  private VeniceStoreConfig mockStoreConfig;
  private ReadOnlyStoreRepository mockReadOnlyStoreRepository;
  private Store mockStore;
  private int testPartition = 0;
  private String resourceName = "test_v1";

  private Message mockMessage;
  private NotificationContext mockContext;

  private OnlineOfflinePartitionStateModelFactory factory;
  private OnlineOfflinePartitionStateModel stateModel;
  private ExecutorService executorService;

  @BeforeClass
  void setup() {
    executorService = Executors.newCachedThreadPool(new DaemonThreadFactory("venice-unittest"));
  }

  @AfterClass
  void cleanup() {
    executorService.shutdownNow();
  }

  @BeforeMethod
  public void setupTestCase() {
    mockIngestionBackend = Mockito.mock(VeniceIngestionBackend.class);
    mockConfigLoader = Mockito.mock(VeniceConfigLoader.class);
    mockServerConfig = Mockito.mock(VeniceServerConfig.class);
    mockStoreConfig = Mockito.mock(VeniceStoreConfig.class);
    mockReadOnlyStoreRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    mockStore = Mockito.mock(Store.class);
    Mockito.when(mockConfigLoader.getVeniceServerConfig()).thenReturn(mockServerConfig);
    Mockito.when(mockConfigLoader.getStoreConfig(resourceName)).thenReturn(mockStoreConfig);
    Mockito.when(mockReadOnlyStoreRepository.getStore(Version.parseStoreFromKafkaTopicName(resourceName)))
        .thenReturn(mockStore);
    Mockito.when(mockStore.getBootstrapToOnlineTimeoutInHours()).thenReturn(Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);

    mockMessage = Mockito.mock(Message.class);
    Mockito.when(mockMessage.getResourceName()).thenReturn(resourceName);

    mockContext = Mockito.mock(NotificationContext.class);

    factory = new OnlineOfflinePartitionStateModelFactory(
        mockIngestionBackend,
        mockConfigLoader,
        this.executorService,
        mockReadOnlyStoreRepository, Optional.empty(), null);
    stateModel = factory.createNewStateModel(resourceName, resourceName + "_" + testPartition);
  }

  @Test
  public void testStartConsumption() {
    stateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);

    Assert.assertEquals(factory.getNotifier().getIngestionCompleteFlag(resourceName, testPartition).getCount(), 1,
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
    Assert.assertNull(factory.getNotifier().getIngestionCompleteFlag(resourceName, testPartition));
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
    factory.getNotifier().removeIngestionCompleteFlag(resourceName, testPartition);
    try {
      stateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext);
      Assert.fail("Latch was deleted, should throw exception before becoming online.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testNumberOfStateTransitionsExceedThreadPoolSize() {
    int threadPoolSize = 1;
    LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 2L, TimeUnit.SECONDS, queue,
        new DaemonThreadFactory("venice-state-transition"));
    executor.allowCoreThreadTimeOut(true);
    factory = new OnlineOfflinePartitionStateModelFactory(mockIngestionBackend, mockConfigLoader, executor,
        mockReadOnlyStoreRepository, Optional.empty(), null);
    ExecutorService testExecutor = factory.getExecutorService("");
    Assert.assertEquals(testExecutor, executor);

    BlockTask blockedTask = new BlockTask();
    testExecutor.submit(blockedTask);
    int taskCount = 10;
    for (int i = 0; i < taskCount; i++) {
      testExecutor.submit(() -> {});
    }
    Assert.assertEquals(queue.size(), taskCount);
    // Test could fail due to the signal() is executed before await()
    TestUtils.waitForNonDeterministicCompletion(1000, TimeUnit.MILLISECONDS, () -> blockedTask.isWaiting);
    blockedTask.resume();

    // After resume the blocking task, it's executed and return true.
    TestUtils.waitForNonDeterministicCompletion(1000, TimeUnit.MILLISECONDS, () -> blockedTask.result);
    // eventually, the queue would be empty because all of task had been executed.
    TestUtils.waitForNonDeterministicCompletion(1000, TimeUnit.MILLISECONDS, () -> queue.isEmpty());
    // Make sure the idle thread will be collected eventually.
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> executor.getPoolSize() == 0);
  }

  private class BlockTask implements Runnable {
    private Lock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private volatile boolean result = false;
    private volatile boolean isWaiting = false;

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
      isWaiting = true;
      try {
        condition.await();
        result = true;
      } catch (InterruptedException e) {
        result = false;
      } finally {
        isWaiting = false;
        lock.unlock();
      }
    }
  }
}
