package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.linkedin.venice.utils.Utils;
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
  private VeniceStoreVersionConfig mockStoreConfig;
  private ReadOnlyStoreRepository mockReadOnlyStoreRepository;
  private HelixPartitionStatusAccessor mockPushStatusAccessor;
  private Store mockStore;
  private int testPartition = 0;
  private String resourceName = "test_v1";

  private Message mockMessage;
  private NotificationContext mockContext;

  private OnlineOfflinePartitionStateModelFactory factory;
  private OnlineOfflinePartitionStateModel stateModel;
  private ExecutorService executorService;

  @BeforeClass
  void setUp() {
    executorService = Executors.newCachedThreadPool(new DaemonThreadFactory("venice-unittest"));
  }

  @AfterClass
  void cleanUp() throws InterruptedException {
    TestUtils.shutdownExecutor(executorService);
  }

  @BeforeMethod
  public void setupTestCase() {
    mockIngestionBackend = Mockito.mock(VeniceIngestionBackend.class);
    mockConfigLoader = Mockito.mock(VeniceConfigLoader.class);
    mockServerConfig = Mockito.mock(VeniceServerConfig.class);
    mockStoreConfig = Mockito.mock(VeniceStoreVersionConfig.class);
    mockReadOnlyStoreRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    mockPushStatusAccessor = Mockito.mock(HelixPartitionStatusAccessor.class);
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
        mockReadOnlyStoreRepository, CompletableFuture.completedFuture(mockPushStatusAccessor), null);
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
    CompletableFuture completeFuture = CompletableFuture.runAsync(() -> {
      Utils.sleep(1000);
      factory.getNotifier().completed(resourceName, testPartition, 0);
    });
    stateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext);
    Assert.assertTrue(completeFuture.isDone());
    Assert.assertNull(factory.getNotifier().getIngestionCompleteFlag(resourceName, testPartition));
  }

  @Test(timeOut = 3000, expectedExceptions = VeniceException.class)
  public void testConsumptionFail() {
    stateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);
    CompletableFuture completeFuture = CompletableFuture.runAsync(() -> {
      Utils.sleep(1000);
      factory.getNotifier().error(resourceName, testPartition, "error", new Exception());
    });
    stateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext);
    Assert.assertTrue(completeFuture.isDone());
  }

  @Test
  public void testWrongWaiting() {
    stateModel.onBecomeBootstrapFromOffline(mockMessage, mockContext);
    factory.getNotifier().removeIngestionCompleteFlag(resourceName, testPartition);
    Assert.assertThrows(VeniceException.class, () -> stateModel.onBecomeOnlineFromBootstrap(mockMessage, mockContext));
  }

  @Test
  public void testNumberOfStateTransitionsExceedThreadPoolSize() throws InterruptedException {
    ThreadPoolExecutor executor = null;
    ExecutorService testExecutor = null;
    try {
      int threadPoolSize = 1;
      LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
      executor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 2L, TimeUnit.SECONDS, queue,
          new DaemonThreadFactory("venice-state-transition"));
      executor.allowCoreThreadTimeOut(true);
      factory = new OnlineOfflinePartitionStateModelFactory(mockIngestionBackend, mockConfigLoader, executor,
          mockReadOnlyStoreRepository, CompletableFuture.completedFuture(mockPushStatusAccessor), null);
      testExecutor = factory.getExecutorService("");
      Assert.assertEquals(testExecutor, executor);

      BlockTask blockedTask = new BlockTask();
      testExecutor.submit(blockedTask);
      int taskCount = 10;
      for (int i = 0; i < taskCount; i++) {
        testExecutor.submit(() -> {});
      }
      Assert.assertEquals(queue.size(), taskCount);
      // Test could fail due to the signal() is executed before await()
      TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> blockedTask.isWaiting);
      blockedTask.resume();

      // After resume the blocking task, it's executed and return true.
      TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> blockedTask.result);
      // eventually, the queue would be empty because all task had been executed.
      TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, queue::isEmpty);
      // Make sure the idle thread will be collected eventually.
      ThreadPoolExecutor finalExecutor = executor;
      TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> finalExecutor.getPoolSize() == 0);
    } finally {
      TestUtils.shutdownExecutor(executor);
      TestUtils.shutdownExecutor(testExecutor);
    }
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
