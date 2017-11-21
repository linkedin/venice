package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadOnlyStoreRepository {
  private static Logger logger = Logger.getLogger(TestHelixReadOnlyStoreRepository.class);

  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String storesPath = "/stores";
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  private HelixReadOnlyStoreRepository repo;
  private HelixReadWriteStoreRepository writeRepo;

  @BeforeMethod
  public void zkSetup() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    zkClient = new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, adapter);
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + storesPath, null, CreateMode.PERSISTENT);

    repo = new HelixReadOnlyStoreRepository(zkClient, adapter, cluster, 1, 1000);
    writeRepo = new HelixReadWriteStoreRepository(zkClient, adapter, cluster, 1, 1000);
    repo.refresh();
    writeRepo.refresh();
  }

  @AfterMethod
  public void zkCleanup() {
    repo.clear();
    writeRepo.clear();
    zkClient.deleteRecursive(clusterPath);
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testGetStore()
      throws InterruptedException {
    // Add and get notificaiton
    Store s1 = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    s1.increaseVersion();
    s1.setReadQuotaInCU(100);
    writeRepo.addStore(s1);
    Thread.sleep(1000L);
    Assert.assertEquals(repo.getStore(s1.getName()), s1, "Can not get store from ZK notification successfully");

    // Update and get notification
    Store s2 = writeRepo.getStore(s1.getName());
    s2.increaseVersion();
    writeRepo.updateStore(s2);
    Thread.sleep(1000L);
    Assert.assertEquals(repo.getStore(s1.getName()), s2, "Can not get store from ZK notification successfully");

    // Delete and get notification
    writeRepo.deleteStore(s1.getName());
    Thread.sleep(1000L);
    Assert.assertNull(repo.getStore(s1.getName()), "Can not get store from ZK notification successfully");
  }

  @Test
  public void testLoadFromZK()
      throws InterruptedException {
    int count = 10;
    Store[] stores = new Store[count];
    for (int i = 0; i < count; i++) {
      Store s = TestUtils.createTestStore("s" + i, "owner", System.currentTimeMillis());
      s.increaseVersion();
      s.setReadQuotaInCU(i + 1);
      writeRepo.addStore(s);
      stores[i] = s;
    }
    Thread.sleep(1000L);
    for(Store store:stores){
      Assert.assertEquals(repo.getStore(store.getName()),store, "Can not get store from ZK notification successfully");
    }

    repo.refresh();
    for(Store store:stores){
      Assert.assertEquals(repo.getStore(store.getName()),store, "Can not get store from ZK after refreshing successfully");
    }
  }

  static class TestListener implements StoreDataChangedListener {
    AtomicInteger creationCount = new AtomicInteger(0);
    AtomicInteger changeCount = new AtomicInteger(0);
    AtomicInteger deletionCount = new AtomicInteger(0);

    @Override
    public void handleStoreCreated(Store store) {
      creationCount.incrementAndGet();
    }

    @Override
    public void handleStoreDeleted(String storeName) {
      deletionCount.incrementAndGet();
    }

    @Override
    public void handleStoreChanged(Store store) {
      logger.info("Received handleStoreChanged: " + store.toString());
      changeCount.incrementAndGet();
    }

    public int getCreationCount() {
      return creationCount.get();
    }

    public int getChangeCount() {
      return changeCount.get();
    }

    public int getDeletionCount() {
      return deletionCount.get();
    }
  }

  @Test
  public void testNotifiers() throws InterruptedException {
    AtomicInteger creationCount = new AtomicInteger(0);
    AtomicInteger changeCount = new AtomicInteger(0);
    AtomicInteger deletionCount = new AtomicInteger(0);
    TestListener testListener = new TestListener();
    repo.registerStoreDataChangedListener(testListener);

    // Verify initial state
    assertListenerCounts(testListener, creationCount.get(), changeCount.get(), deletionCount.get(), "initialization");

    // Verify creation notifications
    writeRepo.addStore(getRandomStore());
    writeRepo.addStore(getRandomStore());
    writeRepo.addStore(getRandomStore());
    assertListenerCounts(testListener, creationCount.addAndGet(3), changeCount.get(), deletionCount.get(), "store creations");

    // Hang on to a store reference so we can tweak it
    Store store = getRandomStore();

    // Verify creation idempotency

    writeRepo.addStore(store);
    try {
      writeRepo.addStore(store);
    } catch (VeniceException e) {
      // Excepted
    }
    assertListenerCounts(testListener, creationCount.addAndGet(1), changeCount.get(), deletionCount.get(), "creation of a duplicate store");

    // Verify change notifications

    store.setCurrentVersion(10);
    writeRepo.updateStore(store);
    assertListenerCounts(testListener, creationCount.get(), changeCount.addAndGet(1), deletionCount.get(), "change of a store-version");

    store.setOwner(TestUtils.getUniqueString("NewRandomOwner"));
    writeRepo.updateStore(store);
    assertListenerCounts(testListener, creationCount.get(), changeCount.addAndGet(1), deletionCount.get(), "change of owner");

    store.setPartitionCount(10);
    writeRepo.updateStore(store);
    assertListenerCounts(testListener, creationCount.get(), changeCount.addAndGet(1), deletionCount.get(), "change of partition count");

    store.setLargestUsedVersionNumber(100);
    writeRepo.updateStore(store);
    assertListenerCounts(testListener, creationCount.get(), changeCount.addAndGet(1), deletionCount.get(), "change of largest used version number");

    store.setEnableWrites(false);
    writeRepo.updateStore(store);
    assertListenerCounts(testListener, creationCount.get(), changeCount.addAndGet(1), deletionCount.get(), "disabling writes");

    store.setEnableReads(false);
    writeRepo.updateStore(store);
    assertListenerCounts(testListener, creationCount.get(), changeCount.addAndGet(1), deletionCount.get(), "disabling reads");

    // Verify deletion notification
    writeRepo.deleteStore(store.getName());
    assertListenerCounts(testListener, creationCount.get(), changeCount.get(), deletionCount.addAndGet(1), "store deletion");

    // Verify that refresh() does not double count notifications...
    repo.refresh();
    assertListenerCounts(testListener, creationCount.get(), changeCount.get(), deletionCount.get(), "ReadOnly Repo refresh()");

    // TODO: Find a good way to test that refresh() works in isolation when there are actually changes to report.
    // Currently, the other listeners are always called first, so refresh() doesn't have any work left...
  }

  @Test(timeOut = 3000)
  public void testNoDeadLockDuringRefresh(){
    final HashSet<String> threadNames = new HashSet<>();
    StoreDataChangedListener listener = new StoreDataChangedListener() {
      HelixReadOnlyStoreRepository repository = repo;

      @Override
      public void handleStoreCreated(Store store) {
        //do not acquire lock because it will be executed in paraell
      }

      @Override
      public void handleStoreDeleted(String storeName) {
        repository.getInternalReadWriteLock().readLock().lock();
        repository.getStore(storeName);
        threadNames.add(Thread.currentThread().getName());
        repository.getInternalReadWriteLock().readLock().unlock();
      }

      @Override
      public void handleStoreChanged(Store store) {
        repository.getInternalReadWriteLock().writeLock().lock();
        repository.getStore(store.getName());
        threadNames.add(Thread.currentThread().getName());
        repository.getInternalReadWriteLock().writeLock().unlock();
      }
    };
    // Add a store to repo
    repo.registerStoreDataChangedListener(listener);
    int storeCount = 20;
    for (int i = 0; i < storeCount; i++) {
      Store s = TestUtils.createTestStore("s" + i, "owner", System.currentTimeMillis());
      s.setReadQuotaInCU(100);
      writeRepo.addStore(s);
    }

    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> repo.getAllStores().size() == storeCount);
    repo.getInternalReadWriteLock().writeLock().lock();
    for (int i = 1; i < storeCount; i++) {
      Store s = writeRepo.getStore("s" + i);
      s.setReadQuotaInCU(10000);
      writeRepo.updateStore(s);
    }
    // As we already lock the read repo, so it will not process the any zk event.
    // So once we manually refresh, we will see all stores have been updated.
    repo.refresh();
    // Same here for store deletion. Will see all store disappear in manual refresh
    for (int i = 1; i < storeCount; i++) {
      writeRepo.deleteStore("s" + i);
    }
    repo.refresh();
    // Ensure the listener is running in current thread only.
    Assert.assertTrue(threadNames.size() == 1);
    Assert.assertEquals(threadNames.iterator().next(), Thread.currentThread().getName());
    repo.getInternalReadWriteLock().writeLock().unlock();
  }

  private void assertListenerCounts(TestListener testListener,
      int expectedCreationCount,
      int expectedChangeCount,
      int expectedDeletionCount,
      String details) {
    TestUtils.waitForNonDeterministicAssertion(3000, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(testListener.getCreationCount(), expectedCreationCount,
          "Listener's creation count should be " + expectedCreationCount + " following: " + details);
      Assert.assertEquals(testListener.getChangeCount(), expectedChangeCount,
          "Listener's change count should be " + expectedChangeCount + " following: " + details);
      Assert.assertEquals(testListener.getDeletionCount(), expectedDeletionCount,
          "Listener's deletion count should be " + expectedDeletionCount + " following: " + details);

      logger.info("Successfully asserted that notifications work after " + details);
    });
  }

  private Store getRandomStore() {
    return new Store(TestUtils.getUniqueString("RandomStore"),
        TestUtils.getUniqueString("RandomOwner"),
        System.currentTimeMillis(),
        PersistenceType.BDB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS);
  }
}
