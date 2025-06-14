package com.linkedin.venice.helix;

import static com.linkedin.venice.utils.TestUtils.getRandomStore;
import static com.linkedin.venice.zk.VeniceZkPaths.STORES;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadOnlyStorageEngineRepository {
  private static final Logger LOGGER = LogManager.getLogger(TestHelixReadOnlyStorageEngineRepository.class);

  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String storesPath = "/" + STORES;
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  private HelixReadOnlyStoreRepository repo;
  private HelixReadWriteStoreRepository writeRepo;

  @BeforeMethod
  public void zkSetup() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    zkClient.setZkSerializer(adapter);
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + storesPath, null, CreateMode.PERSISTENT);

    repo = new HelixReadOnlyStoreRepository(zkClient, adapter, cluster);
    writeRepo = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        cluster,
        Optional.empty(),
        new ClusterLockManager(cluster));
    repo.refresh();
    writeRepo.refresh();
  }

  @AfterMethod
  public void zkCleanup() {
    repo.clear();
    writeRepo.clear();
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testGetStore() throws InterruptedException {
    // Add and get notificaiton
    Store s1 = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    s1.addVersion(new VersionImpl("s1", 1, "pushJobId"));
    s1.setReadQuotaInCU(100);
    writeRepo.addStore(s1);
    Thread.sleep(1000L);
    /**
     * The store instance returned by {@link HelixReadOnlyStoreRepository} is {@link com.linkedin.venice.meta.ReadOnlyStore},
     * so we couldn't compare it directly with the original store, but the cloned store can since the cloned store won't
     * be a {@link ReadOnlyStore}.
     */
    Assert.assertEquals(
        repo.getStore(s1.getName()).cloneStore(),
        s1,
        "Can not get store from ZK notification successfully");

    // Update and get notification
    Store s2 = writeRepo.getStore(s1.getName());
    s2.addVersion(new VersionImpl(s2.getName(), s2.getLargestUsedVersionNumber() + 1, "pushJobId2"));
    writeRepo.updateStore(s2);
    Thread.sleep(1000L);
    Assert.assertEquals(
        repo.getStore(s1.getName()).cloneStore(),
        s2,
        "Can not get store from ZK notification successfully");

    // Delete and get notification
    writeRepo.deleteStore(s1.getName());
    Thread.sleep(1000L);
    Assert.assertNull(repo.getStore(s1.getName()), "Can not get store from ZK notification successfully");
  }

  @Test
  public void testLoadFromZK() throws InterruptedException {
    int count = 10;
    Store[] stores = new Store[count];
    for (int i = 0; i < count; i++) {
      Store s = TestUtils.createTestStore("s" + i, "owner", System.currentTimeMillis());
      s.addVersion(new VersionImpl(s.getName(), s.getLargestUsedVersionNumber() + 1, "pushJobId"));
      s.setReadQuotaInCU(i + 1);
      writeRepo.addStore(s);
      stores[i] = s;
    }
    Thread.sleep(1000L);
    for (Store store: stores) {
      /**
       * The store instance returned by {@link HelixReadOnlyStoreRepository} is {@link com.linkedin.venice.meta.ReadOnlyStore},
       * so we couldn't compare it directly with the original store, but the cloned store can since the cloned store won't
       * be a {@link ReadOnlyStore}.
       */
      Assert.assertEquals(
          repo.getStore(store.getName()).cloneStore(),
          store,
          "Can not get store from ZK notification successfully");
    }

    repo.refresh();
    for (Store store: stores) {
      Assert.assertEquals(
          repo.getStore(store.getName()).cloneStore(),
          store,
          "Can not get store from ZK after refreshing successfully");
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
      LOGGER.info("Received handleStoreChanged: {}", store);
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
    assertListenerCounts(
        testListener,
        creationCount.addAndGet(3),
        changeCount.get(),
        deletionCount.get(),
        "store creations");

    // Hang on to a store reference so we can tweak it
    Store store = getRandomStore();

    // Verify creation idempotency

    writeRepo.addStore(store);
    try {
      writeRepo.addStore(store);
    } catch (VeniceException e) {
      // Excepted
    }
    assertListenerCounts(
        testListener,
        creationCount.addAndGet(1),
        changeCount.get(),
        deletionCount.get(),
        "creation of a duplicate store");

    // Verify change notifications

    store.setCurrentVersion(10);
    writeRepo.updateStore(store);
    assertListenerCounts(
        testListener,
        creationCount.get(),
        changeCount.addAndGet(1),
        deletionCount.get(),
        "change of a store-version");

    store.setOwner(Utils.getUniqueString("NewRandomOwner"));
    writeRepo.updateStore(store);
    assertListenerCounts(
        testListener,
        creationCount.get(),
        changeCount.addAndGet(1),
        deletionCount.get(),
        "change of owner");

    store.setPartitionCount(10);
    writeRepo.updateStore(store);
    assertListenerCounts(
        testListener,
        creationCount.get(),
        changeCount.addAndGet(1),
        deletionCount.get(),
        "change of partition count");

    store.setLargestUsedVersionNumber(100);
    writeRepo.updateStore(store);
    assertListenerCounts(
        testListener,
        creationCount.get(),
        changeCount.addAndGet(1),
        deletionCount.get(),
        "change of largest used version number");

    store.setEnableWrites(false);
    writeRepo.updateStore(store);
    assertListenerCounts(
        testListener,
        creationCount.get(),
        changeCount.addAndGet(1),
        deletionCount.get(),
        "disabling writes");

    store.setEnableReads(false);
    writeRepo.updateStore(store);
    assertListenerCounts(
        testListener,
        creationCount.get(),
        changeCount.addAndGet(1),
        deletionCount.get(),
        "disabling reads");

    // Verify deletion notification
    writeRepo.deleteStore(store.getName());
    assertListenerCounts(
        testListener,
        creationCount.get(),
        changeCount.get(),
        deletionCount.addAndGet(1),
        "store deletion");

    // Verify that refresh() does not double count notifications...
    repo.refresh();
    assertListenerCounts(
        testListener,
        creationCount.get(),
        changeCount.get(),
        deletionCount.get(),
        "ReadOnly Repo refresh()");

    // TODO: Find a good way to test that refresh() works in isolation when there are actually changes to report.
    // Currently, the other listeners are always called first, so refresh() doesn't have any work left...
  }

  private void assertListenerCounts(
      TestListener testListener,
      int expectedCreationCount,
      int expectedChangeCount,
      int expectedDeletionCount,
      String details) {
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          testListener.getCreationCount(),
          expectedCreationCount,
          "Listener's creation count should be " + expectedCreationCount + " following: " + details);
      Assert.assertEquals(
          testListener.getChangeCount(),
          expectedChangeCount,
          "Listener's change count should be " + expectedChangeCount + " following: " + details);
      Assert.assertEquals(
          testListener.getDeletionCount(),
          expectedDeletionCount,
          "Listener's deletion count should be " + expectedDeletionCount + " following: " + details);

      LOGGER.info("Successfully asserted that notifications work after {}", details);
    });
  }
}
