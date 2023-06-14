package com.linkedin.venice.helix;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SystemStoreAttributes;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHelixReadWriteStoreRepositoryAdapter {
  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String storesPath = "/stores";
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  private HelixReadWriteStoreRepositoryAdapter writeRepoAdapter;
  private HelixReadOnlyStoreRepository readOnlyRepo;

  private final VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.META_STORE;
  private String regularStoreName;

  @BeforeClass
  public void zkSetup() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    zkClient.setZkSerializer(adapter);
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + storesPath, null, CreateMode.PERSISTENT);

    HelixReadOnlyZKSharedSystemStoreRepository zkSharedSystemStoreRepository =
        new HelixReadOnlyZKSharedSystemStoreRepository(zkClient, adapter, cluster);
    zkSharedSystemStoreRepository.refresh();
    HelixReadWriteStoreRepository writeRepo = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        cluster,
        Optional.empty(),
        new ClusterLockManager(cluster));
    writeRepo.refresh();

    writeRepoAdapter = new HelixReadWriteStoreRepositoryAdapter(zkSharedSystemStoreRepository, writeRepo, cluster);
    // Create zk shared store first
    Store zkSharedStore =
        TestUtils.createTestStore(systemStoreType.getZkSharedStoreName(), "test_system_store_owner", 1);
    zkSharedStore.setBatchGetLimit(1);
    zkSharedStore.setReadComputationEnabled(false);
    writeRepo.addStore(zkSharedStore);
    TestUtils.waitForNonDeterministicAssertion(
        10,
        TimeUnit.SECONDS,
        () -> assertTrue(writeRepo.hasStore(systemStoreType.getZkSharedStoreName())));
    // Create one regular store
    regularStoreName = Utils.getUniqueString("test_store");
    Store s1 = TestUtils.createTestStore(regularStoreName, "owner", System.currentTimeMillis());
    s1.addVersion(new VersionImpl(s1.getName(), s1.getLargestUsedVersionNumber() + 1, "pushJobId"));
    s1.setReadQuotaInCU(100);
    s1.setBatchGetLimit(100);
    s1.setReadComputationEnabled(true);
    writeRepo.addStore(s1);

    readOnlyRepo = new HelixReadOnlyStoreRepository(zkClient, adapter, cluster, 1, 1000);
    readOnlyRepo.refresh();
  }

  @AfterClass
  public void zkCleanup() {
    readOnlyRepo.clear();
    writeRepoAdapter.clear();
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testAddStoreToStoreRepoAdapter() {
    // Create a regular store
    String anotherRegularStoreName = Utils.getUniqueString("test_store");
    Store testStore = TestUtils.createTestStore(anotherRegularStoreName, "test_owner", 0);
    writeRepoAdapter.addStore(testStore);
    // Verify the store via read only repo
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertTrue(readOnlyRepo.hasStore(anotherRegularStoreName));
      assertTrue(writeRepoAdapter.hasStore(systemStoreType.getSystemStoreName(anotherRegularStoreName)));
    });
    // Adding a system store directly will fail
    assertThrows(
        () -> writeRepoAdapter.addStore(
            TestUtils.createTestStore(systemStoreType.getSystemStoreName(anotherRegularStoreName), "test_owner", 0)));
  }

  @Test
  public void testDeleteStore() {
    // Add a regular store
    String anotherRegularStoreName = Utils.getUniqueString("test_store");
    Store testStore = TestUtils.createTestStore(anotherRegularStoreName, "test_owner", 0);
    writeRepoAdapter.addStore(testStore);
    // Verify the store via read only repo
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertTrue(readOnlyRepo.hasStore(anotherRegularStoreName));
      assertTrue(writeRepoAdapter.hasStore(systemStoreType.getSystemStoreName(anotherRegularStoreName)));
    });
    writeRepoAdapter.deleteStore(anotherRegularStoreName);
    // Deleting a system store directly will fail
    assertThrows(() -> writeRepoAdapter.deleteStore(systemStoreType.getSystemStoreName(anotherRegularStoreName)));

    // Verify the store via read only repo
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertFalse(readOnlyRepo.hasStore(anotherRegularStoreName));
      assertFalse(writeRepoAdapter.hasStore(systemStoreType.getSystemStoreName(anotherRegularStoreName)));
    });
  }

  @Test
  public void testUpdateStore() {
    // Add a regular store
    String anotherRegularStoreName = Utils.getUniqueString("test_store");
    Store testStore = TestUtils.createTestStore(anotherRegularStoreName, "test_owner", 0);
    writeRepoAdapter.addStore(testStore);
    // Verify the store via read only repo
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertTrue(readOnlyRepo.hasStore(anotherRegularStoreName));
      assertTrue(writeRepoAdapter.hasStore(systemStoreType.getSystemStoreName(anotherRegularStoreName)));
      assertEquals(readOnlyRepo.getStore(anotherRegularStoreName).getBatchGetLimit(), -1);
    });

    // Try to update a regular store
    testStore.setBatchGetLimit(1000);
    writeRepoAdapter.updateStore(testStore);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertEquals(readOnlyRepo.getStore(anotherRegularStoreName).getBatchGetLimit(), 1000);
    });

    // System stores should be empty before any system store update
    assertTrue(readOnlyRepo.getStore(anotherRegularStoreName).getSystemStores().isEmpty());

    // Test to update a system store
    Store systemStore = writeRepoAdapter.getStore(systemStoreType.getSystemStoreName(anotherRegularStoreName));
    systemStore.addVersion(new VersionImpl(systemStore.getName(), 1, "test_push_id_1"));
    writeRepoAdapter.updateStore(systemStore);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      Map<String, SystemStoreAttributes> systemStores =
          readOnlyRepo.getStore(anotherRegularStoreName).getSystemStores();
      assertEquals(systemStores.size(), 1);
      assertTrue(systemStores.containsKey(systemStoreType.getPrefix()));
      // a new system store version should present
      assertEquals(systemStores.get(systemStoreType.getPrefix()).getVersions().size(), 1);
    });
  }
}
