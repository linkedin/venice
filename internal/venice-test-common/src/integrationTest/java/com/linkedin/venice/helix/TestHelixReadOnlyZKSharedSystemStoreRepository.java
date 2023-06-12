package com.linkedin.venice.helix;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHelixReadOnlyZKSharedSystemStoreRepository {
  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String storesPath = "/stores";
  private ZkServerWrapper zkServerWrapper;
  private final HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  private HelixReadOnlyZKSharedSystemStoreRepository repo;
  private HelixReadWriteStoreRepository writeRepo;

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

    repo = new HelixReadOnlyZKSharedSystemStoreRepository(zkClient, adapter, cluster);
    writeRepo = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        cluster,
        Optional.empty(),
        new ClusterLockManager(cluster));
    repo.refresh();
    writeRepo.refresh();
    // Create zk shared store first
    Store zkSharedStore =
        TestUtils.createTestStore(systemStoreType.getZkSharedStoreName(), "test_system_store_owner", 1);
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
    writeRepo.addStore(s1);
  }

  @AfterClass
  public void zkCleanup() {
    repo.clear();
    writeRepo.clear();
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testGetSystemStore() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertTrue(writeRepo.hasStore(regularStoreName));
      assertTrue(repo.hasStore(systemStoreType.getZkSharedStoreName()));
      assertNotNull(repo.getStore(systemStoreType.getZkSharedStoreName()));
      assertNull(repo.getStore(regularStoreName));
      assertFalse(repo.hasStore(regularStoreName));
    });
  }

  @Test
  public void testGetStoreOrThrow() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertTrue(writeRepo.hasStore(regularStoreName));
      assertNotNull(repo.getStoreOrThrow(systemStoreType.getZkSharedStoreName()));
      assertThrows(() -> repo.getStoreOrThrow(regularStoreName));
    });
  }

  @Test
  public void testRefreshOneStore() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertThrows(() -> repo.refreshOneStore(regularStoreName));
      Store zkSharedStore = repo.refreshOneStore(systemStoreType.getZkSharedStoreName());
      assertNotNull(zkSharedStore);
    });
  }

  @Test
  public void testGetBatchGetLimit() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertThrows(() -> repo.getBatchGetLimit(regularStoreName));
      assertEquals(repo.getBatchGetLimit(systemStoreType.getZkSharedStoreName()), -1);
    });
  }

  @Test
  public void testIsReadComputationEnabled() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertThrows(() -> repo.isReadComputationEnabled(regularStoreName));
      assertFalse(repo.isReadComputationEnabled(systemStoreType.getZkSharedStoreName()));
    });
  }

  @Test
  public void testCanReadRepoSyncUpWithWriteRepo() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertNotNull(repo.getStore(systemStoreType.getZkSharedStoreName()));
      // The store should not be hybrid yet
      assertFalse(repo.getStore(systemStoreType.getZkSharedStoreName()).isHybrid());
    });
    // Update the zkSharedStore in write repo and check to make sure read repo gets the updates.
    Store zkSharedStore = writeRepo.getStore(systemStoreType.getZkSharedStoreName());
    zkSharedStore.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            3600,
            1,
            60,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    writeRepo.updateStore(zkSharedStore);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertTrue(repo.getStore(systemStoreType.getZkSharedStoreName()).isHybrid());
    });
  }
}
