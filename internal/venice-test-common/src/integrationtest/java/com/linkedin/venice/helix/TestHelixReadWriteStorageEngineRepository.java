package com.linkedin.venice.helix;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Optional;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for HelixReadWriteStoreRepository. All the tests depend on Zookeeper. Please ensure there is a zookeeper
 * available for testing.
 */
public class TestHelixReadWriteStorageEngineRepository {
  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String storesPath = "/stores";
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  HelixReadWriteStoreRepository repo;

  @BeforeMethod
  public void zkSetup() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    zkClient.setZkSerializer(adapter);
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + storesPath, null, CreateMode.PERSISTENT);

    repo = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        cluster,
        Optional.empty(),
        new ClusterLockManager(cluster));
    repo.refresh();
  }

  @AfterMethod
  public void zkCleanup() {
    repo.clear();
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testAddAndReadStore() {
    repo.refresh();
    Store s1 = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    s1.addVersion(new VersionImpl(s1.getName(), s1.getLargestUsedVersionNumber() + 1, "pushJobId"));
    repo.addStore(s1);
    Store s2 = repo.getStore("s1");
    Assert.assertEquals(s2, s1, "Store was not added successfully");
  }

  @Test
  public void testAddAndDeleteStore() {
    Store s1 = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    repo.addStore(s1);
    repo.deleteStore("s1");
    Assert.assertNull(repo.getStore("s1"));
  }

  @Test
  public void testUpdateAndReadStore() {
    Store s1 = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    s1.addVersion(new VersionImpl(s1.getName(), s1.getLargestUsedVersionNumber() + 1, "pushJobId"));
    repo.addStore(s1);
    Store s2 = repo.getStore(s1.getName());
    s2.addVersion(new VersionImpl(s2.getName(), s2.getLargestUsedVersionNumber() + 1, "pushJobId2"));
    repo.updateStore(s2);
    Store s3 = repo.getStore(s2.getName());
    Assert.assertEquals(s3, s2, "Store was not updated successfully.");
  }

  @Test
  public void testLoadFromZK() {
    repo.refresh();
    Store s1 = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    s1.addVersion(new VersionImpl(s1.getName(), s1.getLargestUsedVersionNumber() + 1, "pushJobId"));
    s1.setReadQuotaInCU(100);
    repo.addStore(s1);
    Store s2 = TestUtils.createTestStore("s2", "owner", System.currentTimeMillis());
    s2.addVersion(new VersionImpl(s2.getName(), 3));
    s2.setReadQuotaInCU(200);
    repo.addStore(s2);

    HelixReadWriteStoreRepository newRepo = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        cluster,
        Optional.empty(),
        new ClusterLockManager(cluster));
    newRepo.refresh();
    Assert.assertEquals(newRepo.getStore(s1.getName()), s1, "Can not load stores from ZK successfully");
    Assert.assertEquals(newRepo.getStore(s2.getName()), s2, "Can not load stores from ZK successfully");
    newRepo.clear();
    Assert.assertNull(newRepo.getStore(s1.getName()));
    Assert.assertNull(newRepo.getStore(s2.getName()));
  }
}
