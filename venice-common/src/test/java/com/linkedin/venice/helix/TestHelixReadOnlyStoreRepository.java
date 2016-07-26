package com.linkedin.venice.helix;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadOnlyStoreRepository {
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

    repo = new HelixReadOnlyStoreRepository(zkClient, adapter, cluster);
    writeRepo = new HelixReadWriteStoreRepository(zkClient, adapter, cluster);
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
}
