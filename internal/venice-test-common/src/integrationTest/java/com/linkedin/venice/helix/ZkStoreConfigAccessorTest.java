package com.linkedin.venice.helix;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.StoreConfig;
import java.io.IOException;
import java.util.Optional;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ZkStoreConfigAccessorTest {
  private ZkStoreConfigAccessor accessor;
  private ZkClient zkClient;
  private ZkServerWrapper zk;

  @BeforeMethod
  public void setUp() {
    zk = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zk.getAddress());
    accessor = new ZkStoreConfigAccessor(zkClient, new HelixAdapterSerializer(), Optional.empty());
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.close();
    zk.close();
  }

  @Test
  public void testCreateConfig() {
    String store = "testCreateConfig-store";
    String cluster = "testCreateConfig-cluster";
    accessor.createConfig(store, cluster);

    StoreConfig config = accessor.getStoreConfig(store);
    Assert.assertEquals(config.getStoreName(), store, "Config should be created in ZK correctly");
    Assert.assertEquals(config.getCluster(), cluster, "Config should be created in ZK correctly");
  }

  @Test
  public void testContainsConfig() {
    String store = "testContainsConfig-store";
    String cluster = "testContainsConfig-cluster";
    Assert.assertFalse(accessor.containsConfig(store), "Store config has not been created.");
    accessor.createConfig(store, cluster);
    Assert.assertTrue(accessor.containsConfig(store), "Store should be created correctly.");
  }

  @Test
  public void testUpdateConfig() throws IOException {
    String store = "testContainsConfig-store";
    String cluster = "testContainsConfig-cluster";
    accessor.createConfig(store, cluster);

    StoreConfig config = accessor.getStoreConfig(store);
    String newCluster = "testContainsConfig-new-cluster";
    config.setCluster(newCluster);
    config.setDeleting(true);
    accessor.updateConfig(config, false);
    Assert.assertEquals(
        accessor.getStoreConfig(store).getCluster(),
        newCluster,
        "Store config should be updated correctly.");
    Assert.assertTrue(accessor.getStoreConfig(store).isDeleting(), "Store config should be updated correctly.");
  }

  @Test
  public void testDeleteConfig() {
    String store = "testContainsConfig-store";
    String cluster = "testContainsConfig-cluster";
    accessor.createConfig(store, cluster);
    Assert.assertTrue(accessor.containsConfig(store));
    accessor.deleteConfig(store);
    Assert.assertFalse(accessor.containsConfig(store), "Store Config should be deleted correctly.");
  }

  @Test
  public void testGetAllStores() {
    int storeCount = 10;
    for (int i = 0; i < storeCount; i++) {
      accessor.createConfig("testGetAllStores-store" + i, "testGetAllStores-cluster" + i);
    }
    Assert.assertTrue(accessor.getAllStores().size() == storeCount, "Should get all stores from ZK.");
  }
}
