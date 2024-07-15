package com.linkedin.venice.zk;

import static com.linkedin.venice.zk.VeniceZkPaths.ADMIN_TOPIC_METADATA;
import static com.linkedin.venice.zk.VeniceZkPaths.CLUSTER_ZK_PATHS;
import static com.linkedin.venice.zk.VeniceZkPaths.EXECUTION_IDS;
import static com.linkedin.venice.zk.VeniceZkPaths.PARENT_OFFLINE_PUSHES;
import static com.linkedin.venice.zk.VeniceZkPaths.ROUTERS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORES;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_CONFIGS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_GRAVEYARD;

import com.linkedin.venice.ZkCopier;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestMigrateVeniceZKPaths {
  private String srcZkAddress;
  private ZkClient srcZkClient;
  private ZkServerWrapper srcZkServerWrapper;
  private String destZkAddress;
  private ZkClient destZkClient;
  private ZkServerWrapper destZkServerWrapper;
  private static final String CLUSTER_1 = "cluster1";
  private static final String CLUSTER_2 = "cluster2";
  private final Set<String> CLUSTERS = new HashSet<>(Arrays.asList(CLUSTER_1, CLUSTER_2));
  private static final String BASE_PATH = "/venice-parent";

  @BeforeClass
  public void zkSetup() {
    srcZkServerWrapper = ServiceFactory.getZkServer();
    srcZkAddress = srcZkServerWrapper.getAddress();
    srcZkClient = ZkClientFactory.newZkClient(srcZkAddress);
    destZkServerWrapper = ServiceFactory.getZkServer();
    destZkAddress = destZkServerWrapper.getAddress();
    destZkClient = ZkClientFactory.newZkClient(destZkAddress);
    Assert.assertNotEquals(srcZkAddress, destZkAddress);
    createZkClientPaths(srcZkClient);
  }

  @AfterClass
  public void zkCleanup() {
    srcZkClient.deleteRecursively(BASE_PATH);
    srcZkClient.close();
    srcZkServerWrapper.close();
    destZkClient.deleteRecursively(BASE_PATH);
    destZkClient.close();
    destZkServerWrapper.close();
  }

  /** Test migrating metadata from local source ZK to local destination ZK using ZkCopier.migrateVenicePaths()*/
  @Test
  public void testMigrateVenicePaths() {
    List<String> destZkClientVenicePaths = ZkCopier.migrateVenicePaths(srcZkClient, destZkClient, CLUSTERS, BASE_PATH);
    testZkClientPathsAsserts(destZkClient);
    testVenicePathsAsserts(destZkClientVenicePaths);
  }

  private void createZkClientPaths(ZkClient zkClient) {
    zkClient.create(BASE_PATH, BASE_PATH, CreateMode.PERSISTENT);
    String storeConfigsPath = BASE_PATH + "/" + STORE_CONFIGS;
    zkClient.create(storeConfigsPath, STORE_CONFIGS, CreateMode.PERSISTENT);
    zkClient.create(storeConfigsPath + "/file", "file", CreateMode.PERSISTENT);
    for (String cluster: CLUSTERS) {
      String clusterPath = BASE_PATH + "/" + cluster;
      zkClient.create(clusterPath, cluster, CreateMode.PERSISTENT);
      for (String zkPath: CLUSTER_ZK_PATHS) {
        String clusterZkPath = clusterPath + "/" + zkPath;
        zkClient.create(clusterZkPath, zkPath, CreateMode.PERSISTENT);
        if (zkPath.equals(STORES)) {
          zkClient.create(clusterZkPath + "/testStore", "testStore", CreateMode.PERSISTENT);
        }
      }
    }
  }

  private void testZkClientPathsAsserts(ZkClient zkClient) {
    Assert.assertTrue(zkClient.exists(BASE_PATH));
    Assert.assertEquals(zkClient.readData(BASE_PATH), BASE_PATH);
    String storeConfigsPath = BASE_PATH + "/" + STORE_CONFIGS;
    Assert.assertTrue(zkClient.exists(storeConfigsPath));
    Assert.assertEquals(zkClient.readData(storeConfigsPath), STORE_CONFIGS);
    Assert.assertTrue(zkClient.exists(storeConfigsPath + "/file"));
    Assert.assertEquals(zkClient.readData(storeConfigsPath + "/file"), "file");
    for (String cluster: CLUSTERS) {
      String clusterPath = BASE_PATH + "/" + cluster;
      Assert.assertTrue(zkClient.exists(clusterPath));
      Assert.assertEquals(zkClient.readData(clusterPath), cluster);
      for (String zkPath: CLUSTER_ZK_PATHS) {
        String clusterZkPath = clusterPath + "/" + zkPath;
        Assert.assertTrue(zkClient.exists(clusterZkPath));
        Assert.assertEquals(zkClient.readData(clusterZkPath), zkPath);
        if (zkPath.equals(STORES)) {
          Assert.assertFalse(zkClient.exists(clusterZkPath + "/assertFalse"));
          Assert.assertTrue(zkClient.exists(clusterZkPath + "/testStore"));
          Assert.assertEquals(zkClient.readData(clusterZkPath + "/testStore"), "testStore");
        }
      }
    }
  }

  private void testVenicePathsAsserts(List<String> venicePaths) {
    Assert.assertEquals(venicePaths.size(), 19);
    Assert.assertTrue(venicePaths.contains(BASE_PATH));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + STORE_CONFIGS));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + STORE_CONFIGS + "/file"));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_1));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_1 + "/" + ADMIN_TOPIC_METADATA));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_1 + "/" + EXECUTION_IDS));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_1 + "/" + PARENT_OFFLINE_PUSHES));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_1 + "/" + ROUTERS));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_1 + "/" + STORE_GRAVEYARD));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_1 + "/" + STORES));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_1 + "/" + STORES + "/testStore"));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_2));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_2 + "/" + ADMIN_TOPIC_METADATA));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_2 + "/" + EXECUTION_IDS));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_2 + "/" + PARENT_OFFLINE_PUSHES));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_2 + "/" + ROUTERS));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_2 + "/" + STORE_GRAVEYARD));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_2 + "/" + STORES));
    Assert.assertTrue(venicePaths.contains(BASE_PATH + "/" + CLUSTER_2 + "/" + STORES + "/testStore"));
  }
}
