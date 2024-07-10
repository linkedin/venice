package com.linkedin.venice.zk;

import static com.linkedin.venice.zk.VeniceZkPaths.ADMIN_TOPIC_METADATA;
import static com.linkedin.venice.zk.VeniceZkPaths.EXECUTION_IDS;
import static com.linkedin.venice.zk.VeniceZkPaths.PARENT_OFFLINE_PUSHES;
import static com.linkedin.venice.zk.VeniceZkPaths.ROUTERS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORES;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_CONFIGS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_GRAVEYARD;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import java.util.Arrays;
import java.util.HashSet;
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
  private final Set<String> CLUSTER_ZK_PATHS = new HashSet<>(
      Arrays.asList(ADMIN_TOPIC_METADATA, EXECUTION_IDS, PARENT_OFFLINE_PUSHES, ROUTERS, STORE_GRAVEYARD, STORES));

  @BeforeClass
  public void zkSetup() {
    srcZkServerWrapper = ServiceFactory.getZkServer();
    srcZkAddress = srcZkServerWrapper.getAddress();
    srcZkClient = ZkClientFactory.newZkClient(srcZkAddress);
    destZkServerWrapper = ServiceFactory.getZkServer();
    destZkAddress = destZkServerWrapper.getAddress();
    destZkClient = ZkClientFactory.newZkClient(destZkAddress);
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

  @Test
  public void testMigrateVeniceZKPaths() {
    String[] args = { "--migrate-venice-zk-paths", "--src-zookeeper-url", srcZkAddress, "--dest-zookeeper-url",
        destZkAddress, "--cluster-list", "cluster1, cluster2", "--base-path", BASE_PATH };
    try {
      AdminTool.main(args);
      testZkClientPathsAsserts(destZkClient);
    } catch (Exception e) {
      e.printStackTrace();
    }
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
}
