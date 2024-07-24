package com.linkedin.venice.zk;

import static com.linkedin.venice.zk.VeniceZkPaths.CLUSTER_ZK_PATHS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORES;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_CONFIGS;

import com.linkedin.venice.ZkCopier;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.zookeeper.datamodel.serializer.ByteArraySerializer;
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
    ByteArraySerializer serializer = new ByteArraySerializer();
    srcZkClient.setZkSerializer(serializer);
    destZkClient.setZkSerializer(serializer);
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
    ZkCopier.migrateVenicePaths(srcZkClient, destZkClient, CLUSTERS, BASE_PATH);
    testZkClientPathsAsserts(destZkClient);
  }

  private void createZkClientPaths(ZkClient zkClient) {
    zkClient.create(BASE_PATH, BASE_PATH.getBytes(), CreateMode.PERSISTENT);
    String storeConfigsPath = BASE_PATH + "/" + STORE_CONFIGS;
    zkClient.create(storeConfigsPath, STORE_CONFIGS.getBytes(), CreateMode.PERSISTENT);
    zkClient.create(storeConfigsPath + "/file", "file".getBytes(), CreateMode.PERSISTENT);
    for (String cluster: CLUSTERS) {
      String clusterPath = BASE_PATH + "/" + cluster;
      zkClient.create(clusterPath, cluster.getBytes(), CreateMode.PERSISTENT);
      for (String zkPath: CLUSTER_ZK_PATHS) {
        String clusterZkPath = clusterPath + "/" + zkPath;
        zkClient.create(clusterZkPath, zkPath.getBytes(), CreateMode.PERSISTENT);
        if (zkPath.equals(STORES)) {
          zkClient.create(clusterZkPath + "/testStore", "testStore".getBytes(), CreateMode.PERSISTENT);
        }
      }
    }
  }

  private void testZkClientPathsAsserts(ZkClient zkClient) {
    Assert.assertTrue(zkClient.exists(BASE_PATH));
    Assert.assertEquals(zkClient.readData(BASE_PATH), BASE_PATH.getBytes());
    String storeConfigsPath = BASE_PATH + "/" + STORE_CONFIGS;
    Assert.assertTrue(zkClient.exists(storeConfigsPath));
    Assert.assertEquals(zkClient.readData(storeConfigsPath), STORE_CONFIGS.getBytes());
    Assert.assertTrue(zkClient.exists(storeConfigsPath + "/file"));
    Assert.assertEquals(zkClient.readData(storeConfigsPath + "/file"), "file".getBytes());
    for (String cluster: CLUSTERS) {
      String clusterPath = BASE_PATH + "/" + cluster;
      Assert.assertTrue(zkClient.exists(clusterPath));
      Assert.assertEquals(zkClient.readData(clusterPath), cluster.getBytes());
      for (String zkPath: CLUSTER_ZK_PATHS) {
        String clusterZkPath = clusterPath + "/" + zkPath;
        Assert.assertTrue(zkClient.exists(clusterZkPath));
        Assert.assertEquals(zkClient.readData(clusterZkPath), zkPath.getBytes());
        if (zkPath.equals(STORES)) {
          Assert.assertFalse(zkClient.exists(clusterZkPath + "/assertFalse"));
          Assert.assertTrue(zkClient.exists(clusterZkPath + "/testStore"));
          Assert.assertEquals(zkClient.readData(clusterZkPath + "/testStore"), "testStore".getBytes());
        }
      }
    }
  }
}
