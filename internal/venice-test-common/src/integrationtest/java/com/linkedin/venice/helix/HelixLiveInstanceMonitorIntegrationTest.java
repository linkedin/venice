package com.linkedin.venice.helix;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixLiveInstanceMonitorIntegrationTest {
  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String liveInstancePath = "/LIVEINSTANCES";
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();
  private HelixLiveInstanceMonitor helixLiveInstanceMonitor;

  @BeforeMethod
  public void zkSetup() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    zkClient.setZkSerializer(adapter);
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + liveInstancePath, null, CreateMode.PERSISTENT);
    helixLiveInstanceMonitor = new HelixLiveInstanceMonitor(zkClient, cluster);
  }

  @AfterMethod
  public void zkCleanup() {
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
    zkServerWrapper.close();
    helixLiveInstanceMonitor.clear();
  }

  @Test
  public void testRefresh() {
    Assert.assertTrue(
        helixLiveInstanceMonitor.getAllLiveInstances().isEmpty(),
        "getAllLiveInstances should return empty when there is no live instance");
    String liveNodeId = "localhost_1234";
    zkClient.create(clusterPath + liveInstancePath + "/" + liveNodeId, null, CreateMode.PERSISTENT);
    helixLiveInstanceMonitor.refresh();
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(helixLiveInstanceMonitor.getAllLiveInstances().size(), 1));
    Assert.assertTrue(helixLiveInstanceMonitor.isInstanceAlive(Instance.fromNodeId(liveNodeId)));
  }

  @Test
  public void testLiveInstanceChange() {
    helixLiveInstanceMonitor.refresh();
    Assert.assertTrue(
        helixLiveInstanceMonitor.getAllLiveInstances().isEmpty(),
        "getAllLiveInstances should return empty when there is no live instance");
    String liveNodeId1 = "localhost1_1234";
    String zkPathForLiveNodeId1 = clusterPath + liveInstancePath + "/" + liveNodeId1;
    String liveNodeId2 = "localhost2_1234";
    String zkPathForLiveNodeId2 = clusterPath + liveInstancePath + "/" + liveNodeId2;
    String liveNodeId3 = "localhost3_1234";
    String zkPathForLiveNodeId3 = clusterPath + liveInstancePath + "/" + liveNodeId3;
    String liveNodeId4 = "localhost4_1234";
    String zkPathForLiveNodeId4 = clusterPath + liveInstancePath + "/" + liveNodeId4;
    zkClient.create(zkPathForLiveNodeId1, null, CreateMode.PERSISTENT);
    zkClient.create(zkPathForLiveNodeId2, null, CreateMode.PERSISTENT);
    zkClient.create(zkPathForLiveNodeId3, null, CreateMode.PERSISTENT);
    zkClient.create(zkPathForLiveNodeId4, null, CreateMode.PERSISTENT);

    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(helixLiveInstanceMonitor.getAllLiveInstances().size(), 4));
    Assert.assertTrue(helixLiveInstanceMonitor.isInstanceAlive(Instance.fromNodeId(liveNodeId1)));
    Assert.assertTrue(helixLiveInstanceMonitor.isInstanceAlive(Instance.fromNodeId(liveNodeId2)));
    Assert.assertTrue(helixLiveInstanceMonitor.isInstanceAlive(Instance.fromNodeId(liveNodeId3)));
    Assert.assertTrue(helixLiveInstanceMonitor.isInstanceAlive(Instance.fromNodeId(liveNodeId4)));

    String deadNode1 = liveNodeId1;
    String zkPathForDeadNode1 = clusterPath + liveInstancePath + "/" + deadNode1;
    zkClient.delete(zkPathForDeadNode1);

    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(helixLiveInstanceMonitor.getAllLiveInstances().size(), 3));
    Assert.assertFalse(helixLiveInstanceMonitor.isInstanceAlive(Instance.fromNodeId(deadNode1)));
  }
}
