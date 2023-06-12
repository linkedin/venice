package com.linkedin.venice.helix;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ZkAllowlistAccessorTest {
  private ZkClient zkClient;
  private ZkAllowlistAccessor accessor;
  private ZkServerWrapper zkServerWrapper;
  private int port = 1234;
  private String cluster = "allowlistcluster";

  @BeforeMethod
  public void setUp() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zkServerWrapper.getAddress());
    accessor = new ZkAllowlistAccessor(zkClient, new HelixAdapterSerializer());
    zkClient.createPersistent(HelixUtils.getHelixClusterZkPath(cluster), false);
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.deleteRecursively(HelixUtils.getHelixClusterZkPath(cluster));
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testAddInstanceIntoAllowlist() {
    String helixNodeId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    Assert.assertFalse(
        accessor.isInstanceInAllowlist(cluster, helixNodeId),
        "Instance has not been added into the allowlist.");
    accessor.addInstanceToAllowList(cluster, helixNodeId);
    Assert.assertTrue(
        accessor.isInstanceInAllowlist(cluster, helixNodeId),
        "Instance should have been added into the allowlist.");
  }

  @Test
  public void testRemoveInstanceFromAllowlist() {
    String helixNodeId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    accessor.removeInstanceFromAllowList(cluster, helixNodeId);
    accessor.addInstanceToAllowList(cluster, helixNodeId);
    Assert.assertTrue(
        accessor.isInstanceInAllowlist(cluster, helixNodeId),
        "Instance should have been added into the allowlist.");

    accessor.removeInstanceFromAllowList(cluster, helixNodeId);
    Assert.assertFalse(
        accessor.isInstanceInAllowlist(cluster, helixNodeId),
        "Instance should have been removed from the allowlist.");
  }

  @Test
  public void testGetAllowlist() {
    Assert
        .assertFalse(accessor.isInstanceInAllowlist(cluster, Utils.getHelixNodeIdentifier(Utils.getHostName(), port)));
    Assert.assertTrue(accessor.getAllowList(cluster).isEmpty(), "allowlist should be empty.");
    int instanceCount = 3;
    for (int i = 0; i < instanceCount; i++) {
      accessor.addInstanceToAllowList(cluster, Utils.getHelixNodeIdentifier(Utils.getHostName(), port + i));
    }

    Assert.assertEquals(accessor.getAllowList(cluster).size(), instanceCount);
  }
}
