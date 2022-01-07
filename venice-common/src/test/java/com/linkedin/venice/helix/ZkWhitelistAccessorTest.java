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


public class ZkWhitelistAccessorTest {
  private ZkClient zkClient;
  private ZkWhitelistAccessor accessor;
  private ZkServerWrapper zkServerWrapper;
  private int port = 1234;
  private String cluster = "whitelistcluster";

  @BeforeMethod
  public void setUp() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zkServerWrapper.getAddress());
    accessor = new ZkWhitelistAccessor(zkClient, new HelixAdapterSerializer());
    zkClient.createPersistent(HelixUtils.getHelixClusterZkPath(cluster), false);
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.deleteRecursively(HelixUtils.getHelixClusterZkPath(cluster));
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testAddInstanceIntoWhitelist() {
    String helixNodeId= Utils.getHelixNodeIdentifier(port);
    Assert.assertFalse(accessor.isInstanceInWhitelist(cluster, helixNodeId),
        "Instance has not been added into the white list.");
    accessor.addInstanceToWhiteList(cluster, helixNodeId);
    Assert.assertTrue(accessor.isInstanceInWhitelist(cluster, helixNodeId),
        "Instance should have been added into the white list.");
  }

  @Test
  public void testRemoveInstanceFromWhitelist() {
    String helixNodeId = Utils.getHelixNodeIdentifier(port);
    accessor.removeInstanceFromWhiteList(cluster, helixNodeId);
    accessor.addInstanceToWhiteList(cluster, helixNodeId);
    Assert.assertTrue(accessor.isInstanceInWhitelist(cluster, helixNodeId),
        "Instance should have been added into the white list.");

    accessor.removeInstanceFromWhiteList(cluster, helixNodeId);
    Assert.assertFalse(accessor.isInstanceInWhitelist(cluster, helixNodeId),
        "Instance should have been removed from the white list.");
  }

  @Test
  public void testGetWhitelist() {
    Assert.assertFalse(accessor.isInstanceInWhitelist(cluster, Utils.getHelixNodeIdentifier(port)));
    Assert.assertTrue(accessor.getWhiteList(cluster).isEmpty(), "white list should be empty.");
    int instanceCount = 3;
    for (int i = 0; i < instanceCount; i++) {
      accessor.addInstanceToWhiteList(cluster, Utils.getHelixNodeIdentifier(port + i));
    }

    Assert.assertEquals(accessor.getWhiteList(cluster).size(), instanceCount);
  }
}
