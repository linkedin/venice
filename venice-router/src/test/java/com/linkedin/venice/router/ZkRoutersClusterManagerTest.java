package com.linkedin.venice.router;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ZkRoutersClusterManagerTest {
  private ZkClient zkClient;
  private ZkServerWrapper zkServerWrapper;
  private String clusterName;

  @BeforeMethod
  public void setup() {
    clusterName = "ZkRoutersClusterManagerTest";
    zkServerWrapper = ServiceFactory.getZkServer();
    zkClient = new ZkClient(zkServerWrapper.getAddress());
  }

  @AfterMethod
  public void cleanup() {
    zkServerWrapper.close();
  }

  @Test
  public void testRegisterRouter() {
    int routersCount = 10;
    ZkRoutersClusterManager[] managers = new ZkRoutersClusterManager[routersCount];
    for (int i = 0; i < routersCount; i++) {
      int port = 10555 + i;
      String instanceId = Utils.getHelixNodeIdentifier(port);
      ZkRoutersClusterManager manager = new ZkRoutersClusterManager(zkClient, clusterName, instanceId);
      managers[i] = manager;
      manager.registerCurrentRouter();
      Assert.assertEquals(manager.getRoutersCount(), i + 1,
          "Router count should be updated immediately after being registered.");
    }

    // Ensure each router manager eventually get the correct router count.
    TestUtils.waitForNonDeterministicCompletion(1000, TimeUnit.MILLISECONDS, () -> {
      for (ZkRoutersClusterManager manager : managers) {
        if (manager.getRoutersCount() != routersCount) {
          return false;
        }
      }
      return true;
    });
  }

  @Test
  public void testRouterFailure() {
    int port = 10555;
    ZkRoutersClusterManager manager =
        new ZkRoutersClusterManager(zkClient, clusterName, Utils.getHelixNodeIdentifier(port));
    ZkClient failedZkClient = new ZkClient(zkServerWrapper.getAddress());
    ZkRoutersClusterManager failedManager =
        new ZkRoutersClusterManager(failedZkClient, clusterName, Utils.getHelixNodeIdentifier(port + 1));
    // Register two routers through different zk clients.
    manager.registerCurrentRouter();
    failedManager.registerCurrentRouter();
    // Eventually both manager wil get notification to update router count.
    TestUtils.waitForNonDeterministicCompletion(1000, TimeUnit.MILLISECONDS, () -> manager.getRoutersCount() == 2);
    TestUtils.waitForNonDeterministicCompletion(1000, TimeUnit.MILLISECONDS,
        () -> failedManager.getRoutersCount() == 2);
    // One router failed
    failedZkClient.close();
    // Router count should be updated eventually.
    TestUtils.waitForNonDeterministicCompletion(1000, TimeUnit.MILLISECONDS, () -> manager.getRoutersCount() == 1);
  }

  @Test
  public void testUnregisterRouter() {
    int port = 10555;
    String instanceId = Utils.getHelixNodeIdentifier(port);
    ZkRoutersClusterManager manager = new ZkRoutersClusterManager(zkClient, clusterName, instanceId);
    manager.registerCurrentRouter();
    Assert.assertEquals(manager.getRoutersCount(), 1,
        "Router count should be updated immediately after being registered.");
    manager.unregisterCurrentRouter();
    Assert.assertEquals(manager.getRoutersCount(), 0,
        "Router count should be updated immediately after being unregistered.");
  }
}
