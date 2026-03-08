package com.linkedin.venice.helix;

import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ZkRoutersClusterManagerTest {
  private ZkRoutersTestFixture fixture;

  @BeforeMethod
  public void setUp() {
    fixture = new ZkRoutersTestFixture();
    fixture.setUp();
  }

  @AfterMethod
  public void cleanUp() {
    fixture.tearDown();
  }

  @Test
  public void testRegisterRouter() {
    int routersCount = 10;
    ZkRoutersClusterManager[] managers = new ZkRoutersClusterManager[routersCount];
    for (int i = 0; i < routersCount; i++) {
      int port = 10555 + i;
      String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
      ZkRoutersClusterManager manager = fixture.createManager();
      managers[i] = manager;
      manager.registerRouter(instanceId);
      Assert.assertEquals(
          manager.getLiveRoutersCount(),
          i + 1,
          "Router count should be updated immediately after being registered.");
    }

    // Ensure each router manager eventually get the correct router count. And get the correct router cluster config.
    TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> {
      for (ZkRoutersClusterManager manager: managers) {
        if (manager.getLiveRoutersCount() != routersCount) {
          return false;
        }
      }
      return true;
    });
  }

  @Test
  public void testRouterFailure() {
    int port = 10555;
    ZkRoutersClusterManager manager = fixture.createManager();
    ZkClient failedZkClient = new ZkClient(fixture.getZkServerWrapper().getAddress());
    ZkRoutersClusterManager failedManager = fixture.createManager(failedZkClient);
    // Register two routers through different zk clients.
    manager.registerRouter(Utils.getHelixNodeIdentifier(Utils.getHostName(), port));
    failedManager.registerRouter(Utils.getHelixNodeIdentifier(Utils.getHostName(), port + 1));
    // Eventually both manager wil get notification to update router count.
    TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> manager.getLiveRoutersCount() == 2);
    TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> failedManager.getLiveRoutersCount() == 2);
    // One router failed
    failedZkClient.close();
    // Router count should be updated eventually.
    TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> manager.getLiveRoutersCount() == 1);
  }

  @Test
  public void testUnregisterLiveOuter() {
    int port = 10555;
    String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    ZkRoutersClusterManager manager = fixture.createManager();
    manager.registerRouter(instanceId);
    Assert.assertEquals(
        manager.getLiveRoutersCount(),
        1,
        "Router count should be updated immediately after being registered.");
    manager.unregisterRouter(instanceId);
    Assert.assertEquals(
        manager.getLiveRoutersCount(),
        0,
        "Router count should be updated immediately after being unregistered.");
  }

  @Test
  public void testEnableThrottling() {
    int port = 10555;
    String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    ZkRoutersClusterManager manager = fixture.createManager();
    manager.registerRouter(instanceId);

    manager.enableThrottling(false);
    Assert.assertFalse(manager.isThrottlingEnabled(), "Throttling has been disabled in cluster level config.");
    manager.enableThrottling(true);
    Assert.assertTrue(manager.isThrottlingEnabled(), "Throttling has been enable in cluster level config.");
  }
}
