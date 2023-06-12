package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ZkRoutersClusterManagerTest {
  private ZkClient zkClient;
  private ZkServerWrapper zkServerWrapper;
  private String clusterName;
  private HelixAdapterSerializer adapter;

  @BeforeMethod
  public void setUp() {
    clusterName = "ZkRoutersClusterManagerTest";
    zkServerWrapper = ServiceFactory.getZkServer();
    adapter = new HelixAdapterSerializer();
    zkClient = new ZkClient(zkServerWrapper.getAddress());
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testRegisterRouter() {
    int routersCount = 10;
    ZkRoutersClusterManager[] managers = new ZkRoutersClusterManager[routersCount];
    for (int i = 0; i < routersCount; i++) {
      int port = 10555 + i;
      String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
      ZkRoutersClusterManager manager = createManager(zkClient);
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
    ZkRoutersClusterManager manager = createManager(zkClient);
    ZkClient failedZkClient = new ZkClient(zkServerWrapper.getAddress());
    ZkRoutersClusterManager failedManager = createManager(failedZkClient);
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
    ZkRoutersClusterManager manager = createManager(zkClient);
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
    ZkRoutersClusterManager manager = createManager(zkClient);
    manager.registerRouter(instanceId);

    manager.enableThrottling(false);
    Assert.assertFalse(manager.isThrottlingEnabled(), "Throttling has been disabled in cluster level config.");
    manager.enableThrottling(true);
    Assert.assertTrue(manager.isThrottlingEnabled(), "Throttling has been enable in cluster level config.");
  }

  @Test
  public void testUPdateExpectRouterCount() {
    int port = 10555;
    String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    ZkRoutersClusterManager manager = createManager(zkClient);
    manager.registerRouter(instanceId);
    int expectRouterNumber = -1;
    try {
      manager.updateExpectedRouterCount(expectRouterNumber);
      Assert.fail("Invalid expect router count.");
    } catch (VeniceException e) {
      // expected
    }
    expectRouterNumber = 100;
    manager.updateExpectedRouterCount(expectRouterNumber);
    Assert.assertEquals(
        manager.getExpectedRoutersCount(),
        expectRouterNumber,
        "Expect router count should be updated before.");
  }

  @Test
  public void testEnableMaxCapacityProtection() {
    int port = 10555;
    String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    ZkRoutersClusterManager manager = createManager(zkClient);
    manager.registerRouter(instanceId);

    manager.enableMaxCapacityProtection(false);
    Assert.assertFalse(
        manager.isMaxCapacityProtectionEnabled(),
        "Router protection has been disabled in cluster level config.");
    manager.enableMaxCapacityProtection(true);
    Assert.assertTrue(
        manager.isMaxCapacityProtectionEnabled(),
        "Router protection has been enabled in cluster level config.");
  }

  @Test
  public void testHandleRouterClusterConfigChange() {
    int port = 10555;
    String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    ZkRoutersClusterManager controller = createManager(zkClient);
    ZkRoutersClusterManager router = createManager(new ZkClient(zkServerWrapper.getAddress()));
    router.registerRouter(instanceId);

    int expectedNumber = 100;
    controller.updateExpectedRouterCount(expectedNumber);
    // The controller will know the new router is added.
    TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> controller.getLiveRoutersCount() == 1);
    // The router will know the expected number is updated.
    TestUtils.waitForNonDeterministicCompletion(
        1,
        TimeUnit.SECONDS,
        () -> router.getExpectedRoutersCount() == expectedNumber);

    controller.enableThrottling(false);
    controller.enableMaxCapacityProtection(false);
    // The router will know the throttling and router protection are disabled.
    TestUtils.waitForNonDeterministicCompletion(
        1,
        TimeUnit.SECONDS,
        () -> !router.isThrottlingEnabled() && !router.isMaxCapacityProtectionEnabled());
  }

  @Test
  public void testTriggerRouterClusterConfigChangedEvent() {
    int port = 10555;
    String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    ZkRoutersClusterManager manager = createManager(zkClient);
    manager.registerRouter(instanceId);
    int expectedNumber = 100;

    boolean[] isUpdated = new boolean[1];
    isUpdated[0] = false;
    manager.subscribeRouterClusterConfigChangedEvent(newConfig -> {
      if (newConfig.getExpectedRouterCount() == expectedNumber || newConfig.isThrottlingEnabled() == false) {
        isUpdated[0] = true;
      }
    });

    manager.updateExpectedRouterCount(expectedNumber);

    TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> isUpdated[0]);

    // Trigger event because throttling feature flag was changed.
    isUpdated[0] = false;
    manager.enableThrottling(false);
    TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> isUpdated[0]);
  }

  private ZkRoutersClusterManager createManager(ZkClient zkClient) {
    ZkRoutersClusterManager manager = new ZkRoutersClusterManager(zkClient, adapter, clusterName, 1, 1000);
    manager.refresh();
    manager.createRouterClusterConfig();
    return manager;
  }

  @Test
  public void testRouterClusterConfigCreationWhenZNodeAlreadyExistsWithEmptyContent() {
    String myClusterName = Utils.getUniqueString("test-cluster");
    ZkRoutersClusterManager manager = new ZkRoutersClusterManager(zkClient, adapter, myClusterName, 1, 1000);
    zkClient.create(HelixUtils.getHelixClusterZkPath(myClusterName), null, CreateMode.PERSISTENT);
    zkClient.create(manager.getRouterRootPath(), null, CreateMode.PERSISTENT);

    manager.refresh();
    Assert.assertNotNull(
        zkClient.readData(manager.getRouterRootPath()),
        "Routers ZNode should not be null" + " after refresh");
  }
}
