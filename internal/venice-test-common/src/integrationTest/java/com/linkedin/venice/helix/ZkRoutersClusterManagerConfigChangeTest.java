package com.linkedin.venice.helix;

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


public class ZkRoutersClusterManagerConfigChangeTest {
  private ZkRoutersTestFixture fixture;

  @BeforeMethod
  public void setUp() {
    fixture = new ZkRoutersTestFixture();
    fixture.setUp();
  }

  @AfterMethod
  public void cleanUp() {
    fixture.close();
  }

  @Test
  public void testHandleRouterClusterConfigChange() {
    int port = 10555;
    String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    ZkRoutersClusterManager controller = fixture.createManager();
    ZkRoutersClusterManager router = fixture.createManager(new ZkClient(fixture.getZkServerWrapper().getAddress()));
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
  public void testRouterClusterConfigCreationWhenZNodeAlreadyExistsWithEmptyContent() {
    String myClusterName = Utils.getUniqueString("test-cluster");
    ZkRoutersClusterManager manager =
        new ZkRoutersClusterManager(fixture.getZkClient(), fixture.getAdapter(), myClusterName, 1, 1000);
    fixture.getZkClient().create(HelixUtils.getHelixClusterZkPath(myClusterName), null, CreateMode.PERSISTENT);
    fixture.getZkClient().create(manager.getRouterRootPath(), null, CreateMode.PERSISTENT);

    manager.refresh();
    Assert.assertNotNull(
        fixture.getZkClient().readData(manager.getRouterRootPath()),
        "Routers ZNode should not be null" + " after refresh");
  }
}
