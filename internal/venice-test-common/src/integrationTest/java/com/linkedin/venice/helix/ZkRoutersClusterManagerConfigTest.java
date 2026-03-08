package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ZkRoutersClusterManagerConfigTest {
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
  public void testUpdateExpectRouterCount() {
    int port = 10555;
    String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), port);
    ZkRoutersClusterManager manager = fixture.createManager();
    manager.registerRouter(instanceId);
    Assert.assertEquals(manager.getLiveRouterInstances().size(), 1);
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
    ZkRoutersClusterManager manager = fixture.createManager();
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

}
