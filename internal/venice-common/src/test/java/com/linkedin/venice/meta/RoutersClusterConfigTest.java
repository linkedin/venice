package com.linkedin.venice.meta;

import org.testng.Assert;
import org.testng.annotations.Test;


public class RoutersClusterConfigTest {
  @Test
  public void testSetExpectNumber() {
    int expectedNumber = 100;
    RoutersClusterConfig config = new RoutersClusterConfig();
    config.setExpectedRouterCount(expectedNumber);
    Assert.assertEquals(
        config.getExpectedRouterCount(),
        expectedNumber,
        "Expected number should settled while creating the config object.");
  }

  @Test
  public void testUpdateClusterConfig() {
    int expectedNumber = 10;
    RoutersClusterConfig config = new RoutersClusterConfig();
    Assert.assertTrue(
        config.isMaxCapacityProtectionEnabled() && config.isThrottlingEnabled(),
        "By default all feature should be enabled for the cluster.");

    config.setMaxCapacityProtectionEnabled(false);
    Assert.assertFalse(config.isMaxCapacityProtectionEnabled());
    config.setThrottlingEnabled(false);
    Assert.assertFalse(config.isThrottlingEnabled());
  }
}
