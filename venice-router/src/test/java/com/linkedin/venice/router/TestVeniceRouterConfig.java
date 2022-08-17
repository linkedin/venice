package com.linkedin.venice.router;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceRouterConfig {
  @Test
  public void testParseRetryThresholdForBatchGet() {
    String retryThresholdConfig = "1-10:20,11-50:50,51-200:80,201-:1000";
    TreeMap<Integer, Integer> retryThresholdMap =
        VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
    Assert.assertEquals((int) retryThresholdMap.get(1), 20);
    Assert.assertEquals((int) retryThresholdMap.get(11), 50);
    Assert.assertEquals((int) retryThresholdMap.get(51), 80);
    Assert.assertEquals((int) retryThresholdMap.get(201), 1000);

    Assert.assertEquals((int) retryThresholdMap.floorEntry(1).getValue(), 20);
    Assert.assertEquals((int) retryThresholdMap.floorEntry(30).getValue(), 50);
    Assert.assertEquals((int) retryThresholdMap.floorEntry(500).getValue(), 1000);

    // Config with un-ordered range
    String unorderedRetryThresholdConfig = "51-200:80,11-50:50,201-:1000,1-10:20";
    retryThresholdMap = VeniceRouterConfig.parseRetryThresholdForBatchGet(unorderedRetryThresholdConfig);
    Assert.assertEquals((int) retryThresholdMap.get(1), 20);
    Assert.assertEquals((int) retryThresholdMap.get(11), 50);
    Assert.assertEquals((int) retryThresholdMap.get(51), 80);
    Assert.assertEquals((int) retryThresholdMap.get(201), 1000);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithKeyRangeGap() {
    String retryThresholdConfig = "1-10:20,51-200:80,201-:1000";
    VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithWithInvalidFormat() {
    String retryThresholdConfig = "1-10:20,11-50:50,51-:80,201-:1000";
    VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithInvalidSeparator() {
    String retryThresholdConfig = "1-10:20,11-50:50,51-200::80,201-:1000";
    VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithoutStartingFrom1() {
    String retryThresholdConfig = "2-10:20,11-50:50,51-200::80,201-:1000";
    VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithoutUnlimitedKeyCount() {
    String retryThresholdConfig = "2-10:20,11-50:50,51-200::80,201-500:1000";
    VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }
}
