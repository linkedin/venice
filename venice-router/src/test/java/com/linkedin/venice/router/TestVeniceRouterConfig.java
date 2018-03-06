package com.linkedin.venice.router;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceRouterConfig {

  @Test
  public void testParseRetryThresholdForBatchGet() {
    String retryThresholdConfig = "1-10:20,11-50:50,51-200:80,201-:1000";
    TreeMap<Integer, Integer> retryThresholdMap = VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
    Assert.assertEquals(retryThresholdMap.get(1), new Integer(20));
    Assert.assertEquals(retryThresholdMap.get(11), new Integer(50));
    Assert.assertEquals(retryThresholdMap.get(51), new Integer(80));
    Assert.assertEquals(retryThresholdMap.get(201), new Integer(1000));

    Assert.assertEquals(retryThresholdMap.floorEntry(1).getValue(), new Integer(20));
    Assert.assertEquals(retryThresholdMap.floorEntry(30).getValue(), new Integer(50));
    Assert.assertEquals(retryThresholdMap.floorEntry(500).getValue(), new Integer(1000));

    // Config with un-ordered range
    String unorderedRetryThresholdConfig = "51-200:80,11-50:50,201-:1000,1-10:20";
    retryThresholdMap = VeniceRouterConfig.parseRetryThresholdForBatchGet(unorderedRetryThresholdConfig);
    Assert.assertEquals(retryThresholdMap.get(1), new Integer(20));
    Assert.assertEquals(retryThresholdMap.get(11), new Integer(50));
    Assert.assertEquals(retryThresholdMap.get(51), new Integer(80));
    Assert.assertEquals(retryThresholdMap.get(201), new Integer(1000));
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithKeyRangeGap() {
    String retryThresholdConfig = "1-10:20,51-200:80,201-:1000";
    VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithWithInvalidFormat() {
    String retryThresholdConfig = "1-10:20,11-50:50,51-:80,201-:1000";
    VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithInvalidSeparator() {
    String retryThresholdConfig = "1-10:20,11-50:50,51-200::80,201-:1000";
    VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithoutStartingFrom1() {
    String retryThresholdConfig = "2-10:20,11-50:50,51-200::80,201-:1000";
    VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testParseRetryThresholdForBatchGetWithoutUnlimitedKeyCount() {
    String retryThresholdConfig = "2-10:20,11-50:50,51-200::80,201-500:1000";
    VeniceRouterConfig.parseRetryThresholdForBatchGet(retryThresholdConfig);
  }
}
