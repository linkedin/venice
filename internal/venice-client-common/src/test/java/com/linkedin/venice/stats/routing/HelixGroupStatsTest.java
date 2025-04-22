package com.linkedin.venice.stats.routing;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.Test;


public class HelixGroupStatsTest {
  @Test
  public void testMetrics() {
    MetricsRepository metricsRepository = new MetricsRepository();
    String storeName = "test_store";

    HelixGroupStats stats = new HelixGroupStats(metricsRepository, storeName);

    // No data points
    assertEquals(stats.getGroupResponseWaitingTimeAvg(0), -1d);

    int testGroupId = 1;
    stats.recordGroupNum(3);
    stats.recordGroupRequest(testGroupId);
    stats.recordGroupPendingRequest(testGroupId, 2);
    stats.recordGroupResponseWaitingTime(testGroupId, 10);

    assertTrue(stats.getGroupResponseWaitingTimeAvg(testGroupId) > 0);
  }
}
