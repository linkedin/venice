package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ServerQuotaUsageStatsTest {
  @Test
  public void testGetReadQuotaUsageRatio() {
    MetricsRepository metricsRepository = new MetricsRepository();
    ServerQuotaUsageStats stats = new ServerQuotaUsageStats(metricsRepository, "test-store");
    stats.setNodeQuotaResponsibility(1000);
    stats.recordAllowed(10000);
    double usageRatio = metricsRepository.getMetric(".test-store--quota_requested_usage_ratio.Gauge").value();
    stats.setNodeQuotaResponsibility(500);
    stats.recordAllowed(1);
    // Ensure quota usage don't just 2x when node responsibility changes without any actual traffic spike
    Assert.assertTrue(
        metricsRepository.getMetric(".test-store--quota_requested_usage_ratio.Gauge").value() <= usageRatio);
    Assert.assertEquals(metricsRepository.getMetric(".test-store--quota_node_responsibility_kps.Gauge").value(), 500d);
  }
}
