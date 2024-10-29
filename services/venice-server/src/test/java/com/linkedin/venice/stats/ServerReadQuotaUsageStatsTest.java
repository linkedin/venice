package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ServerReadQuotaUsageStatsTest {
  @Test
  public void testGetReadQuotaUsageRatio() {
    MetricsRepository metricsRepository = new MetricsRepository();
    String storeName = "test-store";
    ServerReadQuotaUsageStats stats = new ServerReadQuotaUsageStats(metricsRepository, storeName);
    stats.setCurrentVersion(1);
    stats.setNodeQuotaResponsibility(1, 1000);
    stats.recordAllowed(1, 10000);
    double usageRatio = metricsRepository.getMetric(".test-store--quota_requested_usage_ratio.Gauge").value();
    Assert.assertEquals(usageRatio, (10000d / 30d) / 1000, 0.001);
    stats.setNodeQuotaResponsibility(2, 100);
    // Ensure quota usage don't just 2x when node responsibility changes on other versions
    Assert.assertTrue(
        metricsRepository.getMetric(".test-store--quota_requested_usage_ratio.Gauge").value() <= usageRatio);
  }
}
