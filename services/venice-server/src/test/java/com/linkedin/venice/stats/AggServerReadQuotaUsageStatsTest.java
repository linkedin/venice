package com.linkedin.venice.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.utils.Time;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggServerReadQuotaUsageStatsTest {
  @Test
  public void testAggServerQuotaUsageStats() {
    Time mockTime = mock(Time.class);
    long start = System.currentTimeMillis();
    doReturn(start).when(mockTime).milliseconds();
    MetricsRepository metricsRepository = new MetricsRepository();
    AggServerQuotaUsageStats aggServerQuotaUsageStats = new AggServerQuotaUsageStats("test_cluster", metricsRepository);
    String storeName = "testStore";
    String storeName2 = "testStore2";
    String currentReadQuotaRequestedQPSString = "." + storeName + "--current_quota_request.Gauge";
    String currentReadQuotaRequestedKPSString = "." + storeName + "--current_quota_request_key_count.Gauge";
    String backupReadQuotaRequestedQPSString = "." + storeName + "--backup_quota_request.Gauge";
    String backupReadQuotaRequestedKPSString = "." + storeName + "--backup_quota_request_key_count.Gauge";
    String quotaUsageRatio = "." + storeName + "--quota_requested_usage_ratio.Gauge";
    String readQuotaRequestedQPSString2 = "." + storeName2 + "--current_quota_request.Gauge";
    String readQuotaRequestedKPSString2 = "." + storeName2 + "--current_quota_request_key_count.Gauge";
    String totalReadQuotaRequestedQPSString = ".total--current_quota_request.Gauge";
    String totalReadQuotaRequestedKPSString = ".total--current_quota_request_key_count.Gauge";
    long batchSize = 100;
    long batchSize2 = 200;
    aggServerQuotaUsageStats.setBackupVersion(storeName, 1);
    aggServerQuotaUsageStats.setCurrentVersion(storeName, 2);
    aggServerQuotaUsageStats.setCurrentVersion(storeName2, 1);
    aggServerQuotaUsageStats.recordAllowed(storeName, 1, batchSize);
    aggServerQuotaUsageStats.recordAllowed(storeName, 2, batchSize);
    aggServerQuotaUsageStats.recordAllowed(storeName, 2, batchSize);
    aggServerQuotaUsageStats.recordAllowed(storeName2, 1, batchSize2);
    aggServerQuotaUsageStats.setNodeQuotaResponsibility(storeName, 1, 100);
    aggServerQuotaUsageStats.setNodeQuotaResponsibility(storeName, 2, 200);

    // Rate metric is amortized over a 30s window
    Assert.assertEquals(metricsRepository.getMetric(currentReadQuotaRequestedQPSString).value(), 2d / 30d, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(backupReadQuotaRequestedQPSString).value(), 1d / 30d, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRequestedQPSString2).value(), 1d / 30d, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(currentReadQuotaRequestedKPSString).value(), 200d / 30d, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(backupReadQuotaRequestedKPSString).value(), 100d / 30d, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRequestedKPSString2).value(), 200d / 30d, 0.05);
    double totalQPS = 4d / 30d;
    double totalKPS = (batchSize2 + batchSize * 3) / 30d;
    Assert.assertEquals(metricsRepository.getMetric(totalReadQuotaRequestedQPSString).value(), totalQPS, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(totalReadQuotaRequestedKPSString).value(), totalKPS, 0.1);
    Assert.assertEquals(metricsRepository.getMetric(quotaUsageRatio).value(), (200d / 30d) / 200d, 0.01);

    String readQuotaRejectedQPSString = "." + storeName + "--quota_rejected_request.Rate";
    String readQuotaRejectedKPSString = "." + storeName + "--quota_rejected_key_count.Rate";
    String readQuotaRejectedQPSString2 = "." + storeName2 + "--quota_rejected_request.Rate";
    String readQuotaRejectedKPSString2 = "." + storeName2 + "--quota_rejected_key_count.Rate";
    String totalReadQuotaRejectedQPSString = ".total--quota_rejected_request.Rate";
    String totalReadQuotaRejectedKPSString = ".total--quota_rejected_key_count.Rate";
    aggServerQuotaUsageStats.recordRejected(storeName, 1, batchSize);
    aggServerQuotaUsageStats.recordRejected(storeName2, 1, batchSize2);
    aggServerQuotaUsageStats.recordRejected(storeName2, 1, batchSize2);

    Assert.assertEquals(metricsRepository.getMetric(readQuotaRejectedQPSString).value(), 1d / 30d, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRejectedQPSString2).value(), 2d / 30d, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRejectedKPSString).value(), 100d / 30d, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRejectedKPSString2).value(), 400d / 30d, 0.05);
    double totalRejectedQPS = metricsRepository.getMetric(readQuotaRejectedQPSString).value()
        + metricsRepository.getMetric(readQuotaRejectedQPSString2).value();
    double totalRejectedKPS = metricsRepository.getMetric(readQuotaRejectedKPSString).value()
        + metricsRepository.getMetric(readQuotaRejectedKPSString2).value();

    Assert.assertEquals(metricsRepository.getMetric(totalReadQuotaRejectedQPSString).value(), totalRejectedQPS, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(totalReadQuotaRejectedKPSString).value(), totalRejectedKPS, 0.05);
  }
}
