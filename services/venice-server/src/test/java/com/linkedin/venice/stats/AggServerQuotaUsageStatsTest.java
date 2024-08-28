package com.linkedin.venice.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.throttle.TokenBucket;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.utils.Time;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggServerQuotaUsageStatsTest {
  @Test
  public void testAggServerQuotaUsageStats() {
    Time mockTime = mock(Time.class);
    long start = System.currentTimeMillis();
    doReturn(start).when(mockTime).milliseconds();
    MetricsRepository metricsRepository = new MetricsRepository();
    AggServerQuotaUsageStats aggServerQuotaUsageStats = new AggServerQuotaUsageStats(metricsRepository);
    String storeName = "testStore";
    String storeName2 = "testStore2";
    String readQuotaRequestedQPSString = "." + storeName + "--quota_request.Rate";
    String readQuotaRequestedKPSString = "." + storeName + "--quota_request_key_count.Rate";
    String readQuotaRequestedQPSString2 = "." + storeName2 + "--quota_request.Rate";
    String readQuotaRequestedKPSString2 = "." + storeName2 + "--quota_request_key_count.Rate";
    String totalReadQuotaRequestedQPSString = ".total--quota_request.Rate";
    String totalReadQuotaRequestedKPSString = ".total--quota_request_key_count.Rate";
    long batchSize = 100;
    long batchSize2 = 200;
    aggServerQuotaUsageStats.recordAllowed(storeName, batchSize);
    aggServerQuotaUsageStats.recordAllowed(storeName, batchSize);
    aggServerQuotaUsageStats.recordAllowed(storeName2, batchSize2);

    // Rate metric is amortized over a 30s window
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRequestedQPSString).value(), 2d / 30d, 0.01);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRequestedQPSString2).value(), 1d / 30d, 0.01);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRequestedKPSString).value(), 200d / 30d, 0.01);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRequestedKPSString2).value(), 200d / 30d, 0.01);
    double totalQPS = metricsRepository.getMetric(readQuotaRequestedQPSString).value()
        + metricsRepository.getMetric(readQuotaRequestedQPSString2).value();
    double totalKPS = metricsRepository.getMetric(readQuotaRequestedKPSString).value()
        + metricsRepository.getMetric(readQuotaRequestedKPSString2).value();
    Assert.assertEquals(metricsRepository.getMetric(totalReadQuotaRequestedQPSString).value(), totalQPS, 0.01);
    Assert.assertEquals(metricsRepository.getMetric(totalReadQuotaRequestedKPSString).value(), totalKPS, 0.01);

    String readQuotaRejectedQPSString = "." + storeName + "--quota_rejected_request.Rate";
    String readQuotaRejectedKPSString = "." + storeName + "--quota_rejected_key_count.Rate";
    String readQuotaRejectedQPSString2 = "." + storeName2 + "--quota_rejected_request.Rate";
    String readQuotaRejectedKPSString2 = "." + storeName2 + "--quota_rejected_key_count.Rate";
    String totalReadQuotaRejectedQPSString = ".total--quota_rejected_request.Rate";
    String totalReadQuotaRejectedKPSString = ".total--quota_rejected_key_count.Rate";
    aggServerQuotaUsageStats.recordRejected(storeName, batchSize);
    aggServerQuotaUsageStats.recordRejected(storeName2, batchSize2);
    aggServerQuotaUsageStats.recordRejected(storeName2, batchSize2);

    Assert.assertEquals(metricsRepository.getMetric(readQuotaRejectedQPSString).value(), 1d / 30d, 0.01);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRejectedQPSString2).value(), 2d / 30d, 0.01);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRejectedKPSString).value(), 100d / 30d, 0.01);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaRejectedKPSString2).value(), 400d / 30d, 0.01);
    double totalRejectedQPS = metricsRepository.getMetric(readQuotaRejectedQPSString).value()
        + metricsRepository.getMetric(readQuotaRejectedQPSString2).value();
    double totalRejectedKPS = metricsRepository.getMetric(readQuotaRejectedKPSString).value()
        + metricsRepository.getMetric(readQuotaRejectedKPSString2).value();
    Assert.assertEquals(metricsRepository.getMetric(totalReadQuotaRejectedQPSString).value(), totalRejectedQPS, 0.05);
    Assert.assertEquals(metricsRepository.getMetric(totalReadQuotaRejectedKPSString).value(), totalRejectedKPS, 0.05);

    String readQuotaUsageRatioString = "." + storeName + "--quota_requested_usage_ratio.Gauge";
    TokenBucket mockTokenBucket = mock(TokenBucket.class);
    double expectedUsageRatio = 0.5;
    doReturn(expectedUsageRatio).when(mockTokenBucket).getStaleUsageRatio();

    Assert.assertEquals(metricsRepository.getMetric(readQuotaUsageRatioString).value(), -1d);
    aggServerQuotaUsageStats.setStoreTokenBucket(storeName, mockTokenBucket);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaUsageRatioString).value(), expectedUsageRatio);
    aggServerQuotaUsageStats.setStoreTokenBucket(storeName, null);
    Assert.assertEquals(metricsRepository.getMetric(readQuotaUsageRatioString).value(), -1d);
  }
}
