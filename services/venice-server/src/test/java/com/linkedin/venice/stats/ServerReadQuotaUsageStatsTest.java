package com.linkedin.venice.stats;

import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  @Test
  public void testGetReadQuotaMetricsWithNoVersionOrRecordings() {
    MetricsRepository metricsRepository = new MetricsRepository();
    String storeName = "test-store";
    int currentVersion = 3;
    int backupVersion = 2;
    ServerReadQuotaUsageStats stats = new ServerReadQuotaUsageStats(metricsRepository, storeName);
    // Stats shouldn't fail if the store don't have any versions yet
    Assert.assertEquals(stats.getVersionedRequestedQPS(backupVersion), 0d);
    Assert.assertEquals(stats.getVersionedRequestedQPS(currentVersion), 0d);
    Assert.assertEquals(stats.getVersionedRequestedKPS(backupVersion), 0d);
    Assert.assertEquals(stats.getVersionedRequestedKPS(currentVersion), 0d);
    Assert.assertEquals(stats.getReadQuotaUsageRatio(), Double.NaN);
    // Stats shouldn't fail if there are no recordings yet
    stats.setCurrentVersion(currentVersion);
    stats.setBackupVersion(backupVersion);
    Assert.assertEquals(stats.getVersionedRequestedQPS(backupVersion), 0d);
    Assert.assertEquals(stats.getVersionedRequestedQPS(currentVersion), 0d);
    Assert.assertEquals(stats.getVersionedRequestedKPS(backupVersion), 0d);
    Assert.assertEquals(stats.getVersionedRequestedKPS(currentVersion), 0d);
    Assert.assertEquals(stats.getReadQuotaUsageRatio(), Double.NaN);
    // The replica receives some assignment and traffic for current version
    stats.setNodeQuotaResponsibility(currentVersion, 1000);
    stats.recordAllowed(currentVersion, 100);
    Assert.assertTrue(stats.getReadQuotaUsageRatio() > 0);
    Assert.assertTrue(stats.getVersionedRequestedQPS(currentVersion) > 0);
    Assert.assertTrue(stats.getVersionedRequestedKPS(currentVersion) > 0);
  }

  /**
   * A non-thread safe map like Int2ObjectOpenHashMap could cause the threads to busy loop inside the internal find
   * method when a race condition happens
   */
  @Test(timeOut = 10 * Time.MS_PER_SECOND)
  public void testVersionedStatsThreadSafe() throws ExecutionException, InterruptedException, TimeoutException {
    MetricsRepository metricsRepository = new MetricsRepository();
    String storeName = "test-store";
    ServerReadQuotaUsageStats stats = new ServerReadQuotaUsageStats(metricsRepository, storeName);
    ExecutorService service =
        Executors.newFixedThreadPool(100, new DaemonThreadFactory("ServerReadQuotaUsageStatsTest"));
    CompletableFuture[] completableFutures = new CompletableFuture[100];
    for (int j = 0; j < 100; j++) {
      completableFutures[j] = CompletableFuture.runAsync(() -> {
        for (int i = 0; i < 100000; i++) {
          stats.recordAllowed(i, 1);
        }
      }, service);
    }
    CompletableFuture.allOf(completableFutures).get(10, TimeUnit.SECONDS);
  }
}
