package com.linkedin.davinci.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import org.rocksdb.Cache;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RocksDBMemoryStatsTest {
  private AsyncGauge.AsyncGaugeExecutor executor;
  private MetricsRepository metricsRepository;

  @BeforeMethod
  public void setUp() {
    // Use a dedicated executor to avoid contention with the shared DEFAULT_ASYNC_GAUGE_EXECUTOR in CI
    executor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    metricsRepository = new MetricsRepository(new MetricConfig(executor));
  }

  @AfterMethod
  public void tearDown() throws Exception {
    metricsRepository.close();
    executor.close();
  }

  @Test
  public void testSetRMDBlockCacheRegistersGauges() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, "test_store", false);

    Cache mockCache = mock(Cache.class);
    when(mockCache.getUsage()).thenReturn(512L);
    when(mockCache.getPinnedUsage()).thenReturn(256L);

    stats.setRMDBlockCache(mockCache, 1024L);

    Assert.assertEquals(
        metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-capacity.Gauge").value(),
        1024.0);
    Assert.assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-usage.Gauge").value(), 512.0);
    Assert.assertEquals(
        metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-pinned-usage.Gauge").value(),
        256.0);
  }

  @Test
  public void testSetRMDBlockCacheReportsLiveUsageValues() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, "test_store", false);

    Cache mockCache = mock(Cache.class);
    when(mockCache.getUsage()).thenReturn(512L);
    when(mockCache.getPinnedUsage()).thenReturn(256L);

    stats.setRMDBlockCache(mockCache, 1024L);

    // Simulate runtime usage changes — usage and pinned-usage gauges should report live values
    when(mockCache.getUsage()).thenReturn(1024L);
    when(mockCache.getPinnedUsage()).thenReturn(512L);

    Assert
        .assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-usage.Gauge").value(), 1024.0);
    Assert.assertEquals(
        metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-pinned-usage.Gauge").value(),
        512.0);
  }
}
