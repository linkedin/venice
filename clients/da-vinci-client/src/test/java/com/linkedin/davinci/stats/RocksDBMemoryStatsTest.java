package com.linkedin.davinci.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.TimeUnit;
import org.rocksdb.Cache;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RocksDBMemoryStatsTest {
  @Test
  public void testSetRMDBlockCacheRegistersGauges() {
    MetricsRepository metricsRepository = new MetricsRepository();
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, "test_store", false);

    Cache mockCache = mock(Cache.class);
    when(mockCache.getUsage()).thenReturn(512L);
    when(mockCache.getPinnedUsage()).thenReturn(256L);

    stats.setRMDBlockCache(mockCache, 1024L);

    // AsyncGauge metrics may not be immediately available after registration; retry until propagated.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-capacity.Gauge").value(),
          1024.0);
      Assert
          .assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-usage.Gauge").value(), 512.0);
      Assert.assertEquals(
          metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-pinned-usage.Gauge").value(),
          256.0);
    });
  }

  @Test
  public void testSetRMDBlockCacheReportsLiveUsageValues() {
    MetricsRepository metricsRepository = new MetricsRepository();
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, "test_store", false);

    Cache mockCache = mock(Cache.class);
    when(mockCache.getUsage()).thenReturn(512L);
    when(mockCache.getPinnedUsage()).thenReturn(256L);

    stats.setRMDBlockCache(mockCache, 1024L);

    // Simulate runtime usage changes — usage and pinned-usage gauges should report live values
    when(mockCache.getUsage()).thenReturn(1024L);
    when(mockCache.getPinnedUsage()).thenReturn(512L);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-usage.Gauge").value(),
          1024.0);
      Assert.assertEquals(
          metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-pinned-usage.Gauge").value(),
          512.0);
    });
  }
}
