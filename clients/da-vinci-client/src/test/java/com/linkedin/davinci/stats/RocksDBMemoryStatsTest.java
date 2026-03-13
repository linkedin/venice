package com.linkedin.davinci.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import org.rocksdb.Cache;
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
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, "test_store", false, "test-cluster");

    Cache mockCache = mock(Cache.class);
    when(mockCache.getUsage()).thenReturn(512L);
    when(mockCache.getPinnedUsage()).thenReturn(256L);

    stats.setRMDBlockCache(mockCache, 1024L);

    assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-capacity.Gauge").value(), 1024.0);
    assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-usage.Gauge").value(), 512.0);
    assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-pinned-usage.Gauge").value(), 256.0);
  }

  @Test
  public void testSetRMDBlockCacheReportsLiveUsageValues() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, "test_store", false, "test-cluster");

    Cache mockCache = mock(Cache.class);
    when(mockCache.getUsage()).thenReturn(512L);
    when(mockCache.getPinnedUsage()).thenReturn(256L);

    stats.setRMDBlockCache(mockCache, 1024L);

    // Simulate runtime usage changes — usage and pinned-usage gauges should report live values
    when(mockCache.getUsage()).thenReturn(1024L);
    when(mockCache.getPinnedUsage()).thenReturn(512L);

    assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-usage.Gauge").value(), 1024.0);
    assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-pinned-usage.Gauge").value(), 512.0);
  }

  @Test
  public void testSetRMDBlockCacheIgnoresDuplicateCall() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, "test_store", false, "test-cluster");

    Cache firstCache = mock(Cache.class);
    when(firstCache.getUsage()).thenReturn(512L);
    when(firstCache.getPinnedUsage()).thenReturn(256L);
    stats.setRMDBlockCache(firstCache, 1024L);

    // Second call with different values should be ignored
    Cache secondCache = mock(Cache.class);
    when(secondCache.getUsage()).thenReturn(9999L);
    when(secondCache.getPinnedUsage()).thenReturn(8888L);
    stats.setRMDBlockCache(secondCache, 5000L);

    // Gauges should still report the first cache's values
    assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-capacity.Gauge").value(), 1024.0);
    assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-usage.Gauge").value(), 512.0);
    assertEquals(metricsRepository.getMetric(".test_store--rocksdb.rmd-block-cache-pinned-usage.Gauge").value(), 256.0);
  }

  @Test
  public void testBlockCacheTehutiSensorsNotRegisteredWithPlainTable() {
    new RocksDBMemoryStats(metricsRepository, "test_store", true, "test-cluster");

    // Non-block-cache Tehuti sensors should be registered
    assertNotNull(metricsRepository.getMetric(".test_store--rocksdb.num-immutable-mem-table.Gauge"));
    assertNotNull(metricsRepository.getMetric(".test_store--rocksdb.compaction-pending.Gauge"));

    // Block cache Tehuti sensors should NOT be registered when plainTableEnabled=true
    assertNull(
        metricsRepository.getMetric(".test_store--rocksdb.block-cache-capacity.Gauge"),
        "block-cache-capacity Tehuti sensor should not be registered when plainTableEnabled=true");
    assertNull(
        metricsRepository.getMetric(".test_store--rocksdb.block-cache-usage.Gauge"),
        "block-cache-usage Tehuti sensor should not be registered when plainTableEnabled=true");
    assertNull(
        metricsRepository.getMetric(".test_store--rocksdb.block-cache-pinned-usage.Gauge"),
        "block-cache-pinned-usage Tehuti sensor should not be registered when plainTableEnabled=true");
  }
}
