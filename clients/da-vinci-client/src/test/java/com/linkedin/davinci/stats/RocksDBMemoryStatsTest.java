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
import org.rocksdb.SstFileManager;
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

  @Test
  public void testMemoryLimitTehutiGauge() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, "test_store", false, "test-cluster");

    // Default memory limit is -1
    assertEquals(metricsRepository.getMetric(".test_store--memory_limit.Gauge").value(), -1.0);

    // Set memory limit and verify Tehuti sensor reflects it
    stats.setMemoryLimit(4096L);
    assertEquals(metricsRepository.getMetric(".test_store--memory_limit.Gauge").value(), 4096.0);
  }

  @Test
  public void testMemoryUsageTehutiGauge() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, "test_store", false, "test-cluster");

    // Without sstFileManager or positive memoryLimit, returns -1
    assertEquals(metricsRepository.getMetric(".test_store--memory_usage.Gauge").value(), -1.0);

    // Set memory limit and sstFileManager
    stats.setMemoryLimit(4096L);
    SstFileManager mockSstFileManager = mock(SstFileManager.class);
    when(mockSstFileManager.getTotalSize()).thenReturn(2048L);
    stats.setSstFileManager(mockSstFileManager);

    assertEquals(metricsRepository.getMetric(".test_store--memory_usage.Gauge").value(), 2048.0);
  }
}
