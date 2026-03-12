package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import org.rocksdb.Cache;
import org.rocksdb.SstFileManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RocksDBMemoryStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STATS_NAME = "test_store";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private Attributes expectedAttributes;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    expectedAttributes =
        Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  @Test
  public void testMetricsRegisteredWithNoPartitions() {
    new RocksDBMemoryStats(metricsRepository, TEST_STATS_NAME, false, TEST_CLUSTER_NAME);

    // With no partitions, all partition-aggregated metrics should return 0
    validateGauge("rocksdb.num_immutable_mem_table", 0);
    validateGauge("rocksdb.total_sst_files_size", 0);
    validateGauge("rocksdb.estimate_num_keys", 0);
    validateGauge("rocksdb.num_running_compactions", 0);

    // Instance-level block cache metrics also 0 with no partitions
    validateGauge("rocksdb.block_cache_capacity", 0);
    validateGauge("rocksdb.block_cache_usage", 0);
  }

  @Test
  public void testBlockCacheMetricsNotRegisteredWithPlainTable() {
    new RocksDBMemoryStats(metricsRepository, TEST_STATS_NAME, true, TEST_CLUSTER_NAME);

    // Non-block-cache metric should still be present
    validateGauge("rocksdb.num_immutable_mem_table", 0);
    validateGauge("rocksdb.compaction_pending", 0);
    validateGauge("rocksdb.num_blob_files", 0);

    // Block cache metrics should NOT be present when plainTableEnabled=true
    String[] blockCacheMetrics =
        { "rocksdb.block_cache_capacity", "rocksdb.block_cache_usage", "rocksdb.block_cache_pinned_usage" };
    for (String metricName: blockCacheMetrics) {
      String fullName = TEST_METRIC_PREFIX + "." + metricName;
      boolean absent = inMemoryMetricReader.collectAllMetrics().stream().noneMatch(md -> md.getName().equals(fullName));
      assertTrue(absent, "Block cache metric '" + fullName + "' should not be registered when plainTableEnabled=true");
    }
  }

  @Test
  public void testMemoryLimitGauge() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, TEST_STATS_NAME, false, TEST_CLUSTER_NAME);

    // Default memory limit is -1
    validateGauge("rocksdb.memory_limit", -1);

    // Set memory limit and verify
    stats.setMemoryLimit(4096L);
    validateGauge("rocksdb.memory_limit", 4096);
  }

  @Test
  public void testMemoryUsageGauge() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, TEST_STATS_NAME, false, TEST_CLUSTER_NAME);

    // Without sstFileManager or positive memoryLimit, returns -1
    validateGauge("rocksdb.memory_usage", -1);

    // Set memory limit but no sstFileManager — still -1
    stats.setMemoryLimit(4096L);
    validateGauge("rocksdb.memory_usage", -1);

    // Set sstFileManager
    SstFileManager mockSstFileManager = mock(SstFileManager.class);
    when(mockSstFileManager.getTotalSize()).thenReturn(2048L);
    stats.setSstFileManager(mockSstFileManager);

    validateGauge("rocksdb.memory_usage", 2048);
  }

  @Test
  public void testMemoryUsageWithZeroMemoryLimit() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, TEST_STATS_NAME, false, TEST_CLUSTER_NAME);

    stats.setMemoryLimit(0);
    SstFileManager mockSstFileManager = mock(SstFileManager.class);
    when(mockSstFileManager.getTotalSize()).thenReturn(2048L);
    stats.setSstFileManager(mockSstFileManager);

    // memoryLimit <= 0 means usage returns -1
    validateGauge("rocksdb.memory_usage", -1);
  }

  @Test
  public void testRMDBlockCacheGauges() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, TEST_STATS_NAME, false, TEST_CLUSTER_NAME);

    Cache mockCache = mock(Cache.class);
    when(mockCache.getUsage()).thenReturn(512L);
    when(mockCache.getPinnedUsage()).thenReturn(256L);

    stats.setRMDBlockCache(mockCache, 1024L);

    validateGauge("rocksdb.rmd_block_cache_capacity", 1024);
    validateGauge("rocksdb.rmd_block_cache_usage", 512);
    validateGauge("rocksdb.rmd_block_cache_pinned_usage", 256);
  }

  @Test
  public void testRMDBlockCacheLiveValueUpdates() {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(metricsRepository, TEST_STATS_NAME, false, TEST_CLUSTER_NAME);

    Cache mockCache = mock(Cache.class);
    when(mockCache.getUsage()).thenReturn(512L);
    when(mockCache.getPinnedUsage()).thenReturn(256L);
    stats.setRMDBlockCache(mockCache, 1024L);

    // Change cache values
    when(mockCache.getUsage()).thenReturn(2048L);
    when(mockCache.getPinnedUsage()).thenReturn(1024L);

    // OTel should reflect live values
    validateGauge("rocksdb.rmd_block_cache_usage", 2048);
    validateGauge("rocksdb.rmd_block_cache_pinned_usage", 1024);
    // Capacity is fixed
    validateGauge("rocksdb.rmd_block_cache_capacity", 1024);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository otelDisabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .build());
    try {
      exerciseAllRecordingPaths(otelDisabledRepo);
    } finally {
      otelDisabledRepo.close();
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() throws Exception {
    AsyncGauge.AsyncGaugeExecutor executor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    MetricsRepository plainRepo = new MetricsRepository(new MetricConfig(executor));
    try {
      exerciseAllRecordingPaths(plainRepo);
    } finally {
      plainRepo.close();
      executor.close();
    }
  }

  /** Exercises all recording paths on a RocksDBMemoryStats instance — used by NPE prevention tests. */
  private static void exerciseAllRecordingPaths(MetricsRepository repo) {
    RocksDBMemoryStats stats = new RocksDBMemoryStats(repo, TEST_STATS_NAME, false, TEST_CLUSTER_NAME);

    stats.setMemoryLimit(1024L);
    SstFileManager mockSstFileManager = mock(SstFileManager.class);
    when(mockSstFileManager.getTotalSize()).thenReturn(512L);
    stats.setSstFileManager(mockSstFileManager);

    Cache mockCache = mock(Cache.class);
    when(mockCache.getUsage()).thenReturn(256L);
    when(mockCache.getPinnedUsage()).thenReturn(128L);
    stats.setRMDBlockCache(mockCache, 512L);
  }

  private void validateGauge(String metricName, long expectedValue) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
