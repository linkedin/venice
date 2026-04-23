package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_ADD_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_ADD_FAILURE_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_BYTES_INSERTED;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_HIT_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_MISS_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_READ_BYTES;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOCK_CACHE_WRITE_BYTES;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.BLOOM_FILTER_USEFUL_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.COMPACTION_CANCELLED_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.GET_HIT_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.MEMTABLE_HIT_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.MEMTABLE_MISS_COUNT;
import static com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity.READ_AMPLIFICATION_FACTOR;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ROCKSDB_LEVEL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

import com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity;
import com.linkedin.venice.stats.dimensions.VeniceRocksDBBlockCacheComponent;
import com.linkedin.venice.stats.dimensions.VeniceRocksDBLevel;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RocksDBStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private Statistics mockStats;
  private RocksDBStats rocksDBStats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    mockStats = mock(Statistics.class);
    rocksDBStats = new RocksDBStats(metricsRepository, "rocksdb_stat", TEST_CLUSTER_NAME);
    rocksDBStats.setRocksDBStat(mockStats);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  // --- Per-component block cache metrics (OTel with COMPONENT dimension) ---

  @Test
  public void testBlockCacheMissCountByComponent() {
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS)).thenReturn(10L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)).thenReturn(20L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_DATA_MISS)).thenReturn(30L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_COMPRESSION_DICT_MISS)).thenReturn(40L);

    validateComponentGauge(10, BLOCK_CACHE_MISS_COUNT, VeniceRocksDBBlockCacheComponent.INDEX);
    validateComponentGauge(20, BLOCK_CACHE_MISS_COUNT, VeniceRocksDBBlockCacheComponent.FILTER);
    validateComponentGauge(30, BLOCK_CACHE_MISS_COUNT, VeniceRocksDBBlockCacheComponent.DATA);
    validateComponentGauge(40, BLOCK_CACHE_MISS_COUNT, VeniceRocksDBBlockCacheComponent.COMPRESSION_DICT);
  }

  @Test
  public void testBlockCacheHitCountByComponent() {
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT)).thenReturn(100L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)).thenReturn(200L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_DATA_HIT)).thenReturn(300L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_COMPRESSION_DICT_HIT)).thenReturn(400L);

    validateComponentGauge(100, BLOCK_CACHE_HIT_COUNT, VeniceRocksDBBlockCacheComponent.INDEX);
    validateComponentGauge(200, BLOCK_CACHE_HIT_COUNT, VeniceRocksDBBlockCacheComponent.FILTER);
    validateComponentGauge(300, BLOCK_CACHE_HIT_COUNT, VeniceRocksDBBlockCacheComponent.DATA);
    validateComponentGauge(400, BLOCK_CACHE_HIT_COUNT, VeniceRocksDBBlockCacheComponent.COMPRESSION_DICT);
  }

  @Test
  public void testBlockCacheAddCountByComponent() {
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_INDEX_ADD)).thenReturn(5L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_FILTER_ADD)).thenReturn(6L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_DATA_ADD)).thenReturn(7L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_COMPRESSION_DICT_ADD)).thenReturn(8L);

    validateComponentGauge(5, BLOCK_CACHE_ADD_COUNT, VeniceRocksDBBlockCacheComponent.INDEX);
    validateComponentGauge(6, BLOCK_CACHE_ADD_COUNT, VeniceRocksDBBlockCacheComponent.FILTER);
    validateComponentGauge(7, BLOCK_CACHE_ADD_COUNT, VeniceRocksDBBlockCacheComponent.DATA);
    validateComponentGauge(8, BLOCK_CACHE_ADD_COUNT, VeniceRocksDBBlockCacheComponent.COMPRESSION_DICT);
  }

  @Test
  public void testBlockCacheBytesInsertByComponent() {
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_INDEX_BYTES_INSERT)).thenReturn(1024L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_FILTER_BYTES_INSERT)).thenReturn(2048L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_DATA_BYTES_INSERT)).thenReturn(4096L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT)).thenReturn(512L);

    validateComponentGauge(1024, BLOCK_CACHE_BYTES_INSERTED, VeniceRocksDBBlockCacheComponent.INDEX);
    validateComponentGauge(2048, BLOCK_CACHE_BYTES_INSERTED, VeniceRocksDBBlockCacheComponent.FILTER);
    validateComponentGauge(4096, BLOCK_CACHE_BYTES_INSERTED, VeniceRocksDBBlockCacheComponent.DATA);
    validateComponentGauge(512, BLOCK_CACHE_BYTES_INSERTED, VeniceRocksDBBlockCacheComponent.COMPRESSION_DICT);
  }

  // --- Joint Tehuti+OTel metrics (cluster-only attributes) ---

  @Test
  public void testJointBlockCacheMetrics() {
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_ADD_FAILURES)).thenReturn(3L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_BYTES_READ)).thenReturn(8192L);
    when(mockStats.getTickerCount(TickerType.BLOCK_CACHE_BYTES_WRITE)).thenReturn(4096L);

    validateClusterGauge(3, BLOCK_CACHE_ADD_FAILURE_COUNT);
    validateClusterGauge(8192, BLOCK_CACHE_READ_BYTES);
    validateClusterGauge(4096, BLOCK_CACHE_WRITE_BYTES);
  }

  @Test
  public void testJointBloomFilterMemtableAndCompactionMetrics() {
    when(mockStats.getTickerCount(TickerType.BLOOM_FILTER_USEFUL)).thenReturn(50L);
    when(mockStats.getTickerCount(TickerType.MEMTABLE_HIT)).thenReturn(60L);
    when(mockStats.getTickerCount(TickerType.MEMTABLE_MISS)).thenReturn(70L);
    when(mockStats.getTickerCount(TickerType.COMPACTION_CANCELLED)).thenReturn(2L);

    validateClusterGauge(50, BLOOM_FILTER_USEFUL_COUNT);
    validateClusterGauge(60, MEMTABLE_HIT_COUNT);
    validateClusterGauge(70, MEMTABLE_MISS_COUNT);
    validateClusterGauge(2, COMPACTION_CANCELLED_COUNT);
  }

  // --- Get Hit by Level (SST_LEVEL dimension) ---

  @Test
  public void testGetHitCountWithSstLevelDimension() {
    when(mockStats.getTickerCount(TickerType.GET_HIT_L0)).thenReturn(10L);
    when(mockStats.getTickerCount(TickerType.GET_HIT_L1)).thenReturn(20L);
    when(mockStats.getTickerCount(TickerType.GET_HIT_L2_AND_UP)).thenReturn(30L);

    validateSstLevelGauge(10, VeniceRocksDBLevel.LEVEL_0);
    validateSstLevelGauge(20, VeniceRocksDBLevel.LEVEL_1);
    validateSstLevelGauge(30, VeniceRocksDBLevel.LEVEL_2_AND_UP);
  }

  // --- Read Amplification (ASYNC_DOUBLE_GAUGE) ---

  @Test
  public void testReadAmplificationFactor() {
    when(mockStats.getTickerCount(TickerType.READ_AMP_TOTAL_READ_BYTES)).thenReturn(1000L);
    when(mockStats.getTickerCount(TickerType.READ_AMP_ESTIMATE_USEFUL_BYTES)).thenReturn(500L);

    OpenTelemetryDataTestUtils.validateDoublePointDataFromGauge(
        inMemoryMetricReader,
        2.0,
        0.01,
        buildClusterAttributes(),
        READ_AMPLIFICATION_FACTOR.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testReadAmplificationFactorReturnsNaNWhenUsefulBytesZero() {
    when(mockStats.getTickerCount(TickerType.READ_AMP_TOTAL_READ_BYTES)).thenReturn(1000L);
    when(mockStats.getTickerCount(TickerType.READ_AMP_ESTIMATE_USEFUL_BYTES)).thenReturn(0L);

    // NaN causes OTel SDK to drop the data point
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    String fullName =
        "venice." + TEST_METRIC_PREFIX + "." + READ_AMPLIFICATION_FACTOR.getMetricEntity().getMetricName();
    boolean hasDataPoint = metricsData.stream()
        .filter(m -> m.getName().equals(fullName))
        .flatMap(m -> m.getDoubleGaugeData().getPoints().stream())
        .anyMatch(p -> p.getAttributes().equals(buildClusterAttributes()));
    assertFalse(hasDataPoint, "Expected no data point when useful bytes is 0 (NaN)");
  }

  // --- Negative tests ---

  @Test
  public void testMetricsReturnNegativeOneBeforeStatSet() {
    InMemoryMetricReader uninitReader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository uninitRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(uninitReader)
            .build())) {
      new RocksDBStats(uninitRepo, "rocksdb_uninit", TEST_CLUSTER_NAME);

      // Joint metrics return -1 when rocksDBStat is null
      OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
          uninitReader,
          -1,
          buildClusterAttributes(),
          BLOOM_FILTER_USEFUL_COUNT.getMetricEntity().getMetricName(),
          TEST_METRIC_PREFIX);

      // Per-component metrics also return -1
      OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
          uninitReader,
          -1,
          buildComponentAttributes(VeniceRocksDBBlockCacheComponent.DATA),
          BLOCK_CACHE_MISS_COUNT.getMetricEntity().getMetricName(),
          TEST_METRIC_PREFIX);

      // SST-level metrics return -1
      OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
          uninitReader,
          -1,
          buildSstLevelAttributes(VeniceRocksDBLevel.LEVEL_0),
          GET_HIT_COUNT.getMetricEntity().getMetricName(),
          TEST_METRIC_PREFIX);
    }
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      RocksDBStats stats = new RocksDBStats(disabledRepo, "rocksdb_stat", TEST_CLUSTER_NAME);
      stats.setRocksDBStat(mockStats);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    RocksDBStats stats = new RocksDBStats(new MetricsRepository(), "rocksdb_stat", TEST_CLUSTER_NAME);
    stats.setRocksDBStat(mockStats);
  }

  // --- Helpers ---

  private void validateClusterGauge(long expectedValue, RocksDBStatsOtelMetricEntity entity) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        buildClusterAttributes(),
        entity.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  private void validateSstLevelGauge(long expectedValue, VeniceRocksDBLevel level) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        buildSstLevelAttributes(level),
        GET_HIT_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  private void validateComponentGauge(
      long expectedValue,
      RocksDBStatsOtelMetricEntity entity,
      VeniceRocksDBBlockCacheComponent component) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        buildComponentAttributes(component),
        entity.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  private static Attributes buildClusterAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }

  private static Attributes buildComponentAttributes(VeniceRocksDBBlockCacheComponent component) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT.getDimensionNameInDefaultFormat(), component.getDimensionValue())
        .build();
  }

  private static Attributes buildSstLevelAttributes(VeniceRocksDBLevel level) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_ROCKSDB_LEVEL.getDimensionNameInDefaultFormat(), level.getDimensionValue())
        .build();
  }
}
