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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.davinci.stats.RocksDBStatsOtelMetricEntity;
import com.linkedin.venice.stats.dimensions.VeniceRocksDBBlockCacheComponent;
import com.linkedin.venice.stats.dimensions.VeniceRocksDBLevel;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.io.IOException;
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
  private AsyncGauge.AsyncGaugeExecutor asyncGaugeExecutor;
  private Statistics mockStats;
  private RocksDBStats rocksDBStats;

  @BeforeMethod
  public void setUp() throws IOException {
    inMemoryMetricReader = InMemoryMetricReader.create();
    // Dedicated AsyncGauge executor: VeniceMetricsRepository.close() shuts down the Tehuti
    // executor it was given. Without this, close() in tearDown / try-with-resources would shut
    // down the static singleton DEFAULT_ASYNC_GAUGE_EXECUTOR JVM-wide and break later tests.
    asyncGaugeExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(new MetricConfig(asyncGaugeExecutor))
            .build());
    mockStats = mock(Statistics.class);
    rocksDBStats = new RocksDBStats(metricsRepository, "rocksdb_stat", TEST_CLUSTER_NAME);
    rocksDBStats.setRocksDBStat(mockStats);
  }

  @AfterMethod
  public void tearDown() throws IOException {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
    if (asyncGaugeExecutor != null) {
      asyncGaugeExecutor.close();
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
  public void testMetricsBehaviorBeforeStatSet() throws IOException {
    InMemoryMetricReader uninitReader = InMemoryMetricReader.create();
    try (AsyncGauge.AsyncGaugeExecutor localExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
        VeniceMetricsRepository uninitRepo = new VeniceMetricsRepository(
            new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
                .setMetricEntities(SERVER_METRIC_ENTITIES)
                .setEmitOtelMetrics(true)
                .setOtelAdditionalMetricsReader(uninitReader)
                .setTehutiMetricConfig(new MetricConfig(localExecutor))
                .build())) {
      new RocksDBStats(uninitRepo, "rocksdb_uninit", TEST_CLUSTER_NAME);

      // Joint metrics (AsyncMetricEntityStateBase) emit the -1 sentinel pre-init — the base
      // wrapper has no liveness contract, so the LongSupplier callback fires every cycle and
      // returns -1 when rocksDBStat is null.
      OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
          uninitReader,
          -1,
          buildClusterAttributes(),
          BLOOM_FILTER_USEFUL_COUNT.getMetricEntity().getMetricName(),
          TEST_METRIC_PREFIX);

      // Per-component and SST-level metrics (AsyncMetricEntityStateOneEnum) honor the dormant
      // contract — the liveStateResolver returns null pre-init, so no data point is emitted.
      Collection<MetricData> metrics = uninitReader.collectAllMetrics();
      assertNull(
          OpenTelemetryDataTestUtils.getLongPointDataFromGaugeIfPresent(
              metrics,
              BLOCK_CACHE_MISS_COUNT.getMetricEntity().getMetricName(),
              TEST_METRIC_PREFIX,
              buildComponentAttributes(VeniceRocksDBBlockCacheComponent.DATA)),
          "Per-component metric should emit no data point pre-init (dormant contract)");
      assertNull(
          OpenTelemetryDataTestUtils.getLongPointDataFromGaugeIfPresent(
              metrics,
              GET_HIT_COUNT.getMetricEntity().getMetricName(),
              TEST_METRIC_PREFIX,
              buildSstLevelAttributes(VeniceRocksDBLevel.LEVEL_0)),
          "SST-level metric should emit no data point pre-init (dormant contract)");
    }
  }

  // --- Completeness guards ---
  // If a new VeniceRocksDBLevel or VeniceRocksDBBlockCacheComponent value is added without
  // updating the corresponding ticker map in RocksDBStats, the liveStateResolver returns null
  // and the new combo silently emits no data point. These tests iterate every enum value and
  // assert each lands a data point, catching the omission at CI time.

  @Test
  public void testEveryVeniceRocksDBLevelEmitsForGetHitCount() {
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    String metricName = GET_HIT_COUNT.getMetricEntity().getMetricName();
    for (VeniceRocksDBLevel level: VeniceRocksDBLevel.values()) {
      assertNotNull(
          OpenTelemetryDataTestUtils.getLongPointDataFromGaugeIfPresent(
              metrics,
              metricName,
              TEST_METRIC_PREFIX,
              buildSstLevelAttributes(level)),
          "VeniceRocksDBLevel." + level + " must emit a data point — add it to "
              + "GET_HIT_TICKER_BY_LEVEL in RocksDBStats.");
    }
  }

  @Test
  public void testEveryVeniceRocksDBBlockCacheComponentEmitsForAllPerComponentMetrics() {
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    RocksDBStatsOtelMetricEntity[] perComponentMetrics =
        { BLOCK_CACHE_MISS_COUNT, BLOCK_CACHE_HIT_COUNT, BLOCK_CACHE_ADD_COUNT, BLOCK_CACHE_BYTES_INSERTED };
    for (VeniceRocksDBBlockCacheComponent component: VeniceRocksDBBlockCacheComponent.values()) {
      Attributes attrs = buildComponentAttributes(component);
      for (RocksDBStatsOtelMetricEntity entity: perComponentMetrics) {
        String metricName = entity.getMetricEntity().getMetricName();
        assertNotNull(
            OpenTelemetryDataTestUtils
                .getLongPointDataFromGaugeIfPresent(metrics, metricName, TEST_METRIC_PREFIX, attrs),
            "VeniceRocksDBBlockCacheComponent." + component + " must emit a data point for " + metricName
                + " — add it to the corresponding ticker map in RocksDBStats.");
      }
    }
  }

  @Test
  public void testOtelEmitsForTotalStatsName() throws IOException {
    // Regression guard: in production AggRocksDBStats only constructs the totalStats instance
    // (named "total") and never per-store stats, so the totalStats IS the canonical recorder.
    // RocksDBStats must not propagate isTotalStats() — otherwise OpenTelemetryMetricsSetup
    // suppresses every RocksDB OTel emission in production.
    InMemoryMetricReader totalReader = InMemoryMetricReader.create();
    try (AsyncGauge.AsyncGaugeExecutor localExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
        VeniceMetricsRepository totalRepo = new VeniceMetricsRepository(
            new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
                .setMetricEntities(SERVER_METRIC_ENTITIES)
                .setEmitOtelMetrics(true)
                .setOtelAdditionalMetricsReader(totalReader)
                .setTehutiMetricConfig(new MetricConfig(localExecutor))
                .build())) {
      Statistics totalMockStats = mock(Statistics.class);
      when(totalMockStats.getTickerCount(TickerType.GET_HIT_L0)).thenReturn(123L);
      RocksDBStats totalStats = new RocksDBStats(totalRepo, "total", TEST_CLUSTER_NAME);
      totalStats.setRocksDBStat(totalMockStats);

      OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
          totalReader,
          123,
          buildSstLevelAttributes(VeniceRocksDBLevel.LEVEL_0),
          GET_HIT_COUNT.getMetricEntity().getMetricName(),
          TEST_METRIC_PREFIX);
    }
  }

  @Test
  public void testNoNpeWhenOtelDisabled() throws IOException {
    try (AsyncGauge.AsyncGaugeExecutor localExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
        VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
            new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
                .setEmitOtelMetrics(false)
                .setTehutiMetricConfig(new MetricConfig(localExecutor))
                .build())) {
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
