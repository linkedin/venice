package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.NativeMetadataRepositoryOtelMetricEntity.METADATA_CACHE_STALENESS;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.time.Clock;
import java.util.Collection;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class NativeMetadataRepositoryStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String METRIC_NAME = METADATA_CACHE_STALENESS.getMetricEntity().getMetricName();

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private Clock mockClock;
  private NativeMetadataRepositoryStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    mockClock = mock(Clock.class);
    doReturn(1000L).when(mockClock).millis();
    stats = new NativeMetadataRepositoryStats(metricsRepository, "test", mockClock);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  @Test
  public void testPerStoreStaleness() {
    stats.updateCacheTimestamp("store-a", TEST_CLUSTER_NAME, 500);
    stats.updateCacheTimestamp("store-b", TEST_CLUSTER_NAME, 800);

    // store-a staleness = 1000 - 500 = 500ms
    validateGauge(500, "store-a");
    // store-b staleness = 1000 - 800 = 200ms
    validateGauge(200, "store-b");
  }

  @Test
  public void testStalenessUpdatesOnClockAdvance() {
    stats.updateCacheTimestamp("store-a", TEST_CLUSTER_NAME, 900);

    validateGauge(100, "store-a");

    // Clock advances
    doReturn(2000L).when(mockClock).millis();
    validateGauge(1100, "store-a");
  }

  @Test
  public void testStalenessUpdatesOnCacheRefresh() {
    stats.updateCacheTimestamp("store-a", TEST_CLUSTER_NAME, 500);
    validateGauge(500, "store-a");

    // Store metadata refreshed — staleness drops
    stats.updateCacheTimestamp("store-a", TEST_CLUSTER_NAME, 900);
    validateGauge(100, "store-a");
  }

  @Test
  public void testRemovedStoreReportsNaN() {
    stats.updateCacheTimestamp("store-a", TEST_CLUSTER_NAME, 500);
    validateGauge(500, "store-a");

    stats.removeCacheTimestamp("store-a");
    // After removal, callback returns NaN (no data). Validate directly since the
    // tolerance-based helper doesn't support NaN comparison (NaN != NaN in IEEE 754).
    validateGaugeAbsent("store-a");
  }

  @Test
  public void testMultiStoreIsolation() {
    stats.updateCacheTimestamp("store-a", TEST_CLUSTER_NAME, 200);
    stats.updateCacheTimestamp("store-b", TEST_CLUSTER_NAME, 900);

    validateGauge(800, "store-a");
    validateGauge(100, "store-b");
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      NativeMetadataRepositoryStats stats = new NativeMetadataRepositoryStats(disabledRepo, "test", mockClock);
      stats.updateCacheTimestamp("store-a", TEST_CLUSTER_NAME, 500);
      stats.removeCacheTimestamp("store-a");
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    NativeMetadataRepositoryStats stats = new NativeMetadataRepositoryStats(new MetricsRepository(), "test", mockClock);
    stats.updateCacheTimestamp("store-a", TEST_CLUSTER_NAME, 500);
    stats.removeCacheTimestamp("store-a");
  }

  /**
   * Verifies that a removed store emits no OTel data point. The callback returns NaN
   * which the OTel SDK drops entirely — the metric data point disappears from collection.
   */
  private void validateGaugeAbsent(String storeName) {
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    String fullMetricName = "venice." + TEST_METRIC_PREFIX + "." + METRIC_NAME;
    boolean hasDataPoint = metricsData.stream()
        .filter(m -> m.getName().equals(fullMetricName))
        .flatMap(m -> m.getDoubleGaugeData().getPoints().stream())
        .anyMatch(p -> p.getAttributes().equals(buildAttributes(storeName)));
    assertFalse(hasDataPoint, "Expected no data point for removed store: " + storeName);
  }

  private void validateGauge(double expectedValue, String storeName) {
    OpenTelemetryDataTestUtils.validateDoublePointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        0.01,
        buildAttributes(storeName),
        METRIC_NAME,
        TEST_METRIC_PREFIX);
  }

  private static Attributes buildAttributes(String storeName) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .build();
  }
}
