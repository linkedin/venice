package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADAPTIVE_THROTTLER_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceAdaptiveThrottlerType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AdaptiveThrottlingServiceStatsTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String OTEL_RECORD_COUNT =
      AdaptiveThrottlingOtelMetricEntity.RECORD_COUNT.getMetricEntity().getMetricName();
  private static final String OTEL_BYTE_COUNT =
      AdaptiveThrottlingOtelMetricEntity.BYTE_COUNT.getMetricEntity().getMetricName();
  private static final String OTEL_CURRENT_LIMIT =
      AdaptiveThrottlingOtelMetricEntity.CURRENT_LIMIT.getMetricEntity().getMetricName();

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private AdaptiveThrottlingServiceStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());
    stats = new AdaptiveThrottlingServiceStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  // --- OTel recording tests ---

  @Test
  public void testAllThrottlerTypesAreExercised() {
    for (VeniceAdaptiveThrottlerType type: VeniceAdaptiveThrottlerType.values()) {
      stats.recordRateForAdaptiveThrottler(type, 10);

      if (AdaptiveThrottlingServiceStats.GAUGE_TYPES.contains(type)) {
        // Gauge types report the value as-is (no interval scaling).
        validateGauge(OTEL_CURRENT_LIMIT, 10, buildAttributes(type));
      } else {
        String metricName = type.getMetricUnit() == MetricUnit.BYTES ? OTEL_BYTE_COUNT : OTEL_RECORD_COUNT;
        validateCounter(metricName, 10, buildAttributes(type));
      }
    }
  }

  @Test
  public void testCounterAccumulation() {
    stats.recordRateForAdaptiveThrottler(VeniceAdaptiveThrottlerType.PUBSUB_CONSUMPTION_RECORDS_COUNT, 100);
    stats.recordRateForAdaptiveThrottler(VeniceAdaptiveThrottlerType.PUBSUB_CONSUMPTION_RECORDS_COUNT, 200);
    stats.recordRateForAdaptiveThrottler(VeniceAdaptiveThrottlerType.PUBSUB_CONSUMPTION_RECORDS_COUNT, 50);

    validateCounter(
        OTEL_RECORD_COUNT,
        350,
        buildAttributes(VeniceAdaptiveThrottlerType.PUBSUB_CONSUMPTION_RECORDS_COUNT));
  }

  // --- Tehuti sensor verification ---

  @Test
  public void testTehutiSensorRecordedOnRecord() {
    stats.recordRateForAdaptiveThrottler(VeniceAdaptiveThrottlerType.PUBSUB_CONSUMPTION_RECORDS_COUNT, 100);

    // Tehuti metric name: .AdaptiveThrottlingService--kafka_consumption_records_count.Rate
    String tehutiMetricName = ".AdaptiveThrottlingService--kafka_consumption_records_count.Rate";
    double tehutiRate = metricsRepository.getMetric(tehutiMetricName).value();
    // Rate is calculated over a time window so the exact value depends on timing; verify it's non-negative
    assertTrue(tehutiRate >= 0, "Tehuti Rate sensor should have a non-negative value");
  }

  @Test
  public void testBlobTransferReportsCurrentLimitAsGauge() {
    stats.recordRateForAdaptiveThrottler(VeniceAdaptiveThrottlerType.BLOB_TRANSFER_WRITE_BANDWIDTH, 1000);

    // OTel async gauge reports the exact current limit, not a value scaled by the refresh interval.
    validateGauge(OTEL_CURRENT_LIMIT, 1000, buildAttributes(VeniceAdaptiveThrottlerType.BLOB_TRANSFER_WRITE_BANDWIDTH));

    // Tehuti AsyncGauge sensor is wired (binding regression guard). Suffix is .Gauge, not .Rate.
    assertNotNull(
        metricsRepository.getMetric(".AdaptiveThrottlingService--blob_transfer_write_bandwidth.Gauge"),
        "Blob transfer write throttler should register a Tehuti gauge sensor");
  }

  @Test
  public void testBlobTransferGaugeReflectsLatestValue() {
    stats.recordRateForAdaptiveThrottler(VeniceAdaptiveThrottlerType.BLOB_TRANSFER_READ_BANDWIDTH, 500);
    stats.recordRateForAdaptiveThrottler(VeniceAdaptiveThrottlerType.BLOB_TRANSFER_READ_BANDWIDTH, 800);

    // Gauge reports the last value (800), not the sum/accumulation that a counter would show.
    validateGauge(OTEL_CURRENT_LIMIT, 800, buildAttributes(VeniceAdaptiveThrottlerType.BLOB_TRANSFER_READ_BANDWIDTH));
  }

  // --- NPE prevention tests ---

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      assertAllMethodsSafe(disabledRepo);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    try {
      assertAllMethodsSafe(plainRepo);
    } finally {
      plainRepo.close();
    }
  }

  private void assertAllMethodsSafe(MetricsRepository repo) {
    AdaptiveThrottlingServiceStats safeStats = new AdaptiveThrottlingServiceStats(repo, TEST_CLUSTER_NAME);
    for (VeniceAdaptiveThrottlerType type: VeniceAdaptiveThrottlerType.values()) {
      safeStats.recordRateForAdaptiveThrottler(type, 100);
    }
  }

  // --- Helper methods ---

  private Attributes buildAttributes(VeniceAdaptiveThrottlerType throttlerType) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_ADAPTIVE_THROTTLER_TYPE.getDimensionNameInDefaultFormat(), throttlerType.getDimensionValue())
        .build();
  }

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateGauge(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
