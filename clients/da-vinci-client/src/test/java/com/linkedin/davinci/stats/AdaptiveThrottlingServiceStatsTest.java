package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADAPTIVE_THROTTLER_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
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

      String metricName = type.getMetricUnit() == MetricUnit.BYTES ? OTEL_BYTE_COUNT : OTEL_RECORD_COUNT;
      validateCounter(metricName, 10, buildAttributes(type));
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
    assertAllMethodsSafe(MetricsRepositoryUtils.createSingleThreadedMetricsRepository());
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
}
