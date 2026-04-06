package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.HeartbeatMonitoringOtelMetricEntity.HEARTBEAT_MONITORING_EXCEPTION_COUNT;
import static com.linkedin.davinci.stats.HeartbeatMonitoringOtelMetricEntity.HEARTBEAT_MONITORING_HEARTBEAT_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HEARTBEAT_COMPONENT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceHeartbeatComponent;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class HeartbeatMonitoringServiceStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STAT_PREFIX = "test-prefix";

  /** OTel metric names */
  private static final String OTEL_EXCEPTION_COUNT_METRIC = HEARTBEAT_MONITORING_EXCEPTION_COUNT.getMetricName();
  private static final String OTEL_HEARTBEAT_COUNT_METRIC = HEARTBEAT_MONITORING_HEARTBEAT_COUNT.getMetricName();

  /** Tehuti metric names (AbstractVeniceStats replaces dots/colons with underscores in the prefix) */
  private static final String TEHUTI_EXCEPTION_METRIC =
      ".test-prefix-heartbeat-monitor-service--heartbeat-monitor-service-exception-count.Count";
  private static final String TEHUTI_REPORTER_METRIC =
      ".test-prefix-heartbeat-monitor-service--heartbeat-reporter.OccurrenceRate";
  private static final String TEHUTI_LOGGER_METRIC =
      ".test-prefix-heartbeat-monitor-service--heartbeat-logger.OccurrenceRate";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private HeartbeatMonitoringServiceStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    this.metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());
    stats = new HeartbeatMonitoringServiceStats(metricsRepository, TEST_STAT_PREFIX, TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  @Test
  public void testRecordReporterHeartbeat() {
    stats.recordReporterHeartbeat();

    // OTel heartbeat counter with REPORTER dimension
    validateCounter(OTEL_HEARTBEAT_COUNT_METRIC, 1, buildExpectedAttributes(VeniceHeartbeatComponent.REPORTER));

    // Tehuti reporter sensor recorded
    assertTrue(metricsRepository.getMetric(TEHUTI_REPORTER_METRIC).value() > 0);

    // Negative assertions: logger Tehuti sensor NOT affected
    assertEquals(metricsRepository.getMetric(TEHUTI_LOGGER_METRIC).value(), 0d);
    // Exception sensor NOT affected
    assertEquals(metricsRepository.getMetric(TEHUTI_EXCEPTION_METRIC).value(), 0d);
  }

  @Test
  public void testRecordLoggerHeartbeat() {
    stats.recordLoggerHeartbeat();

    // OTel heartbeat counter with LOGGER dimension
    validateCounter(OTEL_HEARTBEAT_COUNT_METRIC, 1, buildExpectedAttributes(VeniceHeartbeatComponent.LOGGER));

    // Tehuti logger sensor recorded
    assertTrue(metricsRepository.getMetric(TEHUTI_LOGGER_METRIC).value() > 0);

    // Negative assertions: reporter Tehuti sensor NOT affected
    assertEquals(metricsRepository.getMetric(TEHUTI_REPORTER_METRIC).value(), 0d);
    // Exception sensor NOT affected
    assertEquals(metricsRepository.getMetric(TEHUTI_EXCEPTION_METRIC).value(), 0d);
  }

  @DataProvider(name = "heartbeatComponents")
  public static Object[][] heartbeatComponents() {
    return new Object[][] { { VeniceHeartbeatComponent.REPORTER }, { VeniceHeartbeatComponent.LOGGER } };
  }

  @Test(dataProvider = "heartbeatComponents")
  public void testRecordException(VeniceHeartbeatComponent component) {
    stats.recordHeartbeatExceptionCount(component);

    validateCounter(OTEL_EXCEPTION_COUNT_METRIC, 1, buildExpectedAttributes(component));
    assertEquals(metricsRepository.getMetric(TEHUTI_EXCEPTION_METRIC).value(), 1d);

    // Negative assertions: heartbeat sensors NOT affected
    assertEquals(metricsRepository.getMetric(TEHUTI_REPORTER_METRIC).value(), 0d);
    assertEquals(metricsRepository.getMetric(TEHUTI_LOGGER_METRIC).value(), 0d);
  }

  @Test
  public void testReporterAndLoggerAreIndependent() {
    stats.recordReporterHeartbeat();
    stats.recordReporterHeartbeat();
    stats.recordLoggerHeartbeat();

    // Reporter counter accumulated to 2
    validateCounter(OTEL_HEARTBEAT_COUNT_METRIC, 2, buildExpectedAttributes(VeniceHeartbeatComponent.REPORTER));

    // Logger counter is 1 (independent)
    validateCounter(OTEL_HEARTBEAT_COUNT_METRIC, 1, buildExpectedAttributes(VeniceHeartbeatComponent.LOGGER));
  }

  @Test
  public void testExceptionCounterAccumulationAndIsolation() {
    stats.recordHeartbeatExceptionCount(VeniceHeartbeatComponent.REPORTER);
    stats.recordHeartbeatExceptionCount(VeniceHeartbeatComponent.REPORTER);
    stats.recordHeartbeatExceptionCount(VeniceHeartbeatComponent.LOGGER);

    // Reporter exception counter accumulated to 2
    validateCounter(OTEL_EXCEPTION_COUNT_METRIC, 2, buildExpectedAttributes(VeniceHeartbeatComponent.REPORTER));

    // Logger exception counter is 1 (independent)
    validateCounter(OTEL_EXCEPTION_COUNT_METRIC, 1, buildExpectedAttributes(VeniceHeartbeatComponent.LOGGER));
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      assertAllMethodsSafeWithRepo(disabledRepo);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    assertAllMethodsSafeWithRepo(MetricsRepositoryUtils.createSingleThreadedMetricsRepository());
  }

  private void assertAllMethodsSafeWithRepo(MetricsRepository repo) {
    HeartbeatMonitoringServiceStats safeStats =
        new HeartbeatMonitoringServiceStats(repo, TEST_STAT_PREFIX, TEST_CLUSTER_NAME);
    safeStats.recordReporterHeartbeat();
    safeStats.recordLoggerHeartbeat();
    safeStats.recordHeartbeatExceptionCount(VeniceHeartbeatComponent.REPORTER);
    safeStats.recordHeartbeatExceptionCount(VeniceHeartbeatComponent.LOGGER);
  }

  private Attributes buildExpectedAttributes(VeniceHeartbeatComponent component) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_HEARTBEAT_COMPONENT.getDimensionNameInDefaultFormat(), component.getDimensionValue())
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
