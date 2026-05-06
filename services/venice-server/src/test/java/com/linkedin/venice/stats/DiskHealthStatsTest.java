package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.DiskHealthOtelMetricEntity.DISK_HEALTH_STATUS;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DiskHealthStatsTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String STATS_NAME = "disk_health";
  private static final String METRIC_NAME = DISK_HEALTH_STATUS.getMetricEntity().getMetricName();
  /** Tehuti metric path: .{statsName}--{tehutiMetricName}.{StatTypeSuffix}. */
  private static final String TEHUTI_DISK_HEALTHY = "." + STATS_NAME + "--disk_healthy.Gauge";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  // Dedicated AsyncGauge executor: another test class calling MetricsRepository.close()
  // in the same JVM shuts down the static default executor, which would make
  // AsyncGauge.measure() return 0.0 in this test forever.
  private AsyncGauge.AsyncGaugeExecutor asyncGaugeExecutor;
  private DiskHealthCheckService mockService;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    asyncGaugeExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(new MetricConfig(asyncGaugeExecutor))
            .build());
    mockService = mock(DiskHealthCheckService.class);
    doReturn(true).when(mockService).isDiskHealthy();
    new DiskHealthStats(metricsRepository, mockService, STATS_NAME, TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      // Closes the dedicated executor we injected via setTehutiMetricConfig — does NOT touch
      // the static AsyncGauge.DEFAULT_ASYNC_GAUGE_EXECUTOR shared with other test classes.
      metricsRepository.close();
    }
  }

  @Test
  public void testHealthyDiskReportsOne() {
    OpenTelemetryDataTestUtils
        .validateLongPointDataFromGauge(inMemoryMetricReader, 1, buildAttributes(), METRIC_NAME, TEST_METRIC_PREFIX);
  }

  @Test
  public void testUnhealthyDiskReportsZero() {
    doReturn(false).when(mockService).isDiskHealthy();

    OpenTelemetryDataTestUtils
        .validateLongPointDataFromGauge(inMemoryMetricReader, 0, buildAttributes(), METRIC_NAME, TEST_METRIC_PREFIX);
  }

  @Test
  public void testGaugeUpdatesOnHealthChange() {
    OpenTelemetryDataTestUtils
        .validateLongPointDataFromGauge(inMemoryMetricReader, 1, buildAttributes(), METRIC_NAME, TEST_METRIC_PREFIX);

    // Disk becomes unhealthy
    doReturn(false).when(mockService).isDiskHealthy();

    OpenTelemetryDataTestUtils
        .validateLongPointDataFromGauge(inMemoryMetricReader, 0, buildAttributes(), METRIC_NAME, TEST_METRIC_PREFIX);
  }

  @Test
  public void testTehutiSensorReportsHealth() {
    // Joint Tehuti+OTel API regression: ensures the Tehuti AsyncGauge binding stays wired so a
    // future refactor can't accidentally drop the Tehuti side and only leave OTel emitting.
    assertEquals(metricsRepository.getMetric(TEHUTI_DISK_HEALTHY).value(), 1.0);

    doReturn(false).when(mockService).isDiskHealthy();
    assertEquals(metricsRepository.getMetric(TEHUTI_DISK_HEALTHY).value(), 0.0);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    AsyncGauge.AsyncGaugeExecutor dedicatedExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setEmitOtelMetrics(false)
            .setTehutiMetricConfig(new MetricConfig(dedicatedExecutor))
            .build())) {
      new DiskHealthStats(disabledRepo, mockService, STATS_NAME, TEST_CLUSTER_NAME);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    new DiskHealthStats(new MetricsRepository(), mockService, STATS_NAME, TEST_CLUSTER_NAME);
  }

  private static Attributes buildAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }
}
