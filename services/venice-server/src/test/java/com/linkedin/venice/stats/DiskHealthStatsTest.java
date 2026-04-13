package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.DiskHealthOtelMetricEntity.DISK_HEALTH_STATUS;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DiskHealthStatsTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String METRIC_NAME = DISK_HEALTH_STATUS.getMetricEntity().getMetricName();

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private DiskHealthCheckService mockService;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    mockService = mock(DiskHealthCheckService.class);
    doReturn(true).when(mockService).isDiskHealthy();
    new DiskHealthStats(metricsRepository, mockService, "disk_health", TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
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
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      new DiskHealthStats(disabledRepo, mockService, "disk_health", TEST_CLUSTER_NAME);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    new DiskHealthStats(new MetricsRepository(), mockService, "disk_health", TEST_CLUSTER_NAME);
  }

  private static Attributes buildAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }
}
