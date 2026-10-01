package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SYSTEM_STORE_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceSystemStoreType;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SystemStoreHealthCheckStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String RESOURCE_NAME = "." + TEST_CLUSTER_NAME;
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private SystemStoreHealthCheckStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());

    stats = new SystemStoreHealthCheckStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @Test
  public void testUnhealthyCountPerSystemStoreType() {
    stats.getBadMetaSystemStoreCounter().set(3);
    stats.getBadPushStatusSystemStoreCounter().set(7);
    stats.setMeasured(true);

    // OTel: Validate META_STORE dimension
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName(),
        3,
        clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType.META_STORE));

    // OTel: Validate DAVINCI_PUSH_STATUS_STORE dimension
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName(),
        7,
        clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE));

    // Tehuti
    validateTehutiMetric(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.BAD_META_SYSTEM_STORE_COUNT,
        "Gauge",
        3.0);
    validateTehutiMetric(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.BAD_PUSH_STATUS_SYSTEM_STORE_COUNT,
        "Gauge",
        7.0);
  }

  @Test
  public void testUnrepairableCount() {
    stats.getNotRepairableSystemStoreCounter().set(5);
    stats.setMeasured(true);

    // OTel
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNREPAIRABLE_COUNT
            .getMetricName(),
        5,
        clusterAttributes());

    // Tehuti
    validateTehutiMetric(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.NOT_REPAIRABLE_SYSTEM_STORE_COUNT,
        "Gauge",
        5.0);
  }

  @Test
  public void testHealthCheckErrorCount() {
    stats.getSystemStoreHealthCheckErrorCounter().set(4);
    stats.setMeasured(true);

    // OTel
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_HEALTH_CHECK_ERROR_COUNT
            .getMetricName(),
        4,
        clusterAttributes());

    // Tehuti
    validateTehutiMetric(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.SYSTEM_STORE_HEALTH_CHECK_ERROR_COUNT,
        "Gauge",
        4.0);
  }

  @Test
  public void testCounterResetToZero() {
    stats.getBadMetaSystemStoreCounter().set(5);
    stats.setMeasured(true);
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName(),
        5,
        clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType.META_STORE));

    // A later round resets it to 0, and the gauge reports 0 rather than omitting it
    stats.getBadMetaSystemStoreCounter().set(0);
    stats.setMeasured(true);
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName(),
        0,
        clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType.META_STORE));
    validateTehutiMetric(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.BAD_META_SYSTEM_STORE_COUNT,
        "Gauge",
        0.0);
  }

  @Test
  public void testEveryVeniceSystemStoreTypeEmitsADataPoint() {
    // Every VeniceSystemStoreType must have a live state resolver and backing counter.
    stats.getBadMetaSystemStoreCounter().set(1);
    stats.getBadPushStatusSystemStoreCounter().set(1);
    stats.setMeasured(true);

    String metricName =
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName();
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();

    for (VeniceSystemStoreType type: VeniceSystemStoreType.values()) {
      LongPointData point = OpenTelemetryDataTestUtils.getLongPointDataFromGaugeIfPresent(
          metrics,
          metricName,
          TEST_METRIC_PREFIX,
          clusterAndSystemStoreTypeAttributes(type));
      assertNotNull(
          point,
          "VeniceSystemStoreType." + type + " must emit a data point — add or fix the case in the "
              + "SystemStoreHealthCheckStats liveStateResolver switch (and a backing counter if needed).");
    }
  }

  @Test
  public void testGaugesReportOnlyWhileMeasured() {
    stats.getBadMetaSystemStoreCounter().set(3);
    stats.getNotRepairableSystemStoreCounter().set(5);
    stats.getSystemStoreHealthCheckErrorCounter().set(4);

    // A standby controller has never checked the cluster.
    assertNoGauges();
    validateTehutiMetric(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.BAD_META_SYSTEM_STORE_COUNT,
        "Gauge",
        3.0);

    stats.setMeasured(true);
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName(),
        3,
        clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType.META_STORE));
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNREPAIRABLE_COUNT
            .getMetricName(),
        5,
        clusterAttributes());
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_HEALTH_CHECK_ERROR_COUNT
            .getMetricName(),
        4,
        clusterAttributes());

    // Leadership moved to another controller.
    stats.setMeasured(false);
    assertNoGauges();
  }

  @Test
  public void testRoundInProgressKeepsReportingTheLastCompletedRound() {
    stats.getBadMetaSystemStoreCounter().set(3);
    stats.getNotRepairableSystemStoreCounter().set(5);
    stats.setMeasured(true);

    // The next round updates the bad-store count before its repairs finish.
    stats.getBadMetaSystemStoreCounter().set(7);
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName(),
        3,
        clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType.META_STORE));
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNREPAIRABLE_COUNT
            .getMetricName(),
        5,
        clusterAttributes());
    validateTehutiMetric(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.BAD_META_SYSTEM_STORE_COUNT,
        "Gauge",
        7.0);

    // The round completes with its repairs.
    stats.getNotRepairableSystemStoreCounter().set(2);
    stats.setMeasured(true);
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName(),
        7,
        clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType.META_STORE));
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNREPAIRABLE_COUNT
            .getMetricName(),
        2,
        clusterAttributes());
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    SystemStoreHealthCheckStats disabledStats = new SystemStoreHealthCheckStats(disabledRepo, TEST_CLUSTER_NAME);

    // Should execute without NPE
    disabledStats.getBadMetaSystemStoreCounter().set(1);
    disabledStats.getBadPushStatusSystemStoreCounter().set(2);
    disabledStats.getNotRepairableSystemStoreCounter().set(3);
    disabledStats.getSystemStoreHealthCheckErrorCounter().set(4);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    try {
      SystemStoreHealthCheckStats plainStats = new SystemStoreHealthCheckStats(plainRepo, TEST_CLUSTER_NAME);

      // Should execute without NPE
      plainStats.getBadMetaSystemStoreCounter().set(1);
      plainStats.getBadPushStatusSystemStoreCounter().set(2);
      plainStats.getNotRepairableSystemStoreCounter().set(3);
      plainStats.getSystemStoreHealthCheckErrorCounter().set(4);
    } finally {
      plainRepo.close();
    }
  }

  private static Attributes clusterAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }

  private static Attributes clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType systemStoreType) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_SYSTEM_STORE_TYPE.getDimensionNameInDefaultFormat(), systemStoreType.getDimensionValue())
        .build();
  }

  private void validateGauge(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void assertNoGauges() {
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    for (VeniceSystemStoreType type: VeniceSystemStoreType.values()) {
      OpenTelemetryDataTestUtils.assertNoDataPoint(
          metrics,
          SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT
              .getMetricName(),
          TEST_METRIC_PREFIX,
          clusterAndSystemStoreTypeAttributes(type));
    }
    OpenTelemetryDataTestUtils.assertNoDataPoint(
        metrics,
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNREPAIRABLE_COUNT
            .getMetricName(),
        TEST_METRIC_PREFIX,
        clusterAttributes());
    OpenTelemetryDataTestUtils.assertNoDataPoint(
        metrics,
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_HEALTH_CHECK_ERROR_COUNT
            .getMetricName(),
        TEST_METRIC_PREFIX,
        clusterAttributes());
  }

  private void validateTehutiMetric(
      SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum tehutiEnum,
      String statSuffix,
      double expectedValue) {
    String tehutiMetricName =
        AbstractVeniceStats.getSensorFullName(RESOURCE_NAME, tehutiEnum.getMetricName()) + "." + statSuffix;
    assertNotNull(metricsRepository.getMetric(tehutiMetricName), "Tehuti metric should exist: " + tehutiMetricName);
    assertEquals(
        metricsRepository.getMetric(tehutiMetricName).value(),
        expectedValue,
        "Tehuti metric value mismatch for: " + tehutiMetricName);
  }
}
