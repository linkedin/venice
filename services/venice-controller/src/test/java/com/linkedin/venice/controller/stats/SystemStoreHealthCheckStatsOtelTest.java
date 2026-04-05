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
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
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
  public void testCounterResetToZero() {
    stats.getBadMetaSystemStoreCounter().set(5);
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName(),
        5,
        clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType.META_STORE));

    // Reset to 0 and verify the gauge reflects the updated value
    stats.getBadMetaSystemStoreCounter().set(0);
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
  public void testVeniceSystemStoreTypeMappingIsComplete() {
    // Guards against adding new VeniceSystemStoreType values without updating the switch
    // statements in SystemStoreHealthCheckStats. If a new value is added, the constructor
    // will throw IllegalArgumentException at startup, and this test makes the intent explicit.
    assertEquals(
        VeniceSystemStoreType.values().length,
        2,
        "New VeniceSystemStoreType values were added; update the Tehuti sensor registration and "
            + "OTel callbackProvider switch in SystemStoreHealthCheckStats");
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
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    SystemStoreHealthCheckStats plainStats = new SystemStoreHealthCheckStats(plainRepo, TEST_CLUSTER_NAME);

    // Should execute without NPE
    plainStats.getBadMetaSystemStoreCounter().set(1);
    plainStats.getBadPushStatusSystemStoreCounter().set(2);
    plainStats.getNotRepairableSystemStoreCounter().set(3);
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
