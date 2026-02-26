package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SYSTEM_STORE_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceSystemStoreType;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SystemStoreHealthCheckStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private InMemoryMetricReader inMemoryMetricReader;
  private SystemStoreHealthCheckStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new SystemStoreHealthCheckStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @Test
  public void testUnhealthyCountPerSystemStoreType() {
    stats.getBadMetaSystemStoreCounter().set(3);
    stats.getBadPushStatusSystemStoreCounter().set(7);

    // Validate META_STORE dimension
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName(),
        3,
        clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType.META_STORE));

    // Validate DAVINCI_PUSH_STATUS_STORE dimension
    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricName(),
        7,
        clusterAndSystemStoreTypeAttributes(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE));
  }

  @Test
  public void testUnrepairableCount() {
    stats.getNotRepairableSystemStoreCounter().set(5);

    validateGauge(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNREPAIRABLE_COUNT
            .getMetricName(),
        5,
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
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = new MetricsRepository();
    SystemStoreHealthCheckStats plainStats = new SystemStoreHealthCheckStats(plainRepo, TEST_CLUSTER_NAME);

    // Should execute without NPE
    plainStats.getBadMetaSystemStoreCounter().set(1);
    plainStats.getBadPushStatusSystemStoreCounter().set(2);
    plainStats.getNotRepairableSystemStoreCounter().set(3);
  }

  @Test
  public void testSystemStoreHealthCheckTehutiMetricNameEnum() {
    Map<SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.BAD_META_SYSTEM_STORE_COUNT,
        "bad_meta_system_store_count");
    expectedNames.put(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.BAD_PUSH_STATUS_SYSTEM_STORE_COUNT,
        "bad_push_status_system_store_count");
    expectedNames.put(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.NOT_REPAIRABLE_SYSTEM_STORE_COUNT,
        "not_repairable_system_store_count");

    assertEquals(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New SystemStoreHealthCheckTehutiMetricNameEnum values were added but not included in this test");

    for (SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum enumValue: SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testSystemStoreHealthCheckOtelMetricEntity() {
    Map<SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity, MetricEntity> expectedMetrics =
        new HashMap<>();
    expectedMetrics.put(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT,
        new MetricEntity(
            "system_store.health_check.unhealthy_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Unhealthy system stores, differentiated by system store type",
            Utils.setOf(VENICE_CLUSTER_NAME, VENICE_SYSTEM_STORE_TYPE)));
    expectedMetrics.put(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNREPAIRABLE_COUNT,
        new MetricEntity(
            "system_store.health_check.unrepairable_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "System stores that cannot be repaired",
            Utils.setOf(VENICE_CLUSTER_NAME)));

    assertEquals(
        SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New SystemStoreHealthCheckOtelMetricEntity values were added but not included in this test");

    for (SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity metric: SystemStoreHealthCheckStats.SystemStoreHealthCheckOtelMetricEntity
        .values()) {
      MetricEntity actual = metric.getMetricEntity();
      MetricEntity expected = expectedMetrics.get(metric);

      assertNotNull(expected, "No expected definition for " + metric.name());
      assertEquals(actual.getMetricName(), expected.getMetricName(), "Unexpected metric name for " + metric.name());
      assertEquals(actual.getMetricType(), expected.getMetricType(), "Unexpected metric type for " + metric.name());
      assertEquals(actual.getUnit(), expected.getUnit(), "Unexpected metric unit for " + metric.name());
      assertEquals(
          actual.getDescription(),
          expected.getDescription(),
          "Unexpected metric description for " + metric.name());
      assertEquals(
          actual.getDimensionsList(),
          expected.getDimensionsList(),
          "Unexpected metric dimensions for " + metric.name());
    }

    // Verify all entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
    for (MetricEntity expected: expectedMetrics.values()) {
      boolean found = false;
      for (MetricEntity actual: CONTROLLER_SERVICE_METRIC_ENTITIES) {
        if (Objects.equals(actual.getMetricName(), expected.getMetricName())
            && actual.getMetricType() == expected.getMetricType() && actual.getUnit() == expected.getUnit()
            && Objects.equals(actual.getDescription(), expected.getDescription())
            && Objects.equals(actual.getDimensionsList(), expected.getDimensionsList())) {
          found = true;
          break;
        }
      }
      assertTrue(found, "MetricEntity not found in CONTROLLER_SERVICE_METRIC_ENTITIES: " + expected.getMetricName());
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
}
