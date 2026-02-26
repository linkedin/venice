package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
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


public class DisabledPartitionStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private static final String RESOURCE_NAME = ".test-cluster";
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private DisabledPartitionStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new DisabledPartitionStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @Test
  public void testRecordDisabledPartition() {
    stats.recordDisabledPartition(TEST_STORE_NAME);

    // OTel
    validateCounter(
        DisabledPartitionStats.DisabledPartitionOtelMetricEntity.DISABLED_PARTITION_COUNT.getMetricName(),
        1,
        clusterAndStoreAttributes());

    // Tehuti
    validateTehutiMetric(
        DisabledPartitionStats.DisabledPartitionTehutiMetricNameEnum.DISABLED_PARTITION_COUNT,
        "Total",
        1.0);
  }

  @Test
  public void testRecordClearDisabledPartition() {
    stats.recordDisabledPartition(TEST_STORE_NAME);
    stats.recordDisabledPartition(TEST_STORE_NAME);
    stats.recordDisabledPartition(TEST_STORE_NAME);
    stats.recordClearDisabledPartition(2, TEST_STORE_NAME);

    // OTel: net = +3 - 2 = 1
    validateCounter(
        DisabledPartitionStats.DisabledPartitionOtelMetricEntity.DISABLED_PARTITION_COUNT.getMetricName(),
        1,
        clusterAndStoreAttributes());

    // Tehuti: Total sensor tracks net value
    validateTehutiMetric(
        DisabledPartitionStats.DisabledPartitionTehutiMetricNameEnum.DISABLED_PARTITION_COUNT,
        "Total",
        1.0);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    DisabledPartitionStats disabledStats = new DisabledPartitionStats(disabledRepo, TEST_CLUSTER_NAME);

    disabledStats.recordDisabledPartition(TEST_STORE_NAME);
    disabledStats.recordClearDisabledPartition(1, TEST_STORE_NAME);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = new MetricsRepository();
    DisabledPartitionStats plainStats = new DisabledPartitionStats(plainRepo, TEST_CLUSTER_NAME);

    plainStats.recordDisabledPartition(TEST_STORE_NAME);
    plainStats.recordClearDisabledPartition(1, TEST_STORE_NAME);
  }

  @Test
  public void testDisabledPartitionTehutiMetricNameEnum() {
    Map<DisabledPartitionStats.DisabledPartitionTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(
        DisabledPartitionStats.DisabledPartitionTehutiMetricNameEnum.DISABLED_PARTITION_COUNT,
        "disabled_partition_count");

    assertEquals(
        DisabledPartitionStats.DisabledPartitionTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New DisabledPartitionTehutiMetricNameEnum values were added but not included in this test");

    for (DisabledPartitionStats.DisabledPartitionTehutiMetricNameEnum enumValue: DisabledPartitionStats.DisabledPartitionTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testDisabledPartitionOtelMetricEntity() {
    Map<DisabledPartitionStats.DisabledPartitionOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        DisabledPartitionStats.DisabledPartitionOtelMetricEntity.DISABLED_PARTITION_COUNT,
        new MetricEntity(
            "partition.disabled_partition.count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Partition replicas disabled in Helix due to ingestion errors",
            Utils.setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));

    assertEquals(
        DisabledPartitionStats.DisabledPartitionOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New DisabledPartitionOtelMetricEntity values were added but not included in this test");

    for (DisabledPartitionStats.DisabledPartitionOtelMetricEntity metric: DisabledPartitionStats.DisabledPartitionOtelMetricEntity
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

    // Verify all DisabledPartitionOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
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

  private Attributes clusterAndStoreAttributes() {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
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

  private void validateTehutiMetric(
      DisabledPartitionStats.DisabledPartitionTehutiMetricNameEnum tehutiEnum,
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
