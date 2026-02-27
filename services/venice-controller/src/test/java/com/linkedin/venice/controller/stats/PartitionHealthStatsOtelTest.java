package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.pushmonitor.PushMonitor;
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
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PartitionHealthStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private static final String TEST_TOPIC_NAME = "test-store_v1";
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
  }

  @Test
  public void testRecordUnderReplicatedPartition() {
    AggPartitionHealthStats aggStats = createAggStats(metricsRepository);

    aggStats.reportUnderReplicatedPartition(TEST_TOPIC_NAME, 5);

    // OTel: gauge records the value with cluster+store attributes
    validateGauge(
        PartitionHealthStats.PartitionHealthOtelMetricEntity.PARTITION_UNDER_REPLICATED_COUNT.getMetricEntity()
            .getMetricName(),
        5,
        clusterAndStoreAttributes());

    // Tehuti: Max and Gauge should both be 5
    validateTehutiMetric(TEST_TOPIC_NAME, "Max", 5.0);
    validateTehutiMetric(TEST_TOPIC_NAME, "Gauge", 5.0);
  }

  @Test
  public void testRecordMultipleUnderReplicatedPartitions() {
    AggPartitionHealthStats aggStats = createAggStats(metricsRepository);

    // Record twice - gauge should show the latest value
    aggStats.reportUnderReplicatedPartition(TEST_TOPIC_NAME, 3);
    aggStats.reportUnderReplicatedPartition(TEST_TOPIC_NAME, 7);

    // OTel: gauge should show latest value (7)
    validateGauge(
        PartitionHealthStats.PartitionHealthOtelMetricEntity.PARTITION_UNDER_REPLICATED_COUNT.getMetricEntity()
            .getMetricName(),
        7,
        clusterAndStoreAttributes());

    // Tehuti: Max=7, Gauge=7 (latest)
    validateTehutiMetric(TEST_TOPIC_NAME, "Max", 7.0);
    validateTehutiMetric(TEST_TOPIC_NAME, "Gauge", 7.0);
  }

  @Test
  public void testRecordMultipleStores() {
    AggPartitionHealthStats aggStats = createAggStats(metricsRepository);

    String storeATopic = "store-a_v1";
    String storeBTopic = "store-b_v2";

    aggStats.reportUnderReplicatedPartition(storeATopic, 2);
    aggStats.reportUnderReplicatedPartition(storeBTopic, 4);

    String metricName =
        PartitionHealthStats.PartitionHealthOtelMetricEntity.PARTITION_UNDER_REPLICATED_COUNT.getMetricEntity()
            .getMetricName();

    // OTel: each store should have its own gauge data point with the correct attributes
    Attributes storeAAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "store-a")
        .build();
    validateGauge(metricName, 2, storeAAttributes);

    Attributes storeBAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "store-b")
        .build();
    validateGauge(metricName, 4, storeBAttributes);
  }

  @Test
  public void testZeroUnderReplicatedNotRecorded() {
    AggPartitionHealthStats aggStats = createAggStats(metricsRepository);

    // Zero count should not record (existing behavior)
    aggStats.reportUnderReplicatedPartition(TEST_TOPIC_NAME, 0);

    // OTel: no metrics should be recorded
    assertTrue(inMemoryMetricReader.collectAllMetrics().isEmpty());
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    AggPartitionHealthStats disabledStats = createAggStats(disabledRepo);

    disabledStats.reportUnderReplicatedPartition(TEST_TOPIC_NAME, 5);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = new MetricsRepository();
    AggPartitionHealthStats plainStats = createAggStats(plainRepo);

    plainStats.reportUnderReplicatedPartition(TEST_TOPIC_NAME, 5);
  }

  @Test
  public void testPartitionHealthOtelMetricEntity() {
    Map<PartitionHealthStats.PartitionHealthOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        PartitionHealthStats.PartitionHealthOtelMetricEntity.PARTITION_UNDER_REPLICATED_COUNT,
        new MetricEntity(
            "partition.under_replicated_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Partitions with fewer ready-to-serve replicas than the replication factor",
            Utils.setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));

    assertEquals(
        PartitionHealthStats.PartitionHealthOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New PartitionHealthOtelMetricEntity values were added but not included in this test");

    for (PartitionHealthStats.PartitionHealthOtelMetricEntity metric: PartitionHealthStats.PartitionHealthOtelMetricEntity
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

    // Verify all PartitionHealthOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
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

  private AggPartitionHealthStats createAggStats(MetricsRepository repo) {
    RoutingDataRepository mockRoutingDataRepo = Mockito.mock(RoutingDataRepository.class);
    ReadOnlyStoreRepository mockStoreRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    PushMonitor mockPushMonitor = Mockito.mock(PushMonitor.class);
    return new AggPartitionHealthStats(TEST_CLUSTER_NAME, repo, mockRoutingDataRepo, mockStoreRepo, mockPushMonitor);
  }

  private Attributes clusterAndStoreAttributes() {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
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

  private void validateTehutiMetric(String versionName, String statSuffix, double expectedValue) {
    String statsPrefix = "." + versionName;
    String tehutiMetricName =
        AbstractVeniceStats.getSensorFullName(statsPrefix, PartitionHealthStats.UNDER_REPLICATED_PARTITION_SENSOR) + "."
            + statSuffix;
    assertNotNull(metricsRepository.getMetric(tehutiMetricName), "Tehuti metric should exist: " + tehutiMetricName);
    assertEquals(
        metricsRepository.getMetric(tehutiMetricName).value(),
        expectedValue,
        "Tehuti metric value mismatch for: " + tehutiMetricName);
  }
}
