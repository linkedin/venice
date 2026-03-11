package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT;
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
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
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
  public void testZeroUnderReplicatedRecordsBothOtelAndTehuti() {
    AggPartitionHealthStats aggStats = createAggStats(metricsRepository);

    // Zero count: both OTel and per-store Tehuti should record to reflect recovery.
    // Only total Tehuti stats are skipped (no warn log for healthy partitions).
    aggStats.reportUnderReplicatedPartition(TEST_TOPIC_NAME, 0);

    // OTel: gauge records zero to show the partition is healthy
    validateGauge(
        PartitionHealthStats.PartitionHealthOtelMetricEntity.PARTITION_UNDER_REPLICATED_COUNT.getMetricEntity()
            .getMetricName(),
        0,
        clusterAndStoreAttributes());

    // Tehuti: per-store sensor also records zero
    validateTehutiMetric(TEST_TOPIC_NAME, "Max", 0.0);
    validateTehutiMetric(TEST_TOPIC_NAME, "Gauge", 0.0);
  }

  @Test
  public void testOtelGaugeReflectsRecovery() {
    AggPartitionHealthStats aggStats = createAggStats(metricsRepository);
    String metricName =
        PartitionHealthStats.PartitionHealthOtelMetricEntity.PARTITION_UNDER_REPLICATED_COUNT.getMetricEntity()
            .getMetricName();

    // Initially under-replicated
    aggStats.reportUnderReplicatedPartition(TEST_TOPIC_NAME, 5);
    validateGauge(metricName, 5, clusterAndStoreAttributes());

    // Partition recovers — gauge should drop to 0
    aggStats.reportUnderReplicatedPartition(TEST_TOPIC_NAME, 0);
    validateGauge(metricName, 0, clusterAndStoreAttributes());
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    verifyNoNpeWithRepository(disabledRepo);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    verifyNoNpeWithRepository(new MetricsRepository());
  }

  @Test
  public void testTotalStatsDoNotEmitOtelMetrics() {
    // Per-cluster total stats name follows the pattern "total.<clusterName>" from AbstractVeniceAggStats.
    // OTel should be suppressed for total/aggregate stats to avoid duplicate metric inflation.
    String totalStatsName = STORE_NAME_FOR_TOTAL_STAT + "." + TEST_CLUSTER_NAME;
    PartitionHealthStats totalStats = new PartitionHealthStats(metricsRepository, totalStatsName, TEST_CLUSTER_NAME);

    totalStats.recordUnderReplicatePartition(5);

    // OTel: no gauge data points should exist for this metric (total stats suppresses OTel)
    String metricName =
        PartitionHealthStats.PartitionHealthOtelMetricEntity.PARTITION_UNDER_REPLICATED_COUNT.getMetricEntity()
            .getMetricName();
    String fullMetricName = TEST_METRIC_PREFIX + "." + metricName;
    Collection<MetricData> allMetrics = inMemoryMetricReader.collectAllMetrics();
    boolean found = false;
    for (MetricData md: allMetrics) {
      if (md.getName().equals(fullMetricName)) {
        found = true;
        break;
      }
    }
    assertTrue(!found, "OTel metric should NOT be emitted for total stats, but found: " + fullMetricName);

    // Tehuti: should still work. AbstractVeniceStats replaces dots with underscores in the name,
    // so the sensor prefix uses the transformed name.
    String statsPrefix = "." + totalStatsName.replace(".", "_");
    String sensorName =
        PartitionHealthStats.PartitionHealthTehutiMetricNameEnum.UNDER_REPLICATED_PARTITION.getMetricName();
    String tehutiMetricName = AbstractVeniceStats.getSensorFullName(statsPrefix, sensorName) + ".Gauge";
    assertNotNull(metricsRepository.getMetric(tehutiMetricName), "Tehuti metric should exist: " + tehutiMetricName);
    assertEquals(
        metricsRepository.getMetric(tehutiMetricName).value(),
        5.0,
        "Tehuti metric value mismatch for total stats");
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
    String sensorName =
        PartitionHealthStats.PartitionHealthTehutiMetricNameEnum.UNDER_REPLICATED_PARTITION.getMetricName();
    String tehutiMetricName = AbstractVeniceStats.getSensorFullName(statsPrefix, sensorName) + "." + statSuffix;
    assertNotNull(metricsRepository.getMetric(tehutiMetricName), "Tehuti metric should exist: " + tehutiMetricName);
    assertEquals(
        metricsRepository.getMetric(tehutiMetricName).value(),
        expectedValue,
        "Tehuti metric value mismatch for: " + tehutiMetricName);
  }

  private void verifyNoNpeWithRepository(MetricsRepository repo) {
    AggPartitionHealthStats localStats = createAggStats(repo);
    localStats.reportUnderReplicatedPartition(TEST_TOPIC_NAME, 5);
  }
}
