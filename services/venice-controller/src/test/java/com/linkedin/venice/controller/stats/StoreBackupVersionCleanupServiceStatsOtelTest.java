package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
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


public class StoreBackupVersionCleanupServiceStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private InMemoryMetricReader inMemoryMetricReader;
  private StoreBackupVersionCleanupServiceStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new StoreBackupVersionCleanupServiceStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @Test
  public void testRecordBackupVersionMismatch() {
    stats.recordBackupVersionMismatch();
    validateCounter(
        StoreBackupVersionCleanupServiceStats.BackupVersionCleanupOtelMetricEntity.BACKUP_VERSION_CLEANUP_MISMATCH_COUNT
            .getMetricName(),
        1,
        Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build());
  }

  @Test
  public void testRecordMultipleMismatches() {
    stats.recordBackupVersionMismatch();
    stats.recordBackupVersionMismatch();
    stats.recordBackupVersionMismatch();
    validateCounter(
        StoreBackupVersionCleanupServiceStats.BackupVersionCleanupOtelMetricEntity.BACKUP_VERSION_CLEANUP_MISMATCH_COUNT
            .getMetricName(),
        3,
        Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build());
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    StoreBackupVersionCleanupServiceStats disabledStats =
        new StoreBackupVersionCleanupServiceStats(disabledRepo, TEST_CLUSTER_NAME);

    // Should execute without NPE
    disabledStats.recordBackupVersionMismatch();
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    // When using a plain Tehuti MetricsRepository (not VeniceMetricsRepository),
    // OTel should be disabled and methods should not throw
    MetricsRepository plainRepo = new MetricsRepository();
    StoreBackupVersionCleanupServiceStats plainStats =
        new StoreBackupVersionCleanupServiceStats(plainRepo, TEST_CLUSTER_NAME);

    plainStats.recordBackupVersionMismatch();
  }

  @Test
  public void testBackupVersionCleanupTehutiMetricNameEnum() {
    Map<StoreBackupVersionCleanupServiceStats.BackupVersionCleanupTehutiMetricNameEnum, String> expectedNames =
        new HashMap<>();
    expectedNames.put(
        StoreBackupVersionCleanupServiceStats.BackupVersionCleanupTehutiMetricNameEnum.BACKUP_VERSION_CLEANUP_VERSION_MISMATCH,
        "backup_version_cleanup_version_mismatch");

    assertEquals(
        StoreBackupVersionCleanupServiceStats.BackupVersionCleanupTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New BackupVersionCleanupTehutiMetricNameEnum values were added but not included in this test");

    for (StoreBackupVersionCleanupServiceStats.BackupVersionCleanupTehutiMetricNameEnum enumValue: StoreBackupVersionCleanupServiceStats.BackupVersionCleanupTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testBackupVersionCleanupOtelMetricEntity() {
    Map<StoreBackupVersionCleanupServiceStats.BackupVersionCleanupOtelMetricEntity, MetricEntity> expectedMetrics =
        new HashMap<>();
    expectedMetrics.put(
        StoreBackupVersionCleanupServiceStats.BackupVersionCleanupOtelMetricEntity.BACKUP_VERSION_CLEANUP_MISMATCH_COUNT,
        new MetricEntity(
            "backup_version_cleanup_service.version_mismatch_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of backup version cleanup version mismatches",
            Utils.setOf(VeniceMetricsDimensions.VENICE_CLUSTER_NAME)));

    assertEquals(
        StoreBackupVersionCleanupServiceStats.BackupVersionCleanupOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New BackupVersionCleanupOtelMetricEntity values were added but not included in this test");

    for (StoreBackupVersionCleanupServiceStats.BackupVersionCleanupOtelMetricEntity metric: StoreBackupVersionCleanupServiceStats.BackupVersionCleanupOtelMetricEntity
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

    // Verify all BackupVersionCleanupOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
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

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
