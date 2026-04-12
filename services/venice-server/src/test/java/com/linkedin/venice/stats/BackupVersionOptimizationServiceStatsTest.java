package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.BackupVersionOptimizationOtelMetricEntity.REOPEN_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_OPERATION_OUTCOME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.dimensions.VeniceOperationOutcome;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BackupVersionOptimizationServiceStatsTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private BackupVersionOptimizationServiceStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    stats = new BackupVersionOptimizationServiceStats(
        metricsRepository,
        "BackupVersionOptimizationService",
        TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  @Test
  public void testRecordOptimizationSuccess() {
    stats.recordBackupVersionDatabaseOptimization(TEST_STORE_NAME);
    stats.recordBackupVersionDatabaseOptimization(TEST_STORE_NAME);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        2,
        buildAttributes(TEST_STORE_NAME, VeniceOperationOutcome.SUCCESS),
        REOPEN_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testRecordOptimizationError() {
    stats.recordBackupVersionDatabaseOptimizationError(TEST_STORE_NAME);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildAttributes(TEST_STORE_NAME, VeniceOperationOutcome.FAIL),
        REOPEN_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testOutcomeDimensionIsolation() {
    stats.recordBackupVersionDatabaseOptimization(TEST_STORE_NAME);
    stats.recordBackupVersionDatabaseOptimization(TEST_STORE_NAME);
    stats.recordBackupVersionDatabaseOptimizationError(TEST_STORE_NAME);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        2,
        buildAttributes(TEST_STORE_NAME, VeniceOperationOutcome.SUCCESS),
        REOPEN_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildAttributes(TEST_STORE_NAME, VeniceOperationOutcome.FAIL),
        REOPEN_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testMultiStoreIsolation() {
    String storeA = "store-a";
    String storeB = "store-b";
    stats.recordBackupVersionDatabaseOptimization(storeA);
    stats.recordBackupVersionDatabaseOptimization(storeA);
    stats.recordBackupVersionDatabaseOptimization(storeB);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        2,
        buildAttributes(storeA, VeniceOperationOutcome.SUCCESS),
        REOPEN_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildAttributes(storeB, VeniceOperationOutcome.SUCCESS),
        REOPEN_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testTehutiSensorsRegisteredAndRecorded() {
    String successSensor = ".BackupVersionOptimizationService--backup_version_database_optimization.OccurrenceRate";
    String errorSensor = ".BackupVersionOptimizationService--backup_version_data_optimization_error.OccurrenceRate";

    // Sensors are registered lazily on first recording (per-store computeIfAbsent)
    stats.recordBackupVersionDatabaseOptimization(TEST_STORE_NAME);
    stats.recordBackupVersionDatabaseOptimizationError(TEST_STORE_NAME);

    assertNotNull(metricsRepository.getMetric(successSensor), "optimization sensor should exist after recording");
    assertNotNull(metricsRepository.getMetric(errorSensor), "optimization error sensor should exist after recording");
    assertTrue(metricsRepository.getMetric(successSensor).value() > 0, "optimization rate should be > 0");
    assertTrue(metricsRepository.getMetric(errorSensor).value() > 0, "optimization error rate should be > 0");
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      exerciseAllRecordingPaths(disabledRepo);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    exerciseAllRecordingPaths(new MetricsRepository());
  }

  private void exerciseAllRecordingPaths(MetricsRepository repo) {
    BackupVersionOptimizationServiceStats safeStats =
        new BackupVersionOptimizationServiceStats(repo, "BackupVersionOptimizationService", TEST_CLUSTER_NAME);
    safeStats.recordBackupVersionDatabaseOptimization(TEST_STORE_NAME);
    safeStats.recordBackupVersionDatabaseOptimizationError(TEST_STORE_NAME);
  }

  private Attributes buildAttributes(String storeName, VeniceOperationOutcome outcome) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_OPERATION_OUTCOME.getDimensionNameInDefaultFormat(), outcome.getDimensionValue())
        .build();
  }
}
