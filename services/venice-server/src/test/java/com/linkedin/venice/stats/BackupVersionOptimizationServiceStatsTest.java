package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.BackupVersionOptimizationOtelMetricEntity.REOPEN_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_OPERATION_OUTCOME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.dimensions.VeniceOperationOutcome;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
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
  // Dedicated executor avoids shutting down Tehuti's static DEFAULT_ASYNC_GAUGE_EXECUTOR singleton when this
  // repository is closed in tearDown(). Closing the static executor would break AsyncGauge measurements
  // for any subsequent test running in the same JVM.
  private AsyncGauge.AsyncGaugeExecutor asyncGaugeExecutor;

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
    stats = new BackupVersionOptimizationServiceStats(
        metricsRepository,
        "BackupVersionOptimizationService",
        TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    // Closes only the dedicated executor; the JVM-wide static executor stays alive for other tests.
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

    stats.recordBackupVersionDatabaseOptimization(TEST_STORE_NAME);
    stats.recordBackupVersionDatabaseOptimizationError(TEST_STORE_NAME);

    assertNotNull(metricsRepository.getMetric(successSensor), "optimization sensor should exist after recording");
    assertNotNull(metricsRepository.getMetric(errorSensor), "optimization error sensor should exist after recording");
    assertTrue(metricsRepository.getMetric(successSensor).value() > 0, "optimization rate should be > 0");
    assertTrue(metricsRepository.getMetric(errorSensor).value() > 0, "optimization error rate should be > 0");
  }

  @Test
  public void testTehutiTwoMapSplitNoCrossContamination() {
    String successSensor = ".BackupVersionOptimizationService--backup_version_database_optimization.OccurrenceRate";
    String errorSensor = ".BackupVersionOptimizationService--backup_version_data_optimization_error.OccurrenceRate";

    // Register both Tehuti sensors by exercising each path once.
    stats.recordBackupVersionDatabaseOptimization(TEST_STORE_NAME);
    stats.recordBackupVersionDatabaseOptimizationError(TEST_STORE_NAME);

    double errorBeforeAdditionalSuccesses = metricsRepository.getMetric(errorSensor).value();
    // Recording success many times via the success path must not increment the error sensor.
    for (int i = 0; i < 10; i++) {
      stats.recordBackupVersionDatabaseOptimization(TEST_STORE_NAME);
    }
    // OccurrenceRate is a sliding-window rate; values can drift in the lowest decimals between
    // reads as the clock advances. Tolerate that drift here — the assertion targets cross-sensor
    // contamination, not exact rate values.
    assertEquals(
        metricsRepository.getMetric(errorSensor).value(),
        errorBeforeAdditionalSuccesses,
        1e-3,
        "Error Tehuti sensor should not be incremented by success recordings (two-map split invariant)");

    // Symmetrically, recording errors must not increment the success sensor.
    double successBeforeAdditionalErrors = metricsRepository.getMetric(successSensor).value();
    for (int i = 0; i < 10; i++) {
      stats.recordBackupVersionDatabaseOptimizationError(TEST_STORE_NAME);
    }
    assertEquals(
        metricsRepository.getMetric(successSensor).value(),
        successBeforeAdditionalErrors,
        1e-3,
        "Success Tehuti sensor should not be incremented by error recordings (two-map split invariant)");
  }

  /**
   * Verifies the cluster-name constructor argument actually propagates into the OTel
   * {@code baseDimensionsMap} (and thus into emitted attributes). The other tests all use the
   * same {@code TEST_CLUSTER_NAME} for both setup and assertion, which is tautological — a
   * future bug that hardcoded a wrong cluster name in the dimensions map would still pass them.
   */
  @Test
  public void testClusterNameArgumentPropagatesToOtelAttributes() {
    String differentCluster = "another-cluster";
    AsyncGauge.AsyncGaugeExecutor localExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    InMemoryMetricReader localReader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository localRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(localReader)
            .setTehutiMetricConfig(new MetricConfig(localExecutor))
            .build())) {
      BackupVersionOptimizationServiceStats localStats =
          new BackupVersionOptimizationServiceStats(localRepo, "BackupVersionOptimizationService", differentCluster);
      localStats.recordBackupVersionDatabaseOptimization(TEST_STORE_NAME);

      Attributes expected = Attributes.builder()
          .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), differentCluster)
          .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
          .put(
              VENICE_OPERATION_OUTCOME.getDimensionNameInDefaultFormat(),
              VeniceOperationOutcome.SUCCESS.getDimensionValue())
          .build();
      OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
          localReader,
          1,
          expected,
          REOPEN_COUNT.getMetricEntity().getMetricName(),
          TEST_METRIC_PREFIX);
    }
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    AsyncGauge.AsyncGaugeExecutor localExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setEmitOtelMetrics(false)
            .setTehutiMetricConfig(new MetricConfig(localExecutor))
            .build())) {
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
