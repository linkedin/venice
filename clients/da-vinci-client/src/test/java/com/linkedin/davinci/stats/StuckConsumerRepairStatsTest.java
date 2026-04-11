package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StuckConsumerRepairStatsTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private StuckConsumerRepairStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    stats = new StuckConsumerRepairStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  // --- Positive tests: OTel counter accumulation ---

  @Test
  public void testRecordStuckConsumerFound() {
    stats.recordStuckConsumerFound();
    stats.recordStuckConsumerFound();

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        2,
        buildAttributes(),
        StuckConsumerRepairOtelMetricEntity.STUCK_CONSUMER_DETECTED_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testRecordIngestionTaskRepair() {
    stats.recordIngestionTaskRepair();
    stats.recordIngestionTaskRepair();
    stats.recordIngestionTaskRepair();

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        3,
        buildAttributes(),
        StuckConsumerRepairOtelMetricEntity.STUCK_CONSUMER_TASK_REPAIRED_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testRecordRepairFailure() {
    stats.recordRepairFailure();

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildAttributes(),
        StuckConsumerRepairOtelMetricEntity.STUCK_CONSUMER_UNRESOLVED_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  // --- Positive tests: Tehuti sensor existence and recording ---

  @Test
  public void testTehutiSensorsRegisteredAndRecorded() {
    // OccurrenceRate sensors use the ".OccurrenceRate" suffix
    String foundSensor = ".StuckConsumerRepair--stuck_consumer_found.OccurrenceRate";
    String repairSensor = ".StuckConsumerRepair--ingestion_task_repair.OccurrenceRate";
    String failureSensor = ".StuckConsumerRepair--repair_failure.OccurrenceRate";

    assertNotNull(metricsRepository.getMetric(foundSensor), "Tehuti sensor should exist for stuck_consumer_found");
    assertNotNull(metricsRepository.getMetric(repairSensor), "Tehuti sensor should exist for ingestion_task_repair");
    assertNotNull(metricsRepository.getMetric(failureSensor), "Tehuti sensor should exist for repair_failure");

    // Record and verify values are non-zero (OccurrenceRate is time-dependent, so just assert > 0)
    stats.recordStuckConsumerFound();
    stats.recordIngestionTaskRepair();
    stats.recordRepairFailure();
    assertTrue(metricsRepository.getMetric(foundSensor).value() > 0, "stuck_consumer_found rate should be > 0");
    assertTrue(metricsRepository.getMetric(repairSensor).value() > 0, "ingestion_task_repair rate should be > 0");
    assertTrue(metricsRepository.getMetric(failureSensor).value() > 0, "repair_failure rate should be > 0");
  }

  // --- NPE prevention tests ---

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
    StuckConsumerRepairStats safeStats = new StuckConsumerRepairStats(repo, TEST_CLUSTER_NAME);
    safeStats.recordStuckConsumerFound();
    safeStats.recordIngestionTaskRepair();
    safeStats.recordRepairFailure();
  }

  // --- Helper ---

  private Attributes buildAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }
}
