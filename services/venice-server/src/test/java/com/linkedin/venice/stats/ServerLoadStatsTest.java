package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.ServerLoadOtelMetricEntity.REJECTION_RATIO;
import static com.linkedin.davinci.stats.ServerLoadOtelMetricEntity.REQUEST_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SERVER_LOAD_REQUEST_OUTCOME;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.dimensions.VeniceServerLoadRequestOutcome;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerLoadStatsTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ServerLoadStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    stats = new ServerLoadStats(metricsRepository, "server_load", TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  // --- OTel counter tests with request outcome dimension ---

  @Test
  public void testRecordAcceptedRequest() {
    stats.recordAcceptedRequest();
    stats.recordAcceptedRequest();

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        2,
        buildRequestAttributes(VeniceServerLoadRequestOutcome.ACCEPTED),
        REQUEST_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testRecordRejectedRequest() {
    stats.recordRejectedRequest();

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildRequestAttributes(VeniceServerLoadRequestOutcome.REJECTED),
        REQUEST_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testRequestOutcomeDimensionIsolation() {
    stats.recordAcceptedRequest();
    stats.recordAcceptedRequest();
    stats.recordRejectedRequest();

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        2,
        buildRequestAttributes(VeniceServerLoadRequestOutcome.ACCEPTED),
        REQUEST_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildRequestAttributes(VeniceServerLoadRequestOutcome.REJECTED),
        REQUEST_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  // --- OTel MIN_MAX_COUNT_SUM test for rejection ratio ---

  @Test
  public void testRecordRejectionRatio() {
    stats.recordRejectionRatio(0.25);
    stats.recordRejectionRatio(0.75);

    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        0.25,
        0.75,
        2,
        1.0,
        buildClusterAttributes(),
        REJECTION_RATIO.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  // --- Tehuti sensor tests ---

  @Test
  public void testTehutiSensorsRegisteredAndRecorded() {
    String totalSensor = ".server_load--total_request.OccurrenceRate";
    String rejectedSensor = ".server_load--rejected_request.OccurrenceRate";
    String acceptedSensor = ".server_load--accepted_request.OccurrenceRate";
    String ratioAvgSensor = ".server_load--rejection_ratio.Avg";
    String ratioMaxSensor = ".server_load--rejection_ratio.Max";

    assertNotNull(metricsRepository.getMetric(totalSensor), "total_request sensor should exist");
    assertNotNull(metricsRepository.getMetric(rejectedSensor), "rejected_request sensor should exist");
    assertNotNull(metricsRepository.getMetric(acceptedSensor), "accepted_request sensor should exist");
    assertNotNull(metricsRepository.getMetric(ratioAvgSensor), "rejection_ratio Avg sensor should exist");
    assertNotNull(metricsRepository.getMetric(ratioMaxSensor), "rejection_ratio Max sensor should exist");

    stats.recordTotalRequest();
    stats.recordRejectedRequest();
    stats.recordAcceptedRequest();
    stats.recordRejectionRatio(0.5);

    assertTrue(metricsRepository.getMetric(totalSensor).value() > 0, "total_request rate should be > 0");
    assertTrue(metricsRepository.getMetric(rejectedSensor).value() > 0, "rejected_request rate should be > 0");
    assertTrue(metricsRepository.getMetric(acceptedSensor).value() > 0, "accepted_request rate should be > 0");
    assertTrue(metricsRepository.getMetric(ratioAvgSensor).value() > 0, "rejection_ratio Avg should be > 0");
    assertTrue(metricsRepository.getMetric(ratioMaxSensor).value() > 0, "rejection_ratio Max should be > 0");
  }

  // --- Double-counting prevention: recordTotalRequest() must NOT produce OTel data ---

  @Test
  public void testTotalRequestDoesNotProduceOtelData() {
    stats.recordTotalRequest();
    stats.recordTotalRequest();

    // OTel should have NO data for request count (total is Tehuti-only)
    OpenTelemetryDataTestUtils.assertNoLongSumDataForAttributes(
        inMemoryMetricReader.collectAllMetrics(),
        REQUEST_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX,
        buildRequestAttributes(VeniceServerLoadRequestOutcome.ACCEPTED));
    OpenTelemetryDataTestUtils.assertNoLongSumDataForAttributes(
        inMemoryMetricReader.collectAllMetrics(),
        REQUEST_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX,
        buildRequestAttributes(VeniceServerLoadRequestOutcome.REJECTED));
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
    ServerLoadStats safeStats = new ServerLoadStats(repo, "server_load", TEST_CLUSTER_NAME);
    safeStats.recordTotalRequest();
    safeStats.recordRejectedRequest();
    safeStats.recordAcceptedRequest();
    safeStats.recordRejectionRatio(0.5);
  }

  // --- Helpers ---

  private Attributes buildClusterAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }

  private Attributes buildRequestAttributes(VeniceServerLoadRequestOutcome outcome) {
    return buildClusterAttributes().toBuilder()
        .put(VENICE_SERVER_LOAD_REQUEST_OUTCOME.getDimensionNameInDefaultFormat(), outcome.getDimensionValue())
        .build();
  }
}
