package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.OpenTelemetryMetricsSetup.UNKNOWN_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ParticipantStoreConsumptionStatsTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private static final String TEST_STORE_NAME_2 = "test-store-2";
  // OTel metric names (extracted to reduce verbose .getMetricEntity().getMetricName() chains)
  private static final String OTEL_HEARTBEAT =
      ParticipantStoreConsumptionOtelMetricEntity.HEARTBEAT_COUNT.getMetricEntity().getMetricName();
  private static final String OTEL_FAILED_INIT =
      ParticipantStoreConsumptionOtelMetricEntity.FAILED_INITIALIZATION_COUNT.getMetricEntity().getMetricName();
  private static final String OTEL_KILL_COUNT =
      ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity().getMetricName();
  private static final String OTEL_KILL_LATENCY =
      ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_LATENCY.getMetricEntity().getMetricName();
  private static final String OTEL_FAILED_CONSUMPTION =
      ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_FAILED_CONSUMPTION_COUNT.getMetricEntity()
          .getMetricName();

  // Tehuti metric names
  private static final String TEHUTI_KILLED_PUSH_JOBS_METRIC =
      ".test-cluster-participant_store_consumption_task--killed_push_jobs.Count";
  private static final String TEHUTI_KILL_LATENCY_AVG_METRIC =
      ".test-cluster-participant_store_consumption_task--kill_push_job_latency.Avg";
  private static final String TEHUTI_KILL_LATENCY_MAX_METRIC =
      ".test-cluster-participant_store_consumption_task--kill_push_job_latency.Max";
  private static final String TEHUTI_FAILED_CONSUMPTION_METRIC =
      ".test-cluster-participant_store_consumption_task--kill_push_job_failed_consumption.Count";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ParticipantStoreConsumptionStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());
    stats = new ParticipantStoreConsumptionStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  // --- OTel recording tests ---

  @Test
  public void testRecordHeartbeat() {
    stats.recordHeartbeat();

    validateCounter(OTEL_HEARTBEAT, 1, buildClusterOnlyAttributes());
  }

  @Test
  public void testRecordFailedInitialization() {
    stats.recordFailedInitialization();

    validateCounter(OTEL_FAILED_INIT, 1, buildClusterOnlyAttributes());
  }

  @Test
  public void testRecordKilledPushJobs() {
    stats.recordKilledPushJobs(TEST_STORE_NAME);

    validateCounter(
        OTEL_KILL_COUNT,
        1,
        buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));
  }

  @Test
  public void testRecordFailedKillPushJob() {
    stats.recordFailedKillPushJob(TEST_STORE_NAME);

    validateCounter(OTEL_KILL_COUNT, 1, buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.FAIL));
  }

  @Test
  public void testRecordKillPushJobFailedConsumption() {
    stats.recordKillPushJobFailedConsumption(TEST_STORE_NAME);

    validateCounter(OTEL_FAILED_CONSUMPTION, 1, buildStoreOnlyAttributes(TEST_STORE_NAME));
  }

  @Test
  public void testRecordKillPushJobFailedConsumptionWithUnknownStore() {
    stats.recordKillPushJobFailedConsumption(UNKNOWN_STORE_NAME);

    validateCounter(OTEL_FAILED_CONSUMPTION, 1, buildStoreOnlyAttributes(UNKNOWN_STORE_NAME));
  }

  /**
   * When OTel is enabled, empty store names are rejected by Venice's dimension validation in
   * {@link com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository}.
   * Callers must sanitize to {@link com.linkedin.venice.stats.OpenTelemetryMetricsSetup#UNKNOWN_STORE_NAME}
   * before passing to recording methods.
   */
  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Dimension value cannot be null or empty.*")
  public void testRecordWithEmptyStoreNameThrowsWhenOtelEnabled() {
    stats.recordKillPushJobFailedConsumption("");
  }

  /** When OTel is disabled, recording methods tolerate empty store names because dimension validation is skipped. */
  @Test
  public void testRecordWithEmptyStoreNameSafeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      ParticipantStoreConsumptionStats safeStats =
          new ParticipantStoreConsumptionStats(disabledRepo, TEST_CLUSTER_NAME);
      safeStats.recordKillPushJobFailedConsumption("");
    }
  }

  @Test
  public void testRecordKillPushJobLatency() {
    double latency = 150.0;
    stats.recordKillPushJobLatency(TEST_STORE_NAME, latency);

    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        latency,
        latency,
        1,
        latency,
        buildStoreOnlyAttributes(TEST_STORE_NAME),
        OTEL_KILL_LATENCY,
        TEST_METRIC_PREFIX);
  }

  // --- Accumulation tests ---

  @Test
  public void testCounterAccumulation() {
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    stats.recordKilledPushJobs(TEST_STORE_NAME);

    validateCounter(
        OTEL_KILL_COUNT,
        3,
        buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));
  }

  @Test
  public void testHeartbeatAccumulation() {
    stats.recordHeartbeat();
    stats.recordHeartbeat();

    validateCounter(OTEL_HEARTBEAT, 2, buildClusterOnlyAttributes());
  }

  @Test
  public void testFailedInitializationAccumulation() {
    stats.recordFailedInitialization();
    stats.recordFailedInitialization();
    stats.recordFailedInitialization();

    validateCounter(OTEL_FAILED_INIT, 3, buildClusterOnlyAttributes());
  }

  @Test
  public void testKillPushJobLatencyHistogramMultipleRecordings() {
    stats.recordKillPushJobLatency(TEST_STORE_NAME, 100.0);
    stats.recordKillPushJobLatency(TEST_STORE_NAME, 200.0);

    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        100.0,
        200.0,
        2,
        300.0,
        buildStoreOnlyAttributes(TEST_STORE_NAME),
        OTEL_KILL_LATENCY,
        TEST_METRIC_PREFIX);
  }

  // --- Isolation and negative checks ---

  /** SUCCESS and FAIL share the same OTel metric name but use separate per-store maps; verify no crosstalk. */
  @Test
  public void testSuccessAndFailKillCountsAreIndependent() {
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    stats.recordFailedKillPushJob(TEST_STORE_NAME);

    validateCounter(
        OTEL_KILL_COUNT,
        2,
        buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));
    validateCounter(OTEL_KILL_COUNT, 1, buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.FAIL));
  }

  /**
   * Verifies that {@code recordFailedKillPushJob} (OTel-only) does NOT increment the Tehuti
   * {@code killed_push_jobs} sensor. This guards the two-map split invariant:
   * {@code killedPushJobsPerStore} (joint Tehuti+OTel) vs {@code failedKillPushJobPerStore} (OTel-only).
   */
  @Test
  public void testFailedKillPushJobDoesNotIncrementTehutiKilledPushJobs() {
    // Register the Tehuti sensor by recording a success first
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    double valueAfterSuccess = metricsRepository.getMetric(TEHUTI_KILLED_PUSH_JOBS_METRIC).value();
    assertTrue(valueAfterSuccess > 0, "Tehuti sensor should have a positive value after recording success");

    // Record multiple failures — these must NOT affect the Tehuti sensor
    stats.recordFailedKillPushJob(TEST_STORE_NAME);
    stats.recordFailedKillPushJob(TEST_STORE_NAME);
    stats.recordFailedKillPushJob(TEST_STORE_NAME);

    // Tehuti killed_push_jobs count must remain unchanged
    assertEquals(
        metricsRepository.getMetric(TEHUTI_KILLED_PUSH_JOBS_METRIC).value(),
        valueAfterSuccess,
        "Tehuti killed_push_jobs sensor must not be incremented by recordFailedKillPushJob");
  }

  /** Verifies that the Tehuti Avg and Max sensors are correctly wired through the per-store base metric factory. */
  @Test
  public void testTehutiKillPushJobLatencyValues() {
    stats.recordKillPushJobLatency(TEST_STORE_NAME, 100.0);
    stats.recordKillPushJobLatency(TEST_STORE_NAME, 200.0);

    double avg = metricsRepository.getMetric(TEHUTI_KILL_LATENCY_AVG_METRIC).value();
    double max = metricsRepository.getMetric(TEHUTI_KILL_LATENCY_MAX_METRIC).value();
    assertEquals(avg, 150.0, "Tehuti kill latency Avg should be the mean of 100 and 200");
    assertEquals(max, 200.0, "Tehuti kill latency Max should be the maximum recorded value");
  }

  /** Verifies that the Tehuti Count sensor for failed consumption is correctly wired through the per-store factory. */
  @Test
  public void testTehutiKillPushJobFailedConsumptionCount() {
    stats.recordKillPushJobFailedConsumption(TEST_STORE_NAME);
    stats.recordKillPushJobFailedConsumption(TEST_STORE_NAME);

    double count = metricsRepository.getMetric(TEHUTI_FAILED_CONSUMPTION_METRIC).value();
    assertEquals(count, 2.0, "Tehuti kill_push_job_failed_consumption Count should be 2 after two recordings");
  }

  /** Verifies OTel counter accumulation for the OTel-only FAIL path. */
  @Test
  public void testFailedKillPushJobAccumulation() {
    stats.recordFailedKillPushJob(TEST_STORE_NAME);
    stats.recordFailedKillPushJob(TEST_STORE_NAME);
    stats.recordFailedKillPushJob(TEST_STORE_NAME);

    validateCounter(OTEL_KILL_COUNT, 3, buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.FAIL));
  }

  @Test
  public void testMultipleStoresAreIndependent() {
    // Kill count
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    stats.recordKilledPushJobs(TEST_STORE_NAME_2);

    validateCounter(
        OTEL_KILL_COUNT,
        2,
        buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));
    validateCounter(
        OTEL_KILL_COUNT,
        1,
        buildStoreStatusAttributes(TEST_STORE_NAME_2, VeniceResponseStatusCategory.SUCCESS));

    // Kill latency
    stats.recordKillPushJobLatency(TEST_STORE_NAME, 100.0);
    stats.recordKillPushJobLatency(TEST_STORE_NAME_2, 200.0);

    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        100.0,
        100.0,
        1,
        100.0,
        buildStoreOnlyAttributes(TEST_STORE_NAME),
        OTEL_KILL_LATENCY,
        TEST_METRIC_PREFIX);
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        200.0,
        200.0,
        1,
        200.0,
        buildStoreOnlyAttributes(TEST_STORE_NAME_2),
        OTEL_KILL_LATENCY,
        TEST_METRIC_PREFIX);

    // Failed consumption
    stats.recordKillPushJobFailedConsumption(TEST_STORE_NAME);
    stats.recordKillPushJobFailedConsumption(TEST_STORE_NAME);
    stats.recordKillPushJobFailedConsumption(TEST_STORE_NAME_2);

    validateCounter(OTEL_FAILED_CONSUMPTION, 2, buildStoreOnlyAttributes(TEST_STORE_NAME));
    validateCounter(OTEL_FAILED_CONSUMPTION, 1, buildStoreOnlyAttributes(TEST_STORE_NAME_2));
  }

  // --- NPE prevention tests ---

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      assertAllMethodsSafe(disabledRepo);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    assertAllMethodsSafe(MetricsRepositoryUtils.createSingleThreadedMetricsRepository());
  }

  private void assertAllMethodsSafe(MetricsRepository repo) {
    ParticipantStoreConsumptionStats safeStats = new ParticipantStoreConsumptionStats(repo, TEST_CLUSTER_NAME);
    safeStats.recordHeartbeat();
    safeStats.recordFailedInitialization();
    safeStats.recordKilledPushJobs(TEST_STORE_NAME);
    safeStats.recordFailedKillPushJob(TEST_STORE_NAME);
    safeStats.recordKillPushJobLatency(TEST_STORE_NAME, 100.0);
    safeStats.recordKillPushJobFailedConsumption(TEST_STORE_NAME);
    safeStats.recordKillPushJobFailedConsumption(UNKNOWN_STORE_NAME);
  }

  // --- Helper methods ---

  private Attributes buildClusterOnlyAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }

  private Attributes buildStoreOnlyAttributes(String storeName) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .build();
  }

  private Attributes buildStoreStatusAttributes(String storeName, VeniceResponseStatusCategory statusCategory) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), statusCategory.getDimensionValue())
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
}
