package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.OpenTelemetryMetricsSetup.UNKNOWN_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
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

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.HEARTBEAT_COUNT.getMetricEntity().getMetricName(),
        1,
        buildClusterOnlyAttributes());
  }

  @Test
  public void testRecordFailedInitialization() {
    stats.recordFailedInitialization();

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.FAILED_INITIALIZATION_COUNT.getMetricEntity().getMetricName(),
        1,
        buildClusterOnlyAttributes());
  }

  @Test
  public void testRecordKilledPushJobs() {
    stats.recordKilledPushJobs(TEST_STORE_NAME);

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity().getMetricName(),
        1,
        buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));
  }

  @Test
  public void testRecordFailedKillPushJob() {
    stats.recordFailedKillPushJob(TEST_STORE_NAME);

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity().getMetricName(),
        1,
        buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.FAIL));
  }

  @Test
  public void testRecordKillPushJobFailedConsumption() {
    stats.recordKillPushJobFailedConsumption(TEST_STORE_NAME);

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_FAILED_CONSUMPTION_COUNT.getMetricEntity()
            .getMetricName(),
        1,
        buildStoreOnlyAttributes(TEST_STORE_NAME));
  }

  @Test
  public void testRecordKillPushJobFailedConsumptionWithUnknownStore() {
    stats.recordKillPushJobFailedConsumption(UNKNOWN_STORE_NAME);

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_FAILED_CONSUMPTION_COUNT.getMetricEntity()
            .getMetricName(),
        1,
        buildStoreOnlyAttributes(UNKNOWN_STORE_NAME));
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
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_LATENCY.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  // --- Accumulation tests ---

  @Test
  public void testCounterAccumulation() {
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    stats.recordKilledPushJobs(TEST_STORE_NAME);

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity().getMetricName(),
        3,
        buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));
  }

  @Test
  public void testHeartbeatAccumulation() {
    stats.recordHeartbeat();
    stats.recordHeartbeat();

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.HEARTBEAT_COUNT.getMetricEntity().getMetricName(),
        2,
        buildClusterOnlyAttributes());
  }

  @Test
  public void testFailedInitializationAccumulation() {
    stats.recordFailedInitialization();
    stats.recordFailedInitialization();
    stats.recordFailedInitialization();

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.FAILED_INITIALIZATION_COUNT.getMetricEntity().getMetricName(),
        3,
        buildClusterOnlyAttributes());
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
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_LATENCY.getMetricEntity().getMetricName(),
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
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity().getMetricName(),
        2,
        buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));
    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity().getMetricName(),
        1,
        buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.FAIL));
  }

  @Test
  public void testMultipleStoresAreIndependent() {
    // Kill count
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    stats.recordKilledPushJobs(TEST_STORE_NAME);
    stats.recordKilledPushJobs(TEST_STORE_NAME_2);

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity().getMetricName(),
        2,
        buildStoreStatusAttributes(TEST_STORE_NAME, VeniceResponseStatusCategory.SUCCESS));
    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity().getMetricName(),
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
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_LATENCY.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        200.0,
        200.0,
        1,
        200.0,
        buildStoreOnlyAttributes(TEST_STORE_NAME_2),
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_LATENCY.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);

    // Failed consumption
    stats.recordKillPushJobFailedConsumption(TEST_STORE_NAME);
    stats.recordKillPushJobFailedConsumption(TEST_STORE_NAME);
    stats.recordKillPushJobFailedConsumption(TEST_STORE_NAME_2);

    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_FAILED_CONSUMPTION_COUNT.getMetricEntity()
            .getMetricName(),
        2,
        buildStoreOnlyAttributes(TEST_STORE_NAME));
    validateCounter(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_FAILED_CONSUMPTION_COUNT.getMetricEntity()
            .getMetricName(),
        1,
        buildStoreOnlyAttributes(TEST_STORE_NAME_2));
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
    assertAllMethodsSafe(new MetricsRepository());
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
