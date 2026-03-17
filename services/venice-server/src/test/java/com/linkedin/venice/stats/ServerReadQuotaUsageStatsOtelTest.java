package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.DEFAULT_METRIC_PREFIX;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_QUOTA_REQUEST_OUTCOME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.dimensions.QuotaRequestOutcome;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.TestMockTime;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Collection;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerReadQuotaUsageStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test_store";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private TestMockTime mockTime;

  @BeforeMethod
  public void setUp() {
    mockTime = new TestMockTime();
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  private ServerReadQuotaUsageStats createStats() {
    return new ServerReadQuotaUsageStats(metricsRepository, TEST_STORE_NAME, mockTime, TEST_CLUSTER_NAME);
  }

  private static Attributes buildAttributes(VersionRole role, QuotaRequestOutcome outcome) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .put(VENICE_QUOTA_REQUEST_OUTCOME.getDimensionNameInDefaultFormat(), outcome.getDimensionValue())
        .build();
  }

  private static Attributes buildBaseAttributes() {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .build();
  }

  /** Collects metrics once and validates a Sum data point value for the given attributes. */
  private void assertCounterValue(
      Collection<MetricData> metricsData,
      String metricName,
      Attributes expectedAttributes,
      long expectedValue) {
    LongPointData point = OpenTelemetryDataTestUtils
        .getLongPointDataFromSum(metricsData, metricName, TEST_METRIC_PREFIX, expectedAttributes);
    assertNotNull(point, "Counter '" + metricName + "' should have data for " + expectedAttributes);
    assertEquals(point.getValue(), expectedValue, "Counter '" + metricName + "' value mismatch");
  }

  @Test
  public void testAllowedCountersWithVersionRoles() {
    ServerReadQuotaUsageStats stats = createStats();
    stats.setCurrentVersion(2);
    stats.setBackupVersion(1);

    stats.recordAllowed(2, 100);
    stats.recordAllowed(2, 200);
    stats.recordAllowed(1, 50);

    // Collect once — sumThenReset drains adders
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();

    // CURRENT: 2 requests, 300 keys
    assertCounterValue(
        metricsData,
        "read.quota.request.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.ALLOWED),
        2L);
    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.ALLOWED),
        300L);

    // BACKUP: 1 request, 50 keys
    assertCounterValue(
        metricsData,
        "read.quota.request.count",
        buildAttributes(VersionRole.BACKUP, QuotaRequestOutcome.ALLOWED),
        1L);
    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.BACKUP, QuotaRequestOutcome.ALLOWED),
        50L);
  }

  @Test
  public void testRejectedCounterAccumulation() {
    ServerReadQuotaUsageStats stats = createStats();
    stats.setCurrentVersion(1);

    stats.recordRejected(1, 10);
    stats.recordRejected(1, 20);
    stats.recordRejected(1, 30);

    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();

    // 3 rejected requests, 60 keys
    assertCounterValue(
        metricsData,
        "read.quota.request.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.REJECTED),
        3L);
    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.REJECTED),
        60L);
  }

  @Test
  public void testUnintentionallyAllowedCounterAccumulation() {
    ServerReadQuotaUsageStats stats = createStats();
    stats.setCurrentVersion(1);

    stats.recordAllowedUnintentionally(1, 5);
    stats.recordAllowedUnintentionally(1, 15);

    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();

    // 2 requests, 20 keys
    assertCounterValue(
        metricsData,
        "read.quota.request.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.ALLOWED_UNINTENTIONALLY),
        2L);
    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.ALLOWED_UNINTENTIONALLY),
        20L);
  }

  @Test
  public void testOutcomeIsolation() {
    ServerReadQuotaUsageStats stats = createStats();
    stats.setCurrentVersion(1);

    stats.recordAllowed(1, 100);
    stats.recordRejected(1, 50);
    stats.recordAllowedUnintentionally(1, 25);

    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();

    // Each outcome should be independent — verify key counts
    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.ALLOWED),
        100L);
    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.REJECTED),
        50L);
    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.ALLOWED_UNINTENTIONALLY),
        25L);

    // Each outcome should be independent — verify request counts
    assertCounterValue(
        metricsData,
        "read.quota.request.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.ALLOWED),
        1L);
    assertCounterValue(
        metricsData,
        "read.quota.request.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.REJECTED),
        1L);
    assertCounterValue(
        metricsData,
        "read.quota.request.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.ALLOWED_UNINTENTIONALLY),
        1L);
  }

  @Test
  public void testVersionRoleClassification() {
    ServerReadQuotaUsageStats stats = createStats();
    stats.setCurrentVersion(3);
    stats.setBackupVersion(2);

    // Version 3 = CURRENT, version 2 = BACKUP, version 99 = FUTURE (unknown)
    stats.recordAllowed(3, 100);
    stats.recordAllowed(2, 50);
    stats.recordAllowed(99, 10);

    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();

    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.ALLOWED),
        100L);
    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.BACKUP, QuotaRequestOutcome.ALLOWED),
        50L);
    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.FUTURE, QuotaRequestOutcome.ALLOWED),
        10L);
  }

  @Test
  public void testUsageRatioGaugeValueAccuracy() {
    ServerReadQuotaUsageStats stats = createStats();
    stats.setCurrentVersion(1);
    stats.setNodeQuotaResponsibility(1, 1000);
    stats.recordAllowed(1, 10000);

    // Usage ratio = requestedKPS / nodeKpsResponsibility.
    // Tehuti Rate amortizes over a 30s window: (10000/30) / 1000 ≈ 0.333
    double expectedRatio = (10000.0 / 30.0) / 1000.0;
    OpenTelemetryDataTestUtils.validateDoublePointDataFromGauge(
        inMemoryMetricReader,
        expectedRatio,
        0.05,
        buildBaseAttributes(),
        "read.quota.usage_ratio",
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testUsageRatioReturnsZeroWhenNoVersion() {
    createStats(); // No current version set (defaults to 0) => ratio returns NaN => OTel reports 0.0
    OpenTelemetryDataTestUtils.validateDoublePointDataFromGauge(
        inMemoryMetricReader,
        0.0,
        0.001,
        buildBaseAttributes(),
        "read.quota.usage_ratio",
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testTotalStatsDoNotEmitOtel() {
    // Total stats instance should have OTel disabled
    ServerReadQuotaUsageStats totalStats =
        new ServerReadQuotaUsageStats(metricsRepository, "total", mockTime, TEST_CLUSTER_NAME);
    totalStats.setCurrentVersion(1);
    totalStats.recordAllowed(1, 100);
    totalStats.recordRejected(1, 50);
    totalStats.recordAllowedUnintentionally(1, 25);

    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();

    // Verify no OTel metrics emitted for "total" store — check all outcomes and both counter metrics
    for (QuotaRequestOutcome outcome: QuotaRequestOutcome.values()) {
      Attributes totalAttrs = Attributes.builder()
          .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "total")
          .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
          .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), VersionRole.CURRENT.getDimensionValue())
          .put(VENICE_QUOTA_REQUEST_OUTCOME.getDimensionNameInDefaultFormat(), outcome.getDimensionValue())
          .build();
      for (String metricName: new String[] { "read.quota.request.count", "read.quota.key.count" }) {
        String fullName = fullMetricName(metricName);
        boolean found = metricsData.stream()
            .anyMatch(
                md -> md.getName().equals(fullName)
                    && md.getData().getPoints().stream().anyMatch(p -> p.getAttributes().equals(totalAttrs)));
        assertFalse(found, "Total stats should not emit OTel " + metricName + " for " + outcome);
      }
    }

    // Also verify usage ratio gauge is absent for total
    Attributes totalBaseAttrs = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "total")
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .build();
    boolean hasRatio = metricsData.stream()
        .anyMatch(
            md -> md.getName().equals(fullMetricName("read.quota.usage_ratio"))
                && md.getData().getPoints().stream().anyMatch(p -> p.getAttributes().equals(totalBaseAttrs)));
    assertFalse(hasRatio, "Total stats should not emit OTel usage ratio gauge");
  }

  @Test
  public void testDoubleCountingPrevention() {
    // Create via AggServerQuotaUsageStats to test total + per-store interaction
    AggServerQuotaUsageStats aggStats = new AggServerQuotaUsageStats(TEST_CLUSTER_NAME, metricsRepository);
    aggStats.setCurrentVersion(TEST_STORE_NAME, 1);
    aggStats.recordAllowed(TEST_STORE_NAME, 1, 100);
    aggStats.recordRejected(TEST_STORE_NAME, 1, 50);

    // Per-store OTel counter should have a single recording, not doubled by total
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertCounterValue(
        metricsData,
        "read.quota.request.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.REJECTED),
        1L);
  }

  @Test
  public void testVersionLifecycle() {
    ServerReadQuotaUsageStats stats = createStats();

    // Set up version 2 as current, version 1 as backup
    stats.setCurrentVersion(2);
    stats.setBackupVersion(1);
    stats.recordAllowed(2, 100);
    stats.recordAllowed(1, 50);

    // Drain first collection
    inMemoryMetricReader.collectAllMetrics();

    // Transition: version 3 becomes current, version 2 becomes backup
    stats.setCurrentVersion(3);
    stats.setBackupVersion(2);
    stats.removeVersion(1);

    // Record on new current version
    stats.recordAllowed(3, 200);

    // Second collection: only the delta since the first drain
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertCounterValue(
        metricsData,
        "read.quota.key.count",
        buildAttributes(VersionRole.CURRENT, QuotaRequestOutcome.ALLOWED),
        200L);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository otelDisabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .build());
    try {
      exerciseAllRecordingPaths(otelDisabledRepo);
    } finally {
      otelDisabledRepo.close();
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() throws Exception {
    AsyncGauge.AsyncGaugeExecutor executor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    MetricsRepository plainRepo = new MetricsRepository(new MetricConfig(executor));
    try {
      exerciseAllRecordingPaths(plainRepo);
    } finally {
      plainRepo.close();
      executor.close();
    }
  }

  /** Exercises all recording paths — used by NPE prevention tests. */
  private static void exerciseAllRecordingPaths(MetricsRepository repo) {
    ServerReadQuotaUsageStats stats = new ServerReadQuotaUsageStats(repo, TEST_STORE_NAME);
    stats.setCurrentVersion(1);
    stats.setBackupVersion(0);
    stats.recordAllowed(1, 100);
    stats.recordRejected(1, 50);
    stats.recordAllowedUnintentionally(1, 25);
    stats.setNodeQuotaResponsibility(1, 1000);
    stats.removeVersion(1);
  }

  private static String fullMetricName(String metricName) {
    return DEFAULT_METRIC_PREFIX + TEST_METRIC_PREFIX + "." + metricName;
  }
}
