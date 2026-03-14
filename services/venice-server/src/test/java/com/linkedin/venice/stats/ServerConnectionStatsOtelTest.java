package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.ServerConnectionOtelMetricEntity.CONNECTION_ACTIVE_COUNT;
import static com.linkedin.davinci.stats.ServerConnectionOtelMetricEntity.CONNECTION_REQUEST_COUNT;
import static com.linkedin.davinci.stats.ServerConnectionOtelMetricEntity.CONNECTION_SETUP_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONNECTION_SOURCE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.stats.ServerConnectionOtelMetricEntity;
import com.linkedin.venice.stats.dimensions.VeniceConnectionSource;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerConnectionStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STATS_NAME = "test_server_connection_stats";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ServerConnectionStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    stats = new ServerConnectionStats(metricsRepository, TEST_STATS_NAME, TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  // --- Positive OTel tests ---

  @Test
  public void testConnectionRequestCounterBySource() {
    stats.incrementRouterConnectionCount();
    stats.incrementClientConnectionCount();

    validateCounter(CONNECTION_REQUEST_COUNT, VeniceConnectionSource.ROUTER, 1);
    validateCounter(CONNECTION_REQUEST_COUNT, VeniceConnectionSource.CLIENT, 1);
  }

  @Test
  public void testConnectionActiveCountUpDownCounter() {
    // Router: +2 -1 = net 1; Client: +2 -1 = net 1
    stats.incrementRouterConnectionCount();
    stats.incrementRouterConnectionCount();
    stats.decrementRouterConnectionCount();
    stats.incrementClientConnectionCount();
    stats.incrementClientConnectionCount();
    stats.decrementClientConnectionCount();

    validateCounter(CONNECTION_ACTIVE_COUNT, VeniceConnectionSource.ROUTER, 1);
    validateCounter(CONNECTION_ACTIVE_COUNT, VeniceConnectionSource.CLIENT, 1);
  }

  @Test
  public void testActiveCountNetZero() {
    stats.incrementRouterConnectionCount();
    stats.decrementRouterConnectionCount();

    validateCounter(CONNECTION_ACTIVE_COUNT, VeniceConnectionSource.ROUTER, 0);
  }

  @Test
  public void testSetupTimeHistogram() {
    stats.recordNewConnectionSetupLatency(42.5);
    stats.recordNewConnectionSetupLatency(57.5);

    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        42.5,
        57.5,
        2,
        100.0,
        buildClusterOnlyAttributes(),
        CONNECTION_SETUP_TIME.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testCounterAccumulation() {
    stats.incrementRouterConnectionCount();
    stats.incrementRouterConnectionCount();
    stats.incrementRouterConnectionCount();

    validateCounter(CONNECTION_REQUEST_COUNT, VeniceConnectionSource.ROUTER, 3);
  }

  @Test
  public void testDimensionIsolation() {
    stats.incrementRouterConnectionCount();
    stats.incrementRouterConnectionCount();
    stats.incrementClientConnectionCount();

    validateCounter(CONNECTION_REQUEST_COUNT, VeniceConnectionSource.ROUTER, 2);
    validateCounter(CONNECTION_REQUEST_COUNT, VeniceConnectionSource.CLIENT, 1);
  }

  @Test
  public void testTehutiAndOtelConsistency() {
    int numRouterRequests = 5;
    for (int i = 0; i < numRouterRequests; i++) {
      stats.incrementRouterConnectionCount();
    }

    validateCounter(CONNECTION_REQUEST_COUNT, VeniceConnectionSource.ROUTER, numRouterRequests);

    String tehutiMetricName =
        "." + TEST_STATS_NAME + "--" + ServerConnectionStats.ROUTER_CONNECTION_REQUEST + ".OccurrenceRate";
    assertNotNull(
        metricsRepository.getMetric(tehutiMetricName),
        "Tehuti OccurrenceRate sensor should be registered for router_connection_request");
    double tehutiValue = metricsRepository.getMetric(tehutiMetricName).value();
    assertTrue(tehutiValue > 0, "Tehuti OccurrenceRate should be > 0 after " + numRouterRequests + " recordings");
  }

  // --- Negative OTel tests ---

  @Test
  public void testNewConnectionRequestProducesNoOtelData() {
    // connection_request is Tehuti-only; OTel total is derived at query time
    stats.newConnectionRequest();
    stats.newConnectionRequest();

    // Verify newConnectionRequest() did not produce any OTel connection.request_count data
    long connectionMetricCount = inMemoryMetricReader.collectAllMetrics()
        .stream()
        .filter(m -> m.getName().endsWith("connection.request_count"))
        .count();
    assertEquals(
        connectionMetricCount,
        0L,
        "newConnectionRequest() should not produce OTel connection.request_count data");
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .build());
    try {
      exerciseAllRecordingPaths(disabledRepo);
    } finally {
      disabledRepo.close();
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = new MetricsRepository();
    exerciseAllRecordingPaths(plainRepo);
  }

  /**
   * Exercises all recording paths. Used by NPE prevention tests to verify no NullPointerException
   * is thrown when OTel is disabled or when using a plain MetricsRepository.
   */
  private static void exerciseAllRecordingPaths(MetricsRepository repo) {
    ServerConnectionStats safeStats = new ServerConnectionStats(repo, "npe_test_stats", null);

    safeStats.incrementRouterConnectionCount();
    safeStats.decrementRouterConnectionCount();
    safeStats.incrementClientConnectionCount();
    safeStats.decrementClientConnectionCount();
    safeStats.newConnectionRequest();
    safeStats.recordNewConnectionSetupLatency(10.0);
  }

  // --- Helper methods ---

  private void validateCounter(
      ServerConnectionOtelMetricEntity metric,
      VeniceConnectionSource source,
      long expectedValue) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        buildAttributesWithSource(source),
        metric.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  private Attributes buildAttributesWithSource(VeniceConnectionSource source) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_CONNECTION_SOURCE.getDimensionNameInDefaultFormat(), source.getDimensionValue())
        .build();
  }

  private Attributes buildClusterOnlyAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }
}
