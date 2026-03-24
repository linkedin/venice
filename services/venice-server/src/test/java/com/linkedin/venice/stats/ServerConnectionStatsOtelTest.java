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
  public void testSetupTimeHistogramBySource() {
    stats.recordNewConnectionSetupLatency(42.5, VeniceConnectionSource.ROUTER);
    stats.recordNewConnectionSetupLatency(57.5, VeniceConnectionSource.CLIENT);

    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        42.5,
        42.5,
        1,
        42.5,
        buildAttributesWithSource(VeniceConnectionSource.ROUTER),
        CONNECTION_SETUP_TIME.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        57.5,
        57.5,
        1,
        57.5,
        buildAttributesWithSource(VeniceConnectionSource.CLIENT),
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

  /**
   * Validates that Tehuti and OTel record consistent values across all metric types:
   * request count (OccurrenceRate vs COUNTER), active count (AsyncGauge vs UP_DOWN_COUNTER),
   * and setup latency (Percentile/Avg/Max vs HISTOGRAM).
   */
  @Test
  public void testTehutiAndOtelConsistency() {
    // Record: 3 router connections (2 still active), 1 client, 2 latencies
    stats.incrementRouterConnectionCount();
    stats.incrementRouterConnectionCount();
    stats.incrementRouterConnectionCount();
    stats.decrementRouterConnectionCount();
    stats.incrementClientConnectionCount();
    stats.recordNewConnectionSetupLatency(42.5, VeniceConnectionSource.ROUTER);
    stats.recordNewConnectionSetupLatency(57.5, VeniceConnectionSource.ROUTER);

    // --- OTel validation ---
    validateCounter(CONNECTION_REQUEST_COUNT, VeniceConnectionSource.ROUTER, 3);
    validateCounter(CONNECTION_REQUEST_COUNT, VeniceConnectionSource.CLIENT, 1);
    validateCounter(CONNECTION_ACTIVE_COUNT, VeniceConnectionSource.ROUTER, 2);
    validateCounter(CONNECTION_ACTIVE_COUNT, VeniceConnectionSource.CLIENT, 1);
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        42.5,
        57.5,
        2,
        100.0,
        buildAttributesWithSource(VeniceConnectionSource.ROUTER),
        CONNECTION_SETUP_TIME.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);

    // --- Tehuti validation (same VeniceMetricsRepository, joint API) ---
    String prefix = "." + TEST_STATS_NAME + "--";

    // Request count: OccurrenceRate > 0
    String routerRequestMetric = prefix + ServerConnectionStats.ROUTER_CONNECTION_REQUEST + ".OccurrenceRate";
    assertNotNull(metricsRepository.getMetric(routerRequestMetric));
    assertTrue(metricsRepository.getMetric(routerRequestMetric).value() > 0);

    String clientRequestMetric = prefix + ServerConnectionStats.CLIENT_CONNECTION_REQUEST + ".OccurrenceRate";
    assertNotNull(metricsRepository.getMetric(clientRequestMetric));
    assertTrue(metricsRepository.getMetric(clientRequestMetric).value() > 0);

    // Setup latency: Avg = 50.0, Max = 57.5
    String avgMetric = prefix + ServerConnectionStats.NEW_CONNECTION_SETUP_LATENCY + ".Avg";
    String maxMetric = prefix + ServerConnectionStats.NEW_CONNECTION_SETUP_LATENCY + ".Max";
    assertNotNull(metricsRepository.getMetric(avgMetric));
    assertEquals(metricsRepository.getMetric(avgMetric).value(), 50.0);
    assertEquals(metricsRepository.getMetric(maxMetric).value(), 57.5);
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
    safeStats.recordNewConnectionSetupLatency(10.0, VeniceConnectionSource.ROUTER);
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

}
