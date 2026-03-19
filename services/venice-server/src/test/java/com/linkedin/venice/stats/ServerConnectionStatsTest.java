package com.linkedin.venice.stats;

import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.dimensions.VeniceConnectionSource;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerConnectionStatsTest {
  private static final String STATS_NAME = "test_server_connection_stats";
  private static final String STATS_PREFIX = "." + STATS_NAME + "--";

  private MetricsRepository metricsRepository;
  private ServerConnectionStats stats;

  @BeforeMethod
  public void setUp() {
    metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    stats = new ServerConnectionStats(metricsRepository, STATS_NAME, "test-cluster");
  }

  @AfterMethod
  public void tearDown() {
    metricsRepository.close();
  }

  // --- AsyncGauge tests ---

  @Test
  public void testConnectionCountGauges() {
    stats.incrementRouterConnectionCount();
    stats.incrementClientConnectionCount();

    assertGaugeValue(ServerConnectionStats.ROUTER_CONNECTION_COUNT_GAUGE, 1.0);
    assertGaugeValue(ServerConnectionStats.CLIENT_CONNECTION_COUNT_GAUGE, 1.0);
  }

  @Test
  public void testIncrementDecrementConnectionCount() {
    stats.incrementRouterConnectionCount();
    stats.decrementRouterConnectionCount();
    stats.incrementClientConnectionCount();
    stats.decrementClientConnectionCount();

    assertGaugeValue(ServerConnectionStats.ROUTER_CONNECTION_COUNT_GAUGE, 0.0);
    assertGaugeValue(ServerConnectionStats.CLIENT_CONNECTION_COUNT_GAUGE, 0.0);
  }

  @Test
  public void testDefaultGaugeValues() {
    assertGaugeValue(ServerConnectionStats.ROUTER_CONNECTION_COUNT_GAUGE, 0.0);
    assertGaugeValue(ServerConnectionStats.CLIENT_CONNECTION_COUNT_GAUGE, 0.0);
  }

  // --- OccurrenceRate tests ---

  @Test
  public void testConnectionRequestRates() {
    stats.incrementRouterConnectionCount();
    stats.incrementClientConnectionCount();
    stats.newConnectionRequest();

    assertRatePositive(ServerConnectionStats.ROUTER_CONNECTION_REQUEST);
    assertRatePositive(ServerConnectionStats.CLIENT_CONNECTION_REQUEST);
    assertRatePositive(ServerConnectionStats.CONNECTION_REQUEST);
  }

  // --- Latency tests ---

  @Test
  public void testNewConnectionSetupLatency() {
    stats.recordNewConnectionSetupLatency(42.5, VeniceConnectionSource.ROUTER);

    String avgMetric = STATS_PREFIX + ServerConnectionStats.NEW_CONNECTION_SETUP_LATENCY + ".Avg";
    String maxMetric = STATS_PREFIX + ServerConnectionStats.NEW_CONNECTION_SETUP_LATENCY + ".Max";

    assertEquals(metricsRepository.getMetric(avgMetric).value(), 42.5);
    assertEquals(metricsRepository.getMetric(maxMetric).value(), 42.5);

    stats.recordNewConnectionSetupLatency(57.5, VeniceConnectionSource.CLIENT);
    assertEquals(metricsRepository.getMetric(avgMetric).value(), 50.0);
    assertEquals(metricsRepository.getMetric(maxMetric).value(), 57.5);
  }

  // --- Negative tests ---
  // NPE prevention tests (plain MetricsRepository, OTel disabled) are in ServerConnectionStatsOtelTest.

  // --- Helper methods ---

  private void assertGaugeValue(String sensorName, double expectedValue) {
    String metricName = STATS_PREFIX + sensorName + ".Gauge";
    waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertNotNull(metricsRepository.getMetric(metricName), sensorName + " gauge should be registered");
      assertEquals(metricsRepository.getMetric(metricName).value(), expectedValue);
    });
  }

  private void assertRatePositive(String sensorName) {
    String metricName = STATS_PREFIX + sensorName + ".OccurrenceRate";
    assertNotNull(metricsRepository.getMetric(metricName), sensorName + " OccurrenceRate should be registered");
    double value = metricsRepository.getMetric(metricName).value();
    assertTrue(value > 0, "Expected " + sensorName + " rate > 0, got " + value);
  }
}
