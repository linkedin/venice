package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.TehutiMetric;
import io.tehuti.metrics.stats.AsyncGauge;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DIVStatsReporterTest {
  private static final String[] ALL_SENSOR_NAMES = { "success_msg", "duplicate_msg", "missing_msg", "corrupted_msg",
      "benign_leader_offset_rewind_count", "potentially_lossy_leader_offset_rewind_count",
      "leader_producer_failure_count", "benign_leader_producer_failure_count" };

  private MetricsRepository metricsRepository;
  private MockTehutiReporter reporter;
  private String storeName;
  private DIVStatsReporter divStatsReporter;
  // Use a dedicated AsyncGaugeExecutor: another test class calling MetricsRepository.close()
  // in the same JVM shuts down the static default executor, which would make
  // AsyncGauge.measure() return 0.0 in this test forever.
  private AsyncGauge.AsyncGaugeExecutor asyncGaugeExecutor;

  @BeforeMethod
  public void setUp() {
    asyncGaugeExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    metricsRepository = new MetricsRepository(new MetricConfig(asyncGaugeExecutor));
    reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    storeName = Utils.getUniqueString("store");
    divStatsReporter = new DIVStatsReporter(metricsRepository, storeName, null);
  }

  @AfterMethod
  public void tearDown() throws IOException {
    if (asyncGaugeExecutor != null) {
      asyncGaugeExecutor.close();
    }
  }

  @Test
  public void testDIVReporterCanReport() {
    // Use assertSensorEventually for the same reason as testAllSensorsReportRecordedValues:
    // AsyncGauge.measure submits to a background executor and returns cachedMeasurement=0.0
    // (or stale value) if the 500ms initialMetricsMeasurementTimeoutInMs expires before the
    // first sample lands. Retry up to 5s for the actual value.
    assertSensorEventually("success_msg", (double) NULL_DIV_STATS.code);

    DIVStats stats = new DIVStats();
    stats.recordSuccessMsg();
    divStatsReporter.setStats(stats);
    assertSensorEventually("success_msg", 1d);

    divStatsReporter.setStats(null);
    assertSensorEventually("success_msg", (double) NULL_DIV_STATS.code);
  }

  @Test
  public void testDIVStatsCounter() {
    DIVStatsReporter mockDIVStatsReporter = mock(DIVStatsReporter.class);
    doReturn(mock(DIVStats.class)).when(mockDIVStatsReporter).getStats();
    DIVStatsReporter.DIVStatsGauge counter =
        new DIVStatsReporter.DIVStatsGauge(mockDIVStatsReporter, () -> 1L, "testDIVStatsCounter");
    // Same AsyncGauge cached-zero race as testDIVReporterCanReport: measure() submits to a
    // background executor and returns the cached value (0.0 on the first call) if the
    // 500 ms initialMetricsMeasurementTimeoutInMs elapses before the first sample lands.
    // Retry until the gauge has caught up.
    MetricConfig metricConfig = new MetricConfig(asyncGaugeExecutor);
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> assertEquals(counter.measure(metricConfig, System.currentTimeMillis()), 1.0));
  }

  @Test
  public void testAllSensorsReturnNullDivStatsWhenNoStatsSet() {
    for (String sensorName: ALL_SENSOR_NAMES) {
      assertEquals(
          querySensor(sensorName).value(),
          (double) NULL_DIV_STATS.code,
          "Expected NULL_DIV_STATS for " + sensorName + " when stats is null");
    }
  }

  @Test
  public void testAllSensorsReturnZeroForNewStats() {
    DIVStats stats = new DIVStats();
    divStatsReporter.setStats(stats);

    for (String sensorName: ALL_SENSOR_NAMES) {
      assertEquals(querySensor(sensorName).value(), 0d, "Expected 0 for " + sensorName + " with new stats");
    }
  }

  @Test
  public void testAllSensorsReportRecordedValues() {
    DIVStats stats = new DIVStats();
    divStatsReporter.setStats(stats);

    stats.recordSuccessMsg();
    stats.recordSuccessMsg();
    assertSensorEventually("success_msg", 2d);

    stats.recordDuplicateMsg();
    assertSensorEventually("duplicate_msg", 1d);

    stats.recordMissingMsg();
    stats.recordMissingMsg();
    stats.recordMissingMsg();
    assertSensorEventually("missing_msg", 3d);

    stats.recordCorruptedMsg();
    assertSensorEventually("corrupted_msg", 1d);

    stats.recordBenignLeaderOffsetRewind();
    stats.recordBenignLeaderOffsetRewind();
    assertSensorEventually("benign_leader_offset_rewind_count", 2d);

    stats.recordPotentiallyLossyLeaderOffsetRewind();
    assertSensorEventually("potentially_lossy_leader_offset_rewind_count", 1d);

    stats.recordLeaderProducerFailure();
    assertSensorEventually("leader_producer_failure_count", 1d);

    stats.recordBenignLeaderProducerFailure();
    stats.recordBenignLeaderProducerFailure();
    assertSensorEventually("benign_leader_producer_failure_count", 2d);
  }

  /*
   * AsyncGauge submits the measurement to a thread pool and waits up to 500 ms for the first
   * result; if the runner is loaded and the future doesn't complete within that window, the
   * gauge returns its cached value (0.0 on the first call) and the assertion sees 0.0 instead
   * of the value we just recorded. The next measure() call observes the now-completed future
   * and returns the real value, so retrying for a few seconds removes the race without changing
   * what the test is verifying.
   */
  private void assertSensorEventually(String sensorName, double expected) {
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        true,
        () -> assertEquals(querySensor(sensorName).value(), expected));
  }

  private TehutiMetric querySensor(String sensorName) {
    return reporter.query("." + storeName + "--" + sensorName + ".DIVStatsGauge");
  }
}
