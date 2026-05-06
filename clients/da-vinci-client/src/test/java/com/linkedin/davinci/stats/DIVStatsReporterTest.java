package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.TehutiMetric;
import io.tehuti.metrics.stats.AsyncGauge;
import java.io.IOException;
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
    assertEquals(querySensor("success_msg").value(), (double) NULL_DIV_STATS.code);

    DIVStats stats = new DIVStats();
    stats.recordSuccessMsg();
    divStatsReporter.setStats(stats);
    assertEquals(querySensor("success_msg").value(), 1d);

    divStatsReporter.setStats(null);
    assertEquals(querySensor("success_msg").value(), (double) NULL_DIV_STATS.code);
  }

  @Test
  public void testDIVStatsCounter() {
    DIVStatsReporter mockDIVStatsReporter = mock(DIVStatsReporter.class);
    doReturn(mock(DIVStats.class)).when(mockDIVStatsReporter).getStats();
    DIVStatsReporter.DIVStatsGauge counter =
        new DIVStatsReporter.DIVStatsGauge(mockDIVStatsReporter, () -> 1L, "testDIVStatsCounter");
    assertEquals(counter.measure(new MetricConfig(asyncGaugeExecutor), System.currentTimeMillis()), 1.0);
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
    assertEquals(querySensor("success_msg").value(), 2d);

    stats.recordDuplicateMsg();
    assertEquals(querySensor("duplicate_msg").value(), 1d);

    stats.recordMissingMsg();
    stats.recordMissingMsg();
    stats.recordMissingMsg();
    assertEquals(querySensor("missing_msg").value(), 3d);

    stats.recordCorruptedMsg();
    assertEquals(querySensor("corrupted_msg").value(), 1d);

    stats.recordBenignLeaderOffsetRewind();
    stats.recordBenignLeaderOffsetRewind();
    assertEquals(querySensor("benign_leader_offset_rewind_count").value(), 2d);

    stats.recordPotentiallyLossyLeaderOffsetRewind();
    assertEquals(querySensor("potentially_lossy_leader_offset_rewind_count").value(), 1d);

    stats.recordLeaderProducerFailure();
    assertEquals(querySensor("leader_producer_failure_count").value(), 1d);

    stats.recordBenignLeaderProducerFailure();
    stats.recordBenignLeaderProducerFailure();
    assertEquals(querySensor("benign_leader_producer_failure_count").value(), 2d);
  }

  private TehutiMetric querySensor(String sensorName) {
    return reporter.query("." + storeName + "--" + sensorName + ".DIVStatsGauge");
  }
}
