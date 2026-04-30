package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_DELETE_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_DELETE_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_LATENCY;
import static com.linkedin.venice.stats.StatsErrorCode.NULL_INGESTION_STATS;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DaVinciRecordTransformerStatsTest {
  final static double latency = 10;
  final long timestamp = System.currentTimeMillis();

  // Dedicated AsyncGauge executor: the default static singleton can be shut down by other tests
  // calling MetricsRepository.close() in the same JVM, which makes AsyncGauge.measure() return 0.0
  // permanently.
  private AsyncGauge.AsyncGaugeExecutor asyncGaugeExecutor;

  @BeforeMethod
  public void setUp() {
    asyncGaugeExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
  }

  @AfterMethod
  public void tearDown() throws IOException {
    if (asyncGaugeExecutor != null) {
      asyncGaugeExecutor.close();
    }
  }

  private MetricsRepository newMetricsRepository() {
    return new MetricsRepository(new MetricConfig(asyncGaugeExecutor));
  }

  @Test
  public void testPutLatency() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordPutLatency(latency, timestamp);
    Assert.assertEquals(stats.getPutLatencySensor().getAvg(), latency);
  }

  @Test
  public void testDeleteLatency() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordDeleteLatency(latency, timestamp);
    Assert.assertEquals(stats.getDeleteLatencySensor().getAvg(), latency);
  }

  @Test
  public void testPutErrorCount() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordPutError(timestamp);
    stats.recordPutError(timestamp);
    Assert.assertEquals(stats.getPutErrorCount(), 2.0);
  }

  @Test
  public void testDeleteErrorCount() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordDeleteError(timestamp);
    stats.recordDeleteError(timestamp);
    Assert.assertEquals(stats.getDeleteErrorCount(), 2.0);
  }

  @Test
  public void testDaVinciRecordTransformerStatsReporterCanReportForGauge() {
    MetricsRepository metricsRepository = newMetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    String storeName = Utils.getUniqueString("store");
    String metricPrefix = "." + storeName + "--";
    String metricPostfix = "_avg_ms.DaVinciRecordTransformerStatsGauge";

    DaVinciRecordTransformerStatsReporter recordTransformerStatsReporter =
        new DaVinciRecordTransformerStatsReporter(metricsRepository, storeName, null);
    double nullStat = NULL_INGESTION_STATS.code;

    String putLatency = metricPrefix + RECORD_TRANSFORMER_PUT_LATENCY + metricPostfix;
    assertEquals(reporter.query(putLatency).value(), nullStat);

    String deleteLatency = metricPrefix + RECORD_TRANSFORMER_DELETE_LATENCY + metricPostfix;
    assertEquals(reporter.query(deleteLatency).value(), nullStat);

    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordPutLatency(latency, timestamp);
    recordTransformerStatsReporter.setStats(stats);

    assertEquals(reporter.query(putLatency).value(), latency);
  }

  @Test
  public void testDaVinciRecordTransformerStatsReporterCanReportForCount() {
    MetricsRepository metricsRepository = newMetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    String storeName = Utils.getUniqueString("store");
    String metricPrefix = "." + storeName + "--";
    String metricPostfix = ".DaVinciRecordTransformerStatsGauge";

    DaVinciRecordTransformerStatsReporter recordTransformerStatsReporter =
        new DaVinciRecordTransformerStatsReporter(metricsRepository, storeName, null);
    double nullStat = 0;

    String putErrorCount = metricPrefix + RECORD_TRANSFORMER_PUT_ERROR_COUNT + metricPostfix;
    assertEquals(reporter.query(putErrorCount).value(), nullStat);

    String deleteErrorCount = metricPrefix + RECORD_TRANSFORMER_DELETE_ERROR_COUNT + metricPostfix;
    assertEquals(reporter.query(deleteErrorCount).value(), nullStat);

    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();

    stats.recordPutError(timestamp);
    recordTransformerStatsReporter.setStats(stats);
    assertEquals(reporter.query(putErrorCount).value(), 1.0);

    stats.recordDeleteError(timestamp);
    recordTransformerStatsReporter.setStats(stats);
    assertEquals(reporter.query(deleteErrorCount).value(), 1.0);
  }
}
