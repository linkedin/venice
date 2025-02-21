package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_DELETE_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_DELETE_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_ON_END_VERSION_INGESTION_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_ON_RECOVERY_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_ON_START_VERSION_INGESTION_LATENCY;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerStats.RECORD_TRANSFORMER_PUT_LATENCY;
import static com.linkedin.venice.stats.StatsErrorCode.NULL_INGESTION_STATS;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DaVinciRecordTransformerStatsTest {
  final static double latency = 10;
  final long timestamp = System.currentTimeMillis();

  @Test
  public void testRecordTransformerPutLatency() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordTransformerPutLatency(latency, timestamp);
    Assert.assertEquals(stats.getRecordTransformerPutLatencySensor().getAvg(), latency);
  }

  @Test
  public void testRecordTransformerDeleteLatency() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordTransformerDeleteLatency(latency, timestamp);
    Assert.assertEquals(stats.getRecordTransformerDeleteLatencySensor().getAvg(), latency);
  }

  @Test
  public void testRecordTransformerOnRecoveryLatency() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordTransformerOnRecoveryLatency(latency, timestamp);
    Assert.assertEquals(stats.getRecordTransformerOnRecoveryLatencySensor().getAvg(), latency);
  }

  @Test
  public void testRecordTransformerOnStartVersionIngestionLatency() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordTransformerOnStartVersionIngestionLatency(latency, timestamp);
    Assert.assertEquals(stats.getRecordTransformerOnStartVersionIngestionLatencySensor().getAvg(), latency);
  }

  @Test
  public void testRecordTransformerOnEndVersionIngestionLatency() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordTransformerOnEndVersionIngestionLatency(latency, timestamp);
    Assert.assertEquals(stats.getRecordTransformerOnEndVersionIngestionLatencySensor().getAvg(), latency);
  }

  @Test
  public void testRecordTransformerPutErrorCount() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordTransformerPutError(1, timestamp);
    stats.recordTransformerPutError(1, timestamp);
    Assert.assertEquals(stats.getRecordTransformerPutErrorCount(), 2.0);
  }

  @Test
  public void testRecordTransformerDeleteErrorCount() {
    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordTransformerDeleteError(1, timestamp);
    stats.recordTransformerDeleteError(1, timestamp);
    Assert.assertEquals(stats.getRecordTransformerDeleteErrorCount(), 2.0);
  }

  @Test
  public void testDaVinciRecordTransformerStatsReporterCanReportForGauge() {
    MetricsRepository metricsRepository = new MetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    String storeName = Utils.getUniqueString("store");
    String recordTransformerMetricPrefix = "." + storeName + "--";
    String recordTransformerMetricPostfix = "_avg_µs.DaVinciRecordTransformerStatsGauge";

    DaVinciRecordTransformerStatsReporter recordTransformerStatsReporter =
        new DaVinciRecordTransformerStatsReporter(metricsRepository, storeName, null);
    double nullStat = NULL_INGESTION_STATS.code;

    String startLatency = recordTransformerMetricPrefix + RECORD_TRANSFORMER_ON_START_VERSION_INGESTION_LATENCY
        + recordTransformerMetricPostfix;
    assertEquals(reporter.query(startLatency).value(), nullStat);

    String endLatency = recordTransformerMetricPrefix + RECORD_TRANSFORMER_ON_END_VERSION_INGESTION_LATENCY
        + recordTransformerMetricPostfix;
    assertEquals(reporter.query(endLatency).value(), nullStat);

    String onRecoveryLatency =
        recordTransformerMetricPrefix + RECORD_TRANSFORMER_ON_RECOVERY_LATENCY + recordTransformerMetricPostfix;
    assertEquals(reporter.query(onRecoveryLatency).value(), nullStat);

    String putLatency = recordTransformerMetricPrefix + RECORD_TRANSFORMER_PUT_LATENCY + recordTransformerMetricPostfix;
    assertEquals(reporter.query(putLatency).value(), nullStat);

    String deleteLatency =
        recordTransformerMetricPrefix + RECORD_TRANSFORMER_DELETE_LATENCY + recordTransformerMetricPostfix;
    assertEquals(reporter.query(deleteLatency).value(), nullStat);

    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();
    stats.recordTransformerPutLatency(latency, timestamp);
    recordTransformerStatsReporter.setStats(stats);

    assertEquals(reporter.query(putLatency).value(), latency);
  }

  @Test
  public void testDaVinciRecordTransformerStatsReporterCanReportForCount() {
    MetricsRepository metricsRepository = new MetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    String storeName = Utils.getUniqueString("store");
    String recordTransformerMetricPrefix = "." + storeName + "--";
    String recordTransformerMetricPostfix = ".DaVinciRecordTransformerStatsGauge";

    DaVinciRecordTransformerStatsReporter recordTransformerStatsReporter =
        new DaVinciRecordTransformerStatsReporter(metricsRepository, storeName, null);
    double nullStat = 0;

    String transformerPutErrorCount =
        recordTransformerMetricPrefix + RECORD_TRANSFORMER_PUT_ERROR_COUNT + recordTransformerMetricPostfix;
    assertEquals(reporter.query(transformerPutErrorCount).value(), nullStat);

    String transformerDeleteErrorCount =
        recordTransformerMetricPrefix + RECORD_TRANSFORMER_DELETE_ERROR_COUNT + recordTransformerMetricPostfix;
    assertEquals(reporter.query(transformerDeleteErrorCount).value(), nullStat);

    DaVinciRecordTransformerStats stats = new DaVinciRecordTransformerStats();

    stats.recordTransformerPutError(1, timestamp);
    recordTransformerStatsReporter.setStats(stats);
    assertEquals(reporter.query(transformerPutErrorCount).value(), 1.0);

    stats.recordTransformerDeleteError(1, timestamp);
    recordTransformerStatsReporter.setStats(stats);
    assertEquals(reporter.query(transformerDeleteErrorCount).value(), 1.0);
  }
}
