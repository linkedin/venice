package com.linkedin.davinci.stats;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BlobTransferStatsTest {
  // Use a dedicated AsyncGaugeExecutor: another test class calling MetricsRepository.close()
  // in the same JVM shuts down the static default executor, which would make
  // AsyncGauge.measure() return 0.0 in this test forever.
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
  public void testRecordBlobTransferResponsesCount() {
    BlobTransferStats stats = new BlobTransferStats();
    stats.recordBlobTransferResponsesCount();
    Assert.assertEquals(1.0, stats.getBlobTransferTotalNumResponses());
  }

  @Test
  public void testRecordBlobTransferResponsesBasedOnBootstrapStatus() {
    BlobTransferStats stats = new BlobTransferStats();
    stats.recordBlobTransferResponsesBasedOnBoostrapStatus(true);
    Assert.assertEquals(1.0, stats.getBlobTransferSuccessNumResponses());

    stats.recordBlobTransferResponsesBasedOnBoostrapStatus(false);
    Assert.assertEquals(1.0, stats.getBlobTransferFailedNumResponses());
  }

  @Test
  public void testRecordBlobTransferFileReceiveThroughput() {
    double throughput = 5.0;
    BlobTransferStats stats = new BlobTransferStats();
    stats.recordBlobTransferFileReceiveThroughput(throughput);
    Assert.assertEquals(throughput, stats.getBlobTransferFileReceiveThroughput());
  }

  @Test
  public void testRecordBlobTransferTimeInSec() {
    double timeInSec = 10.0;
    BlobTransferStats stats = new BlobTransferStats();
    stats.recordBlobTransferTimeInSec(timeInSec);
    Assert.assertEquals(timeInSec, stats.getBlobTransferTime());
  }

  @Test
  public void blobTransferStatsReporterCanReportForGauge() {
    MetricsRepository metricsRepository = newMetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    String storeName = Utils.getUniqueString("store");

    BlobTransferStatsReporter blobTransferStatsReporter =
        new BlobTransferStatsReporter(metricsRepository, storeName, null);

    assertSensorEventually(reporter, "." + storeName + "--blob_transfer_time.IngestionStatsGauge", -20.0);
    assertSensorEventually(
        reporter,
        "." + storeName + "--blob_transfer_file_receive_throughput.IngestionStatsGauge",
        -20.0);

    BlobTransferStats stats = new BlobTransferStats();
    stats.recordBlobTransferFileReceiveThroughput(5.0);
    stats.recordBlobTransferTimeInSec(10.0);
    blobTransferStatsReporter.setStats(stats);

    assertSensorEventually(reporter, "." + storeName + "--blob_transfer_time.IngestionStatsGauge", 10.0);
    assertSensorEventually(
        reporter,
        "." + storeName + "--blob_transfer_file_receive_throughput.IngestionStatsGauge",
        5.0);
  }

  @Test
  public void blobTransferStatsReporterCanReportForCount() {
    MetricsRepository metricsRepository = newMetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    String storeName = Utils.getUniqueString("store");

    BlobTransferStatsReporter blobTransferStatsReporter =
        new BlobTransferStatsReporter(metricsRepository, storeName, null);

    assertSensorEventually(
        reporter,
        "." + storeName + "--blob_transfer_total_num_responses.IngestionStatsGauge",
        -20.0);
    assertSensorEventually(
        reporter,
        "." + storeName + "--blob_transfer_successful_num_responses.IngestionStatsGauge",
        -20.0);
    assertSensorEventually(
        reporter,
        "." + storeName + "--blob_transfer_failed_num_responses.IngestionStatsGauge",
        -20.0);

    BlobTransferStats stats = new BlobTransferStats();
    stats.recordBlobTransferResponsesCount();
    stats.recordBlobTransferResponsesBasedOnBoostrapStatus(true);
    stats.recordBlobTransferResponsesBasedOnBoostrapStatus(false);

    blobTransferStatsReporter.setStats(stats);
    assertSensorEventually(reporter, "." + storeName + "--blob_transfer_total_num_responses.IngestionStatsGauge", 1.0);
    assertSensorEventually(
        reporter,
        "." + storeName + "--blob_transfer_successful_num_responses.IngestionStatsGauge",
        1.0);
    assertSensorEventually(reporter, "." + storeName + "--blob_transfer_failed_num_responses.IngestionStatsGauge", 1.0);
  }

  /**
   * Retry the sensor read up to 5s. Tehuti's AsyncGauge.measure submits to a background
   * executor and waits initialMetricsMeasurementTimeoutInMs (default 500ms) for the first
   * result; under CI load the first call returns cachedMeasurement (0.0) or stale value
   * before the executor finishes. Same pattern as DIVStatsReporterTest.assertSensorEventually.
   */
  private static void assertSensorEventually(MockTehutiReporter reporter, String sensorName, double expected) {
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(reporter.query(sensorName).value(), expected));
  }
}
