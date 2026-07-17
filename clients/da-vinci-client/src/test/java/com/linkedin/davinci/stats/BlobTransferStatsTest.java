package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceBlobTransferSource.DAVINCI_PEER;
import static com.linkedin.venice.stats.dimensions.VeniceBlobTransferSource.VENICE_SERVER;

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
  public void testRecordBlobTransferRequestsBySourceAndOutcome() {
    BlobTransferStats stats = new BlobTransferStats();

    stats.recordBlobTransferRequest(DAVINCI_PEER, true);
    stats.recordBlobTransferRequest(DAVINCI_PEER, false);
    stats.recordBlobTransferRequest(VENICE_SERVER, true);
    stats.recordBlobTransferRequest(VENICE_SERVER, false);

    Assert.assertEquals(stats.getBlobTransferRequestCount(DAVINCI_PEER, true), 1.0);
    Assert.assertEquals(stats.getBlobTransferRequestCount(DAVINCI_PEER, false), 1.0);
    Assert.assertEquals(stats.getBlobTransferRequestCount(VENICE_SERVER, true), 1.0);
    Assert.assertEquals(stats.getBlobTransferRequestCount(VENICE_SERVER, false), 1.0);
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

    Assert.assertEquals(reporter.query("." + storeName + "--blob_transfer_time.IngestionStatsGauge").value(), -20.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_file_receive_throughput.IngestionStatsGauge").value(),
        -20.0);

    BlobTransferStats stats = new BlobTransferStats();
    stats.recordBlobTransferFileReceiveThroughput(5.0);
    stats.recordBlobTransferTimeInSec(10.0);
    blobTransferStatsReporter.setStats(stats);

    Assert.assertEquals(reporter.query("." + storeName + "--blob_transfer_time.IngestionStatsGauge").value(), 10.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_file_receive_throughput.IngestionStatsGauge").value(),
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

    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_total_num_responses.IngestionStatsGauge").value(),
        -20.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_successful_num_responses.IngestionStatsGauge").value(),
        -20.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_failed_num_responses.IngestionStatsGauge").value(),
        -20.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_davinci_peer_successful_num_requests.IngestionStatsGauge")
            .value(),
        -20.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_davinci_peer_failed_num_requests.IngestionStatsGauge")
            .value(),
        -20.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_venice_server_successful_num_requests.IngestionStatsGauge")
            .value(),
        -20.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_venice_server_failed_num_requests.IngestionStatsGauge")
            .value(),
        -20.0);

    BlobTransferStats stats = new BlobTransferStats();
    stats.recordBlobTransferResponsesCount();
    stats.recordBlobTransferResponsesBasedOnBoostrapStatus(true);
    stats.recordBlobTransferResponsesBasedOnBoostrapStatus(false);
    stats.recordBlobTransferRequest(DAVINCI_PEER, true);
    stats.recordBlobTransferRequest(DAVINCI_PEER, false);
    stats.recordBlobTransferRequest(VENICE_SERVER, true);
    stats.recordBlobTransferRequest(VENICE_SERVER, false);

    blobTransferStatsReporter.setStats(stats);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_total_num_responses.IngestionStatsGauge").value(),
        1.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_successful_num_responses.IngestionStatsGauge").value(),
        1.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_failed_num_responses.IngestionStatsGauge").value(),
        1.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_davinci_peer_successful_num_requests.IngestionStatsGauge")
            .value(),
        1.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_davinci_peer_failed_num_requests.IngestionStatsGauge")
            .value(),
        1.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_venice_server_successful_num_requests.IngestionStatsGauge")
            .value(),
        1.0);
    Assert.assertEquals(
        reporter.query("." + storeName + "--blob_transfer_venice_server_failed_num_requests.IngestionStatsGauge")
            .value(),
        1.0);
  }
}
