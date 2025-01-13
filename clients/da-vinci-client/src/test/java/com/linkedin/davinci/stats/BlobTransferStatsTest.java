package com.linkedin.davinci.stats;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BlobTransferStatsTest {
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
    MetricsRepository metricsRepository = new MetricsRepository();
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
    MetricsRepository metricsRepository = new MetricsRepository();
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

    BlobTransferStats stats = new BlobTransferStats();
    stats.recordBlobTransferResponsesCount();
    stats.recordBlobTransferResponsesBasedOnBoostrapStatus(true);
    stats.recordBlobTransferResponsesBasedOnBoostrapStatus(false);

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
  }
}
