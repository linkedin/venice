package com.linkedin.davinci.stats;

import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferStatus;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Gauge;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class that exposes stats related to blob transfers
 */
public class BlobTransferStats {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferStats.class);

  // Per-instance metrics
  // As a receiver, track the number of requests received from remote hosts,
  // along with counts for successful, failed, and rejected requests handled by this host.
  protected static final String BLOB_TRANSFER_TOTAL_NUM_REQUESTS = "blob_transfer_total_num_requests";
  protected static final String BLOB_TRANSFER_SUCCESSFUL_NUM_REQUESTS = "blob_transfer_successful_num_requests";
  protected static final String BLOB_TRANSFER_FAILED_NUM_REQUESTS = "blob_transfer_failed_num_requests";
  protected static final String BLOB_TRANSFER_REJECTED_NUM_REQUESTS = "blob_transfer_rejected_num_requests";

  // As a sender, track the number of requests sent for bootstrap,
  // including counts for successful and failed responses from the remote receiver.
  // This can also represent the number of partitions successfully or unsuccessfully bootstrapped via blob transfer.
  protected static final String BLOB_TRANSFER_TOTAL_NUM_RESPONSES = "blob_transfer_total_num_responses";
  protected static final String BLOB_TRANSFER_SUCCESSFUL_NUM_RESPONSES = "blob_transfer_successful_num_responses";
  protected static final String BLOB_TRANSFER_FAILED_NUM_RESPONSES = "blob_transfer_failed_num_responses";

  // The blob file receiving throughput (in MB/sec) and time (in sec)
  protected static final String BLOB_TRANSFER_THROUGHPUT = "blob_transfer_file_receive_throughput";
  protected static final String BLOB_TRANSFER_TIME = "blob_transfer_time";

  private static final MetricConfig METRIC_CONFIG = new MetricConfig();
  private final MetricsRepository localMetricRepository;
  private Count blobTransferTotalNumRequestsCount = new Count();
  private Sensor blobTransferTotalNumRequestsSensor;
  private Count blobTransferSuccessNumRequestsCount = new Count();
  private Sensor blobTransferSuccessNumRequestsSensor;
  private Count blobTransferFailedNumRequestsCount = new Count();
  private Sensor blobTransferFailedNumRequestsSensor;
  private Count blobTransferRejectedNumRequestsCount = new Count();
  private Sensor blobTransferRejectedNumRequestsSensor;
  private Count blobTransferTotalNumResponsesCount = new Count();
  private Sensor blobTransferTotalNumResponsesSensor;
  private Count blobTransferSuccessNumResponsesCount = new Count();
  private Sensor blobTransferSuccessNumResponsesSensor;
  private Count blobTransferFailedNumResponsesCount = new Count();
  private Sensor blobTransferFailedNumResponsesSensor;
  private Gauge blobTransferFileReceiveThroughputGauge = new Gauge();
  private Sensor blobTransferFileReceiveThroughputSensor;
  private Gauge blobTransferTimeGauge = new Gauge();
  private Sensor blobTransferTimeSensor;

  public BlobTransferStats() {
    localMetricRepository = new MetricsRepository(METRIC_CONFIG);

    blobTransferTotalNumRequestsSensor = localMetricRepository.sensor(BLOB_TRANSFER_TOTAL_NUM_REQUESTS);
    blobTransferTotalNumRequestsSensor.add(BLOB_TRANSFER_TOTAL_NUM_REQUESTS, blobTransferTotalNumRequestsCount);

    blobTransferSuccessNumRequestsSensor = localMetricRepository.sensor(BLOB_TRANSFER_SUCCESSFUL_NUM_REQUESTS);
    blobTransferSuccessNumRequestsSensor
        .add(BLOB_TRANSFER_SUCCESSFUL_NUM_REQUESTS, blobTransferSuccessNumRequestsCount);

    blobTransferFailedNumRequestsSensor = localMetricRepository.sensor(BLOB_TRANSFER_FAILED_NUM_REQUESTS);
    blobTransferFailedNumRequestsSensor.add(BLOB_TRANSFER_FAILED_NUM_REQUESTS, blobTransferFailedNumRequestsCount);

    blobTransferRejectedNumRequestsSensor = localMetricRepository.sensor(BLOB_TRANSFER_REJECTED_NUM_REQUESTS);
    blobTransferRejectedNumRequestsSensor
        .add(BLOB_TRANSFER_REJECTED_NUM_REQUESTS, blobTransferRejectedNumRequestsCount);

    blobTransferTotalNumResponsesSensor = localMetricRepository.sensor(BLOB_TRANSFER_TOTAL_NUM_RESPONSES);
    blobTransferTotalNumResponsesSensor.add(BLOB_TRANSFER_TOTAL_NUM_RESPONSES, blobTransferTotalNumResponsesCount);

    blobTransferSuccessNumResponsesSensor = localMetricRepository.sensor(BLOB_TRANSFER_SUCCESSFUL_NUM_RESPONSES);
    blobTransferSuccessNumResponsesSensor
        .add(BLOB_TRANSFER_SUCCESSFUL_NUM_RESPONSES, blobTransferSuccessNumResponsesCount);

    blobTransferFailedNumResponsesSensor = localMetricRepository.sensor(BLOB_TRANSFER_FAILED_NUM_RESPONSES);
    blobTransferFailedNumResponsesSensor.add(BLOB_TRANSFER_FAILED_NUM_RESPONSES, blobTransferFailedNumResponsesCount);

    blobTransferFileReceiveThroughputSensor = localMetricRepository.sensor(BLOB_TRANSFER_THROUGHPUT);
    blobTransferFileReceiveThroughputSensor.add(BLOB_TRANSFER_THROUGHPUT, blobTransferFileReceiveThroughputGauge);

    blobTransferTimeSensor = localMetricRepository.sensor(BLOB_TRANSFER_TIME);
    blobTransferTimeSensor.add(BLOB_TRANSFER_TIME, blobTransferTimeGauge);
  }

  /**
   * When receiving a blob transfer request, bump the total requests amount this host receive.
   */
  public void recordBlobTransferRequestsCount() {
    blobTransferTotalNumRequestsSensor.record();
  }

  /**
   * When receiving a blob transfer request, bump the total requests amount this host receive,
   * based on the host status, bump the successful, failed or rejected requests amount.
   * @param status the status of the blob transfer request
   */
  public void recordBlobTransferRequestsStatus(BlobTransferStatus status) {
    if (status.equals(BlobTransferStatus.SUCCESS)) {
      blobTransferSuccessNumRequestsSensor.record();
    } else if (status.equals(BlobTransferStatus.FAILED)) {
      blobTransferFailedNumRequestsSensor.record();
    } else if (status.equals(BlobTransferStatus.REJECTED)) {
      blobTransferRejectedNumRequestsSensor.record();
    }
  }

  /**
   * Update the blob transfer response stats regardless the response status.
   */
  public void recordBlobTransferResponsesCount() {
    blobTransferTotalNumResponsesSensor.record();
  }

  /**
   * When receiving a blob transfer response from other remote host,
   * based on the blob transfer bootstrap status, bump the successful or failed responses amount.
   * @param isblobTransferSuccess the status of the blob transfer response, true for success, false for failure
   */
  public void recordBlobTransferResponsesBasedOnBoostrapStatus(boolean isblobTransferSuccess) {
    if (isblobTransferSuccess) {
      blobTransferSuccessNumResponsesSensor.record();
    } else {
      blobTransferFailedNumResponsesSensor.record();
    }
  }

  /**
   * Record the blob transfer time.
   * @param throughput in MB/sec
   */
  public void recordBlobTransferFileReceiveThroughput(double throughput) {
    blobTransferFileReceiveThroughputSensor.record(throughput, System.currentTimeMillis());
  }

  /**
   * Record the blob transfer time.
   * @param time the time in second
   */
  public void recordBlobTransferTimeInSec(double time) {
    blobTransferTimeSensor.record(time, System.currentTimeMillis());
  }

  /**
   * All get methods to get the sensor value
   * @return the sensor value
   */
  public double getBlobTransferTotalNumRequests() {
    if (blobTransferTotalNumRequestsCount == null) {
      return 0;
    } else {
      return blobTransferTotalNumRequestsCount.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
  }

  public double getBlobTransferSuccessNumRequests() {
    if (blobTransferSuccessNumRequestsCount == null) {
      return 0;
    } else {
      return blobTransferSuccessNumRequestsCount.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
  }

  public double getBlobTransferFailedNumRequests() {
    if (blobTransferFailedNumRequestsCount == null) {
      return 0;
    } else {
      return blobTransferFailedNumRequestsCount.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
  }

  public double getBlobTransferRejectedNumRequests() {
    if (blobTransferRejectedNumRequestsCount == null) {
      return 0;
    } else {
      return blobTransferRejectedNumRequestsCount.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
  }

  public double getBlobTransferTotalNumResponses() {
    if (blobTransferTotalNumResponsesCount == null) {
      return 0;
    } else {
      return blobTransferTotalNumResponsesCount.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
  }

  public double getBlobTransferSuccessNumResponses() {
    if (blobTransferSuccessNumResponsesCount == null) {
      return 0;
    } else {
      return blobTransferSuccessNumResponsesCount.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
  }

  public double getBlobTransferFailedNumResponses() {
    if (blobTransferFailedNumResponsesCount == null) {
      return 0;
    } else {
      return blobTransferFailedNumResponsesCount.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
  }

  public double getBlobTransferFileReceiveThroughput() {
    if (blobTransferFileReceiveThroughputGauge == null) {
      return 0;
    } else {
      return blobTransferFileReceiveThroughputGauge.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
  }

  public double getBlobTransferTime() {
    if (blobTransferTimeGauge == null) {
      return 0;
    } else {
      return blobTransferTimeGauge.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
  }
}
