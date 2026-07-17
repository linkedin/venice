package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.stats.dimensions.VeniceBlobTransferSource;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
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

  // As a sender, track the number of requests sent for bootstrap,
  // including counts for successful and failed responses from the remote receiver.
  // This can also represent the number of partitions successfully or unsuccessfully bootstrapped via blob transfer.
  protected static final String BLOB_TRANSFER_TOTAL_NUM_RESPONSES = "blob_transfer_total_num_responses";
  protected static final String BLOB_TRANSFER_SUCCESSFUL_NUM_RESPONSES = "blob_transfer_successful_num_responses";
  protected static final String BLOB_TRANSFER_FAILED_NUM_RESPONSES = "blob_transfer_failed_num_responses";

  protected static final String BLOB_TRANSFER_DAVINCI_PEER_SUCCESSFUL_NUM_REQUESTS =
      "blob_transfer_davinci_peer_successful_num_requests";
  protected static final String BLOB_TRANSFER_DAVINCI_PEER_FAILED_NUM_REQUESTS =
      "blob_transfer_davinci_peer_failed_num_requests";
  protected static final String BLOB_TRANSFER_VENICE_SERVER_SUCCESSFUL_NUM_REQUESTS =
      "blob_transfer_venice_server_successful_num_requests";
  protected static final String BLOB_TRANSFER_VENICE_SERVER_FAILED_NUM_REQUESTS =
      "blob_transfer_venice_server_failed_num_requests";
  // The blob file receiving throughput (in MB/sec) and time (in sec)
  protected static final String BLOB_TRANSFER_THROUGHPUT = "blob_transfer_file_receive_throughput";
  protected static final String BLOB_TRANSFER_TIME = "blob_transfer_time";
  protected static final String BLOB_TRANSFER_BYTES_RECEIVED = "blob_transfer_bytes_received";
  protected static final String BLOB_TRANSFER_BYTES_SENT = "blob_transfer_bytes_sent";

  private static final MetricConfig METRIC_CONFIG = new MetricConfig();
  private final MetricsRepository localMetricRepository;
  private Count blobTransferTotalNumResponsesCount = new Count();
  private Sensor blobTransferTotalNumResponsesSensor;
  private Count blobTransferSuccessNumResponsesCount = new Count();
  private Sensor blobTransferSuccessNumResponsesSensor;
  private Count blobTransferFailedNumResponsesCount = new Count();
  private Sensor blobTransferFailedNumResponsesSensor;
  private Count blobTransferDaVinciPeerSuccessfulNumRequestsCount = new Count();
  private Sensor blobTransferDaVinciPeerSuccessfulNumRequestsSensor;
  private Count blobTransferDaVinciPeerFailedNumRequestsCount = new Count();
  private Sensor blobTransferDaVinciPeerFailedNumRequestsSensor;
  private Count blobTransferVeniceServerSuccessfulNumRequestsCount = new Count();
  private Sensor blobTransferVeniceServerSuccessfulNumRequestsSensor;
  private Count blobTransferVeniceServerFailedNumRequestsCount = new Count();
  private Sensor blobTransferVeniceServerFailedNumRequestsSensor;
  private Gauge blobTransferFileReceiveThroughputGauge = new Gauge();
  private Sensor blobTransferFileReceiveThroughputSensor;
  private Gauge blobTransferTimeGauge = new Gauge();
  private Sensor blobTransferTimeSensor;
  private LongAdderRateGauge blobTransferBytesReceivedSensor;
  private LongAdderRateGauge blobTransferBytesSentSensor;

  public BlobTransferStats() {
    this(new SystemTime());
  }

  public BlobTransferStats(Time time) {
    localMetricRepository = new MetricsRepository(METRIC_CONFIG);
    blobTransferBytesReceivedSensor = new LongAdderRateGauge(time);
    blobTransferBytesSentSensor = new LongAdderRateGauge(time);

    blobTransferTotalNumResponsesSensor = localMetricRepository.sensor(BLOB_TRANSFER_TOTAL_NUM_RESPONSES);
    blobTransferTotalNumResponsesSensor.add(BLOB_TRANSFER_TOTAL_NUM_RESPONSES, blobTransferTotalNumResponsesCount);

    blobTransferSuccessNumResponsesSensor = localMetricRepository.sensor(BLOB_TRANSFER_SUCCESSFUL_NUM_RESPONSES);
    blobTransferSuccessNumResponsesSensor
        .add(BLOB_TRANSFER_SUCCESSFUL_NUM_RESPONSES, blobTransferSuccessNumResponsesCount);

    blobTransferFailedNumResponsesSensor = localMetricRepository.sensor(BLOB_TRANSFER_FAILED_NUM_RESPONSES);
    blobTransferFailedNumResponsesSensor.add(BLOB_TRANSFER_FAILED_NUM_RESPONSES, blobTransferFailedNumResponsesCount);

    blobTransferDaVinciPeerSuccessfulNumRequestsSensor = registerCountSensor(
        BLOB_TRANSFER_DAVINCI_PEER_SUCCESSFUL_NUM_REQUESTS,
        blobTransferDaVinciPeerSuccessfulNumRequestsCount);
    blobTransferDaVinciPeerFailedNumRequestsSensor = registerCountSensor(
        BLOB_TRANSFER_DAVINCI_PEER_FAILED_NUM_REQUESTS,
        blobTransferDaVinciPeerFailedNumRequestsCount);
    blobTransferVeniceServerSuccessfulNumRequestsSensor = registerCountSensor(
        BLOB_TRANSFER_VENICE_SERVER_SUCCESSFUL_NUM_REQUESTS,
        blobTransferVeniceServerSuccessfulNumRequestsCount);
    blobTransferVeniceServerFailedNumRequestsSensor = registerCountSensor(
        BLOB_TRANSFER_VENICE_SERVER_FAILED_NUM_REQUESTS,
        blobTransferVeniceServerFailedNumRequestsCount);
    blobTransferFileReceiveThroughputSensor = localMetricRepository.sensor(BLOB_TRANSFER_THROUGHPUT);
    blobTransferFileReceiveThroughputSensor.add(BLOB_TRANSFER_THROUGHPUT, blobTransferFileReceiveThroughputGauge);

    blobTransferTimeSensor = localMetricRepository.sensor(BLOB_TRANSFER_TIME);
    blobTransferTimeSensor.add(BLOB_TRANSFER_TIME, blobTransferTimeGauge);

    registerSensor(localMetricRepository, BLOB_TRANSFER_BYTES_RECEIVED, blobTransferBytesReceivedSensor);
    registerSensor(localMetricRepository, BLOB_TRANSFER_BYTES_SENT, blobTransferBytesSentSensor);
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

  public void recordBlobTransferRequest(VeniceBlobTransferSource source, boolean isSuccess) {
    switch (source) {
      case DAVINCI_PEER:
        if (isSuccess) {
          blobTransferDaVinciPeerSuccessfulNumRequestsSensor.record();
        } else {
          blobTransferDaVinciPeerFailedNumRequestsSensor.record();
        }
        break;
      case VENICE_SERVER:
        if (isSuccess) {
          blobTransferVeniceServerSuccessfulNumRequestsSensor.record();
        } else {
          blobTransferVeniceServerFailedNumRequestsSensor.record();
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported blob transfer source: " + source);
    }
  }

  /**
   * Record the blob transfer file receive throughput.
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

  public double getBlobTransferRequestCount(VeniceBlobTransferSource source, boolean isSuccess) {
    Count count;
    switch (source) {
      case DAVINCI_PEER:
        count = isSuccess
            ? blobTransferDaVinciPeerSuccessfulNumRequestsCount
            : blobTransferDaVinciPeerFailedNumRequestsCount;
        break;
      case VENICE_SERVER:
        count = isSuccess
            ? blobTransferVeniceServerSuccessfulNumRequestsCount
            : blobTransferVeniceServerFailedNumRequestsCount;
        break;
      default:
        throw new IllegalArgumentException("Unsupported blob transfer source: " + source);
    }
    return count.measure(METRIC_CONFIG, System.currentTimeMillis());
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

  public double getBlobTransferBytesReceived() {
    return blobTransferBytesReceivedSensor.getRate();
  }

  public void recordBlobTransferBytesReceived(long value) {
    blobTransferBytesReceivedSensor.record(value);
  }

  public double getBlobTransferBytesSent() {
    return blobTransferBytesSentSensor.getRate();
  }

  public void recordBlobTransferBytesSent(long value) {
    blobTransferBytesSentSensor.record(value);
  }

  void registerSensor(MetricsRepository localMetricRepository, String sensorName, LongAdderRateGauge gauge) {
    Sensor sensor = localMetricRepository.sensor(sensorName);
    sensor.add(sensorName + "_rate", gauge);
  }

  private Sensor registerCountSensor(String sensorName, Count count) {
    Sensor sensor = localMetricRepository.sensor(sensorName);
    sensor.add(sensorName, count);
    return sensor;
  }

}
