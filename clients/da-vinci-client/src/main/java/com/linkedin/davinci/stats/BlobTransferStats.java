package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.stats.dimensions.VeniceBlobTransferFallbackReason;
import com.linkedin.venice.stats.dimensions.VeniceBlobTransferSource;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Gauge;
import java.util.EnumMap;
import java.util.Map;
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

  // Replicas that bootstrapped from Kafka instead of blob transfer, split by why blob transfer was not used.
  protected static final String BLOB_TRANSFER_KAFKA_FALLBACK_NO_CANDIDATES =
      "blob_transfer_kafka_fallback_no_candidates";
  protected static final String BLOB_TRANSFER_KAFKA_FALLBACK_ALL_HOSTS_FAILED =
      "blob_transfer_kafka_fallback_all_hosts_failed";

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
  private final Map<VeniceBlobTransferSource, Map<VeniceResponseStatusCategory, CountSensor>> requestCounters =
      new EnumMap<>(VeniceBlobTransferSource.class);
  private final Map<VeniceBlobTransferFallbackReason, CountSensor> kafkaFallbackCounters =
      new EnumMap<>(VeniceBlobTransferFallbackReason.class);
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

    registerRequestCounter(
        VeniceBlobTransferSource.DAVINCI_PEER,
        VeniceResponseStatusCategory.SUCCESS,
        BLOB_TRANSFER_DAVINCI_PEER_SUCCESSFUL_NUM_REQUESTS);
    registerRequestCounter(
        VeniceBlobTransferSource.DAVINCI_PEER,
        VeniceResponseStatusCategory.FAIL,
        BLOB_TRANSFER_DAVINCI_PEER_FAILED_NUM_REQUESTS);
    registerRequestCounter(
        VeniceBlobTransferSource.VENICE_SERVER,
        VeniceResponseStatusCategory.SUCCESS,
        BLOB_TRANSFER_VENICE_SERVER_SUCCESSFUL_NUM_REQUESTS);
    registerRequestCounter(
        VeniceBlobTransferSource.VENICE_SERVER,
        VeniceResponseStatusCategory.FAIL,
        BLOB_TRANSFER_VENICE_SERVER_FAILED_NUM_REQUESTS);

    kafkaFallbackCounters.put(
        VeniceBlobTransferFallbackReason.NO_CANDIDATES,
        new CountSensor(localMetricRepository, BLOB_TRANSFER_KAFKA_FALLBACK_NO_CANDIDATES));
    kafkaFallbackCounters.put(
        VeniceBlobTransferFallbackReason.ALL_HOSTS_FAILED,
        new CountSensor(localMetricRepository, BLOB_TRANSFER_KAFKA_FALLBACK_ALL_HOSTS_FAILED));

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

  public void recordBlobTransferRequest(VeniceBlobTransferSource source, VeniceResponseStatusCategory status) {
    requestCounters.get(source).get(status).record();
  }

  public void recordBlobTransferKafkaFallback(VeniceBlobTransferFallbackReason reason) {
    kafkaFallbackCounters.get(reason).record();
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

  public double getBlobTransferRequestCount(VeniceBlobTransferSource source, VeniceResponseStatusCategory status) {
    return requestCounters.get(source).get(status).measure();
  }

  public double getBlobTransferKafkaFallbackCount(VeniceBlobTransferFallbackReason reason) {
    return kafkaFallbackCounters.get(reason).measure();
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

  private void registerRequestCounter(
      VeniceBlobTransferSource source,
      VeniceResponseStatusCategory status,
      String sensorName) {
    requestCounters.computeIfAbsent(source, k -> new EnumMap<>(VeniceResponseStatusCategory.class))
        .put(status, new CountSensor(localMetricRepository, sensorName));
  }

  /** A Tehuti {@link Count} paired with the {@link Sensor} that feeds it. */
  private static class CountSensor {
    private final Count count = new Count();
    private final Sensor sensor;

    CountSensor(MetricsRepository metricsRepository, String sensorName) {
      this.sensor = metricsRepository.sensor(sensorName);
      this.sensor.add(sensorName, count);
    }

    void record() {
      sensor.record();
    }

    double measure() {
      return count.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
  }
}
