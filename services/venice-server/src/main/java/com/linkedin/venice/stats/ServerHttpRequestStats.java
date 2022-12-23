package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.Total;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;


/**
 * {@code ServerHttpRequestStats} contains a list of counters in order to mainly measure the performance of
 * handling requests from Routers.
 */
public class ServerHttpRequestStats extends AbstractVeniceHttpStats {
  private final Sensor successRequestSensor;
  private final Sensor errorRequestSensor;
  private final Sensor successRequestLatencySensor;
  private final Sensor errorRequestLatencySensor;
  private final Sensor databaseLookupLatencySensor;
  private final Sensor databaseLookupLatencyForSmallValueSensor;
  private final Sensor databaseLookupLatencyForLargeValueSensor;
  private final Sensor multiChunkLargeValueCountSensor;
  private final Sensor requestKeyCountSensor;
  private final Sensor successRequestKeyCountSensor;
  private final Sensor requestSizeInBytesSensor;
  private final Sensor storageExecutionHandlerSubmissionWaitTime;
  private final Sensor storageExecutionQueueLenSensor;

  private final Sensor requestFirstPartLatencySensor;
  private final Sensor requestSecondPartLatencySensor;
  private final Sensor requestPartsInvokeDelayLatencySensor;
  private final Sensor requestPartCountSensor;

  private final Sensor readComputeLatencySensor;
  private final Sensor readComputeLatencyForSmallValueSensor;
  private final Sensor readComputeLatencyForLargeValueSensor;
  private final Sensor readComputeDeserializationLatencySensor;
  private final Sensor readComputeDeserializationLatencyForSmallValueSensor;
  private final Sensor readComputeDeserializationLatencyForLargeValueSensor;
  private final Sensor readComputeSerializationLatencySensor;
  private final Sensor readComputeSerializationLatencyForSmallValueSensor;
  private final Sensor readComputeSerializationLatencyForLargeValueSensor;
  private final Sensor dotProductCountSensor;
  private final Sensor cosineSimilaritySensor;
  private final Sensor hadamardProductSensor;
  private final Sensor countOperatorSensor;

  private final Sensor earlyTerminatedEarlyRequestCountSensor;

  private Sensor requestKeySizeSensor;
  private Sensor requestValueSizeSensor;

  // Ratio sensors are not directly written to, but they still get their state updated indirectly
  @SuppressWarnings("unused")
  private final Sensor successRequestKeyRatioSensor, successRequestRatioSensor;

  public ServerHttpRequestStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    this(metricsRepository, storeName, requestType, false, null);
  }

  public ServerHttpRequestStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      boolean isKeyValueProfilingEnabled,
      ServerHttpRequestStats totalStats) {
    super(metricsRepository, storeName, requestType);

    /**
     * Check java doc of function: {@link TehutiUtils.RatioStat} to understand why choosing {@link Rate} instead of
     * {@link io.tehuti.metrics.stats.SampledStat}.
     */
    Rate successRequest = new OccurrenceRate();
    Rate errorRequest = new OccurrenceRate();
    successRequestSensor = registerSensor("success_request", successRequest);
    errorRequestSensor = registerSensor("error_request", errorRequest);
    successRequestLatencySensor = getPercentileStatSensor("success_request_latency");
    errorRequestLatencySensor = getPercentileStatSensor("error_request_latency");
    successRequestRatioSensor =
        registerSensor("success_request_ratio", new TehutiUtils.RatioStat(successRequest, errorRequest));

    databaseLookupLatencySensor = registerPerStoreAndTotal(
        "storage_engine_query_latency",
        totalStats,
        () -> totalStats.databaseLookupLatencySensor,
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("storage_engine_query_latency")),
        new Avg(),
        new Max());
    databaseLookupLatencyForSmallValueSensor = registerPerStoreAndTotal(
        "storage_engine_query_latency_for_small_value",
        totalStats,
        () -> totalStats.databaseLookupLatencyForSmallValueSensor,
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("storage_engine_query_latency_for_small_value")),
        new Avg(),
        new Max());
    databaseLookupLatencyForLargeValueSensor = registerPerStoreAndTotal(
        "storage_engine_query_latency_for_large_value",
        totalStats,
        () -> totalStats.databaseLookupLatencyForLargeValueSensor,
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("storage_engine_query_latency_for_large_value")),
        new Avg(),
        new Max());

    storageExecutionHandlerSubmissionWaitTime = registerSensor(
        "storage_execution_handler_submission_wait_time",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("storage_execution_handler_submission_wait_time")),
        new Max(),
        new Avg());

    storageExecutionQueueLenSensor = registerSensor("storage_execution_queue_len", new Max(), new Avg());

    List<MeasurableStat> largeValueLookupStats = new ArrayList();

    /**
     * This is the max number of large values assembled per query. Useful to know if a given
     * store is currently exercising the large value feature on the read path, or not.
     *
     * For single gets, valid values would be 0 or 1.
     * For batch gets, valid values would be between 0 and {@link requestKeyCountSensor}'s Max.
     */
    largeValueLookupStats.add(new Max(0));

    /**
     * This represents the rate of requests which included at least one large value.
     */
    largeValueLookupStats.add(new OccurrenceRate());
    if (RequestType.MULTI_GET == requestType) {
      /**
       * This represents the average number of large values contained within a given batch get.
       *
       * N.B.: This is not useful for single get, as it will always be equal to Max, since we
       *       only record the metric when at least one large value look up occurred.
       */
      largeValueLookupStats.add(new Avg());

      /**
       * This represents the total rate of large values getting re-assembled, across all batch
       * gets.
       *
       * N.B.: This is only useful for batch gets. If we included this metric for single gets,
       *       it would always be equal to the OccurrenceRate, since single gets that include
       *       large value will only ever contain exactly 1 large value.
       */
      largeValueLookupStats.add(new Rate());
    }
    // multiChunkLargeValueCountSensor = registerSensor(
    // "storage_engine_large_value_lookup",
    // largeValueLookupStats.toArray(new MeasurableStat[largeValueLookupStats.size()]));
    multiChunkLargeValueCountSensor = registerPerStoreAndTotal(
        "storage_engine_large_value_lookup",
        totalStats,
        () -> totalStats.multiChunkLargeValueCountSensor,
        largeValueLookupStats.toArray(new MeasurableStat[largeValueLookupStats.size()]));

    Rate requestKeyCount = new OccurrenceRate();
    Rate successRequestKeyCount = new OccurrenceRate();
    requestKeyCountSensor = registerPerStoreAndTotal(
        "request_key_count",
        totalStats,
        () -> totalStats.requestKeyCountSensor,
        new Rate(),
        requestKeyCount,
        new Avg(),
        new Max());
    successRequestKeyCountSensor = registerPerStoreAndTotal(
        "success_request_key_count",
        totalStats,
        () -> totalStats.successRequestKeyCountSensor,
        new Rate(),
        successRequestKeyCount,
        new Avg(),
        new Max());
    requestSizeInBytesSensor = registerPerStoreAndTotal(
        "request_size_in_bytes",
        totalStats,
        () -> totalStats.requestSizeInBytesSensor,
        new Avg(),
        new Min(),
        new Max());
    successRequestKeyRatioSensor = registerSensor(
        "success_request_key_ratio",
        new TehutiUtils.SimpleRatioStat(successRequestKeyCount, requestKeyCount));

    requestFirstPartLatencySensor = registerPerStoreAndTotal(
        "request_first_part_latency",
        totalStats,
        () -> totalStats.requestFirstPartLatencySensor,
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("request_first_part_latency")),
        new Avg(),
        new Max());
    requestSecondPartLatencySensor = registerPerStoreAndTotal(
        "request_second_part_latency",
        totalStats,
        () -> totalStats.requestSecondPartLatencySensor,
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("request_second_part_latency")),
        new Avg(),
        new Max());

    requestPartsInvokeDelayLatencySensor = registerPerStoreAndTotal(
        "request_parts_invoke_delay_latency",
        totalStats,
        () -> totalStats.requestPartsInvokeDelayLatencySensor,
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("request_parts_invoke_delay_latency")),
        new Avg(),
        new Max());

    requestPartCountSensor = registerPerStoreAndTotal(
        "request_part_count",
        totalStats,
        () -> totalStats.requestPartCountSensor,
        new Avg(),
        new Min(),
        new Max());

    readComputeLatencySensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_latency",
        totalStats,
        () -> totalStats.readComputeLatencySensor,
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("storage_engine_read_compute_latency")),
        new Avg(),
        new Max());
    readComputeLatencyForSmallValueSensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_latency_for_small_value",
        totalStats,
        () -> totalStats.readComputeLatencyForSmallValueSensor,
        TehutiUtils
            .getPercentileStat(getName(), getFullMetricName("storage_engine_read_compute_latency_for_small_value")),
        new Avg(),
        new Max());
    readComputeLatencyForLargeValueSensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_latency_for_large_value",
        totalStats,
        () -> totalStats.readComputeLatencyForLargeValueSensor,
        TehutiUtils
            .getPercentileStat(getName(), getFullMetricName("storage_engine_read_compute_latency_for_large_value")),
        new Avg(),
        new Max());

    readComputeDeserializationLatencySensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_deserialization_latency",
        totalStats,
        () -> totalStats.readComputeDeserializationLatencySensor,
        TehutiUtils
            .getPercentileStat(getName(), getFullMetricName("storage_engine_read_compute_latency_for_large_value")),
        new Avg(),
        new Max());
    readComputeDeserializationLatencyForSmallValueSensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_deserialization_latency_for_small_value",
        totalStats,
        () -> totalStats.readComputeDeserializationLatencyForSmallValueSensor,
        TehutiUtils.getPercentileStat(
            getName(),
            getFullMetricName("storage_engine_read_compute_deserialization_latency_for_small_value")),
        new Avg(),
        new Max());
    readComputeDeserializationLatencyForLargeValueSensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_deserialization_latency_for_large_value",
        totalStats,
        () -> totalStats.readComputeDeserializationLatencyForLargeValueSensor,
        TehutiUtils.getPercentileStat(
            getName(),
            getFullMetricName("storage_engine_read_compute_deserialization_latency_for_large_value")),
        new Avg(),
        new Max());

    readComputeSerializationLatencySensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_serialization_latency",
        totalStats,
        () -> totalStats.readComputeSerializationLatencySensor,
        TehutiUtils
            .getPercentileStat(getName(), getFullMetricName("storage_engine_read_compute_serialization_latency")),
        new Avg(),
        new Max());

    readComputeSerializationLatencyForSmallValueSensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_serialization_latency_for_small_value",
        totalStats,
        () -> totalStats.readComputeSerializationLatencyForSmallValueSensor,
        TehutiUtils.getPercentileStat(
            getName(),
            getFullMetricName("storage_engine_read_compute_serialization_latency_for_small_value")),
        new Avg(),
        new Max());

    readComputeSerializationLatencyForLargeValueSensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_serialization_latency_for_large_value",
        totalStats,
        () -> totalStats.readComputeSerializationLatencyForLargeValueSensor,
        TehutiUtils.getPercentileStat(
            getName(),
            getFullMetricName("storage_engine_read_compute_serialization_latency_for_large_value")),
        new Avg(),
        new Max());

    /**
     * Total will reflect counts for the entire server host, while Avg will reflect the counts for each request.
     */
    dotProductCountSensor = registerPerStoreAndTotal(
        "dot_product_count",
        totalStats,
        () -> totalStats.dotProductCountSensor,
        new Total(),
        new Avg());
    cosineSimilaritySensor = registerPerStoreAndTotal(
        "cosine_similarity_count",
        totalStats,
        () -> totalStats.cosineSimilaritySensor,
        new Total(),
        new Avg());
    hadamardProductSensor = registerPerStoreAndTotal(
        "hadamard_product_count",
        totalStats,
        () -> totalStats.hadamardProductSensor,
        new Total(),
        new Avg());
    countOperatorSensor = registerPerStoreAndTotal(
        "count_operator_count",
        totalStats,
        () -> totalStats.countOperatorSensor,
        new Total(),
        new Avg());
    earlyTerminatedEarlyRequestCountSensor = registerPerStoreAndTotal(
        "early_terminated_request_count",
        totalStats,
        () -> totalStats.earlyTerminatedEarlyRequestCountSensor,
        new OccurrenceRate());

    if (isKeyValueProfilingEnabled) {
      String requestValueSizeSensorName = "request_value_size";
      requestValueSizeSensor = registerPerStoreAndTotal(
          requestValueSizeSensorName,
          totalStats,
          () -> totalStats.requestValueSizeSensor,
          new Avg(),
          new Max(),
          TehutiUtils.getFineGrainedPercentileStat(getName(), getFullMetricName(requestValueSizeSensorName)));
      String requestKeySizeSensorName = "request_key_size";
      requestKeySizeSensor = registerPerStoreAndTotal(
          requestKeySizeSensorName,
          totalStats,
          () -> totalStats.requestKeySizeSensor,
          new Avg(),
          new Max(),
          TehutiUtils.getFineGrainedPercentileStat(getName(), getFullMetricName(requestKeySizeSensorName)));
    }
  }

  private Sensor registerPerStoreAndTotal(
      String sensorName,
      ServerHttpRequestStats totalStats,
      Supplier<Sensor> totalSensor,
      MeasurableStat... stats) {
    Sensor[] parent = totalStats == null ? null : new Sensor[] { totalSensor.get() };
    return registerSensor(sensorName, parent, stats);
  }

  public void recordSuccessRequest() {
    successRequestSensor.record();
  }

  public void recordErrorRequest() {
    errorRequestSensor.record();
  }

  public void recordSuccessRequestLatency(double latency) {
    successRequestLatencySensor.record(latency);
  }

  public void recordErrorRequestLatency(double latency) {
    errorRequestLatencySensor.record(latency);
  }

  public void recordDatabaseLookupLatency(double latency, boolean assembledMultiChunkLargeValue) {
    databaseLookupLatencySensor.record(latency);
    if (assembledMultiChunkLargeValue) {
      databaseLookupLatencyForLargeValueSensor.record(latency);
    } else {
      databaseLookupLatencyForSmallValueSensor.record(latency);
    }
  }

  public void recordRequestKeyCount(int keyCount) {
    requestKeyCountSensor.record(keyCount);
  }

  public void recordSuccessRequestKeyCount(int successKeyCount) {
    successRequestKeyCountSensor.record(successKeyCount);
  }

  public void recordRequestSizeInBytes(int requestSizeInBytes) {
    requestSizeInBytesSensor.record(requestSizeInBytes);
  }

  public void recordMultiChunkLargeValueCount(int multiChunkLargeValueCount) {
    multiChunkLargeValueCountSensor.record(multiChunkLargeValueCount);
  }

  public void recordStorageExecutionHandlerSubmissionWaitTime(double submissionWaitTime) {
    storageExecutionHandlerSubmissionWaitTime.record(submissionWaitTime);
  }

  public void recordStorageExecutionQueueLen(int len) {
    storageExecutionQueueLenSensor.record(len);
  }

  public void recordRequestFirstPartLatency(double latency) {
    requestFirstPartLatencySensor.record(latency);
  }

  public void recordRequestSecondPartLatency(double latency) {
    requestSecondPartLatencySensor.record(latency);
  }

  public void recordRequestPartsInvokeDelayLatency(double latency) {
    requestPartsInvokeDelayLatencySensor.record(latency);
  }

  public void recordRequestPartCount(int partCount) {
    requestPartCountSensor.record(partCount);
  }

  public void recordReadComputeLatency(double latency, boolean assembledMultiChunkLargeValue) {
    readComputeLatencySensor.record(latency);
    if (assembledMultiChunkLargeValue) {
      readComputeLatencyForLargeValueSensor.record(latency);
    } else {
      readComputeLatencyForSmallValueSensor.record(latency);
    }
  }

  public void recordReadComputeDeserializationLatency(double latency, boolean assembledMultiChunkLargeValue) {
    readComputeDeserializationLatencySensor.record(latency);
    if (assembledMultiChunkLargeValue) {
      readComputeDeserializationLatencyForLargeValueSensor.record(latency);
    } else {
      readComputeDeserializationLatencyForSmallValueSensor.record(latency);
    }
  }

  public void recordReadComputeSerializationLatency(double latency, boolean assembledMultiChunkLargeValue) {
    readComputeSerializationLatencySensor.record(latency);
    if (assembledMultiChunkLargeValue) {
      readComputeSerializationLatencyForLargeValueSensor.record(latency);
    } else {
      readComputeSerializationLatencyForSmallValueSensor.record(latency);
    }
  }

  public void recordDotProductCount(int count) {
    dotProductCountSensor.record(count);
  }

  public void recordCosineSimilarityCount(int count) {
    cosineSimilaritySensor.record(count);
  }

  public void recordHadamardProduct(int count) {
    hadamardProductSensor.record(count);
  }

  public void recordCountOperator(int count) {
    countOperatorSensor.record(count);
  }

  public void recordEarlyTerminatedEarlyRequest() {
    earlyTerminatedEarlyRequestCountSensor.record();
  }

  public void recordKeySizeInByte(long keySize) {
    requestKeySizeSensor.record(keySize);
  }

  public void recordValueSizeInByte(long valueSize) {
    requestValueSizeSensor.record(valueSize);
  }

  private Sensor getPercentileStatSensor(String name) {
    return registerSensor(
        name,
        TehutiUtils.getPercentileStat(getName(), getFullMetricName(name)),
        new Avg(),
        new Max());
  }
}
