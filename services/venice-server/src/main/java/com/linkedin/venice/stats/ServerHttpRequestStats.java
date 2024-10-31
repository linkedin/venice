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
  private final Sensor readComputeSerializationLatencySensor;
  private final Sensor readComputeEfficiencySensor;
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
  private final Sensor misroutedStoreVersionSensor;
  private final Sensor flushLatencySensor;
  private final Sensor responseSizeSensor;

  private static final MetricsRepository dummySystemStoreMetricRepo = new MetricsRepository();

  public ServerHttpRequestStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      boolean isKeyValueProfilingEnabled,
      ServerHttpRequestStats totalStats,
      boolean isDaVinciClient) {
    super(isDaVinciClient ? dummySystemStoreMetricRepo : metricsRepository, storeName, requestType);

    /**
     * Check java doc of function: {@link TehutiUtils.RatioStat} to understand why choosing {@link Rate} instead of
     * {@link io.tehuti.metrics.stats.SampledStat}.
     */
    Rate successRequest = new OccurrenceRate();
    Rate errorRequest = new OccurrenceRate();
    successRequestSensor =
        registerPerStoreAndTotal("success_request", totalStats, () -> totalStats.successRequestSensor, successRequest);
    errorRequestSensor =
        registerPerStoreAndTotal("error_request", totalStats, () -> totalStats.errorRequestSensor, errorRequest);
    successRequestRatioSensor = registerSensor(
        "success_request_ratio",
        new TehutiUtils.RatioStat(successRequest, errorRequest, "success_request_ratio"));

    errorRequestLatencySensor = registerPerStoreAndTotal(
        "error_request_latency",
        totalStats,
        () -> totalStats.errorRequestLatencySensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(getName(), getFullMetricName("error_request_latency")));

    successRequestLatencySensor = registerPerStoreAndTotal(
        "success_request_latency",
        totalStats,
        () -> totalStats.successRequestLatencySensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(getName(), getFullMetricName("success_request_latency")));
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
        TehutiUtils.getPercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_query_latency_for_small_value")));
    databaseLookupLatencyForLargeValueSensor = registerPerStoreAndTotal(
        "storage_engine_query_latency_for_large_value",
        totalStats,
        () -> totalStats.databaseLookupLatencyForLargeValueSensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_query_latency_for_large_value")));

    storageExecutionHandlerSubmissionWaitTime = registerSensor(
        "storage_execution_handler_submission_wait_time",
        TehutiUtils.getPercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_execution_handler_submission_wait_time")));

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
    multiChunkLargeValueCountSensor = registerPerStoreAndTotal(
        "storage_engine_large_value_lookup",
        totalStats,
        () -> totalStats.multiChunkLargeValueCountSensor,
        largeValueLookupStats.toArray(new MeasurableStat[0]));

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
        new TehutiUtils.SimpleRatioStat(successRequestKeyCount, requestKeyCount, "success_request_key_ratio"));

    requestFirstPartLatencySensor = registerPerStoreAndTotal(
        "request_first_part_latency",
        totalStats,
        () -> totalStats.requestFirstPartLatencySensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(getName(), getFullMetricName("request_first_part_latency")));
    requestSecondPartLatencySensor = registerPerStoreAndTotal(
        "request_second_part_latency",
        totalStats,
        () -> totalStats.requestSecondPartLatencySensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(getName(), getFullMetricName("request_second_part_latency")));

    requestPartsInvokeDelayLatencySensor = registerPerStoreAndTotal(
        "request_parts_invoke_delay_latency",
        totalStats,
        () -> totalStats.requestPartsInvokeDelayLatencySensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(getName(), getFullMetricName("request_parts_invoke_delay_latency")));

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
        TehutiUtils
            .getPercentileStatWithAvgAndMax(getName(), getFullMetricName("storage_engine_read_compute_latency")));
    readComputeLatencyForSmallValueSensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_latency_for_small_value",
        totalStats,
        () -> totalStats.readComputeLatencyForSmallValueSensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_read_compute_latency_for_small_value")));
    readComputeLatencyForLargeValueSensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_latency_for_large_value",
        totalStats,
        () -> totalStats.readComputeLatencyForLargeValueSensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_read_compute_latency_for_large_value")));

    readComputeDeserializationLatencySensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_deserialization_latency",
        totalStats,
        () -> totalStats.readComputeDeserializationLatencySensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_read_compute_deserialization_latency")));

    readComputeSerializationLatencySensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_serialization_latency",
        totalStats,
        () -> totalStats.readComputeSerializationLatencySensor,
        TehutiUtils.getPercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_read_compute_serialization_latency")));

    readComputeEfficiencySensor = registerPerStoreAndTotal(
        "storage_engine_read_compute_efficiency",
        totalStats,
        () -> totalStats.readComputeEfficiencySensor,
        new Avg(),
        new Min(),
        new Max());

    /**
     * Total will reflect counts for the entire server host, while Avg will reflect the counts for each request.
     */
    dotProductCountSensor = registerPerStoreAndTotal(
        "dot_product_count",
        totalStats,
        () -> totalStats.dotProductCountSensor,
        avgAndTotal());
    cosineSimilaritySensor = registerPerStoreAndTotal(
        "cosine_similarity_count",
        totalStats,
        () -> totalStats.cosineSimilaritySensor,
        avgAndTotal());
    hadamardProductSensor = registerPerStoreAndTotal(
        "hadamard_product_count",
        totalStats,
        () -> totalStats.hadamardProductSensor,
        avgAndTotal());
    countOperatorSensor = registerPerStoreAndTotal(
        "count_operator_count",
        totalStats,
        () -> totalStats.countOperatorSensor,
        avgAndTotal());

    earlyTerminatedEarlyRequestCountSensor = registerPerStoreAndTotal(
        "early_terminated_request_count",
        totalStats,
        () -> totalStats.earlyTerminatedEarlyRequestCountSensor,
        new OccurrenceRate());

    if (isKeyValueProfilingEnabled || requestType == RequestType.SINGLE_GET) {
      // size profiling is only expensive for requests with lots of keys, but we keep it always on for single gets...
      String requestValueSizeSensorName = "request_value_size";
      requestValueSizeSensor = registerPerStoreAndTotal(
          requestValueSizeSensorName,
          totalStats,
          () -> totalStats.requestValueSizeSensor,
          TehutiUtils
              .getFineGrainedPercentileStatWithAvgAndMax(getName(), getFullMetricName(requestValueSizeSensorName)));
      String requestKeySizeSensorName = "request_key_size";
      requestKeySizeSensor = registerPerStoreAndTotal(
          requestKeySizeSensorName,
          totalStats,
          () -> totalStats.requestKeySizeSensor,
          TehutiUtils
              .getFineGrainedPercentileStatWithAvgAndMax(getName(), getFullMetricName(requestKeySizeSensorName)));
    }
    misroutedStoreVersionSensor = registerPerStoreAndTotal(
        "misrouted_store_version_request_count",
        totalStats,
        () -> totalStats.misroutedStoreVersionSensor,
        new OccurrenceRate());
    flushLatencySensor = registerPerStoreAndTotal(
        "flush_latency",
        totalStats,
        () -> totalStats.flushLatencySensor,
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("flush_latency")));
    responseSizeSensor = registerPerStoreAndTotal(
        "response_size",
        totalStats,
        () -> totalStats.responseSizeSensor,
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_size")));
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
  }

  public void recordReadComputeSerializationLatency(double latency, boolean assembledMultiChunkLargeValue) {
    readComputeSerializationLatencySensor.record(latency);
  }

  public void recordReadComputeEfficiency(double efficiency) {
    readComputeEfficiencySensor.record(efficiency);
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

  public void recordKeySizeInByte(int keySize) {
    requestKeySizeSensor.record(keySize);
  }

  public void recordValueSizeInByte(int valueSize) {
    requestValueSizeSensor.record(valueSize);
  }

  public void recordMisroutedStoreVersionRequest() {
    misroutedStoreVersionSensor.record();
  }

  public void recordFlushLatency(double latency) {
    flushLatencySensor.record(latency);
  }

  public void recordResponseSize(int size) {
    responseSizeSensor.record(size);
  }
}
