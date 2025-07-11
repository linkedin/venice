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
  private final Sensor requestSizeInBytesSensor;
  private final Sensor storageExecutionHandlerSubmissionWaitTime;
  private final Sensor storageExecutionQueueLenSensor;

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
  private final Sensor successRequestRatioSensor;
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
        TehutiUtils.get99PercentileStatWithAvgAndMax(getName(), getFullMetricName("storage_engine_query_latency")));
    databaseLookupLatencyForSmallValueSensor = registerPerStoreAndTotal(
        "storage_engine_query_latency_for_small_value",
        totalStats,
        () -> totalStats.databaseLookupLatencyForSmallValueSensor,
        TehutiUtils.get99PercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_query_latency_for_small_value")));
    databaseLookupLatencyForLargeValueSensor = registerPerStoreAndTotal(
        "storage_engine_query_latency_for_large_value",
        totalStats,
        () -> totalStats.databaseLookupLatencyForLargeValueSensor,
        TehutiUtils.get99PercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_engine_query_latency_for_large_value")));

    storageExecutionHandlerSubmissionWaitTime = registerOnlyTotalSensor(
        "storage_execution_handler_submission_wait_time",
        totalStats,
        () -> totalStats.storageExecutionHandlerSubmissionWaitTime,
        TehutiUtils.get99PercentileStatWithAvgAndMax(
            getName(),
            getFullMetricName("storage_execution_handler_submission_wait_time")));

    storageExecutionQueueLenSensor = registerOnlyTotalSensor(
        "storage_execution_queue_len",
        totalStats,
        () -> totalStats.storageExecutionQueueLenSensor,
        new Max(),
        new Avg());

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
    if (requestType != RequestType.SINGLE_GET) {
      /**
       * It is duplicate to have the key count tracking for single-get requests since the key count rate will be same
       * as the request rate.
       */
      requestKeyCountSensor = registerPerStoreAndTotal(
          "request_key_count",
          totalStats,
          () -> totalStats.requestKeyCountSensor,
          new Rate(),
          requestKeyCount,
          new Avg(),
          new Max());
    } else {
      requestKeyCountSensor = null;
    }
    requestSizeInBytesSensor = registerPerStoreAndTotal(
        "request_size_in_bytes",
        totalStats,
        () -> totalStats.requestSizeInBytesSensor,
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
    if (requestType == RequestType.COMPUTE) {
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
    } else {
      dotProductCountSensor = null;
      cosineSimilaritySensor = null;
      hadamardProductSensor = null;
      countOperatorSensor = null;
    }

    earlyTerminatedEarlyRequestCountSensor = registerPerStoreAndTotal(
        "early_terminated_request_count",
        totalStats,
        () -> totalStats.earlyTerminatedEarlyRequestCountSensor,
        new OccurrenceRate());

    if (isKeyValueProfilingEnabled || requestType == RequestType.SINGLE_GET) {
      final MeasurableStat[] valueSizeStats;
      final MeasurableStat[] keySizeStats;
      String requestValueSizeSensorName = "request_value_size";
      String requestKeySizeSensorName = "request_key_size";
      if (isKeyValueProfilingEnabled) {
        valueSizeStats = TehutiUtils
            .getFineGrainedPercentileStatWithAvgAndMax(getName(), getFullMetricName(requestValueSizeSensorName));
        keySizeStats = TehutiUtils
            .getFineGrainedPercentileStatWithAvgAndMax(getName(), getFullMetricName(requestKeySizeSensorName));
      } else {
        valueSizeStats = new MeasurableStat[] { new Avg(), new Max() };
        keySizeStats = new MeasurableStat[] { new Avg(), new Max() };
      }

      requestValueSizeSensor = registerPerStoreAndTotal(
          requestValueSizeSensorName,
          totalStats,
          () -> totalStats.requestValueSizeSensor,
          valueSizeStats);
      requestKeySizeSensor = registerPerStoreAndTotal(
          requestKeySizeSensorName,
          totalStats,
          () -> totalStats.requestKeySizeSensor,
          keySizeStats);
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
    if (requestKeyCountSensor != null) {
      requestKeyCountSensor.record(keyCount);
    }
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
    if (dotProductCountSensor != null) {
      dotProductCountSensor.record(count);
    }
  }

  public void recordCosineSimilarityCount(int count) {
    if (cosineSimilaritySensor != null) {
      cosineSimilaritySensor.record(count);
    }
  }

  public void recordHadamardProduct(int count) {
    if (hadamardProductSensor != null) {
      hadamardProductSensor.record(count);
    }
  }

  public void recordCountOperator(int count) {
    if (countOperatorSensor != null) {
      countOperatorSensor.record(count);
    }
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
