package com.linkedin.venice.client.stats;

import static com.linkedin.venice.client.stats.ClientMetricEntity.RETRY_CALL_COUNT;
import static com.linkedin.venice.client.stats.ClientStats.ClientTehutiMetricName.APP_TIMED_OUT_REQUEST;
import static com.linkedin.venice.client.stats.ClientStats.ClientTehutiMetricName.APP_TIMED_OUT_REQUEST_RESULT_RATIO;
import static com.linkedin.venice.client.stats.ClientStats.ClientTehutiMetricName.CLIENT_FUTURE_TIMEOUT;
import static com.linkedin.venice.client.stats.ClientStats.ClientTehutiMetricName.SUCCESS_REQUEST_DUPLICATE_KEY_COUNT;
import static com.linkedin.venice.stats.dimensions.RequestRetryType.ERROR_RETRY;
import static com.linkedin.venice.stats.dimensions.StreamProgress.FIRST;
import static com.linkedin.venice.stats.dimensions.StreamProgress.PCT_50;
import static com.linkedin.venice.stats.dimensions.StreamProgress.PCT_90;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.ClientType;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.StreamProgress;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;


/**
 * This class provides the stats for Venice client.
 */

public class ClientStats extends BasicClientStats {
  private final Map<Integer, Sensor> httpStatusSensorMap = new VeniceConcurrentHashMap<>();
  private final MetricEntityStateBase appTimedOutRequestCount;
  private final Sensor retryKeySuccessRatioSensor;
  /**
   * Tracks the number of keys handled via MultiGet fallback mechanism for Client-Compute.
   */
  private final Sensor multiGetFallbackSensor;

  private final MetricEntityStateOneEnum<RequestRetryType> errorRetryRequest;
  private final MetricEntityStateBase retryKeyCount;
  private final MetricEntityStateBase retrySuccessKeyCount;
  private final MetricEntityStateBase requestSerializationTime;
  private final MetricEntityStateBase requestSubmissionToResponseHandlingTime;
  private final MetricEntityStateBase responseDeserializationTime;
  private final MetricEntityStateBase responseDecompressionTime;
  private final MetricEntityStateOneEnum<StreamProgress> batchStreamProgressTimeToReceiveFirstRecord;
  private final MetricEntityStateOneEnum<StreamProgress> batchStreamProgressTimeToReceiveP50thRecord;
  private final MetricEntityStateOneEnum<StreamProgress> batchStreamProgressTimeToReceiveP90thRecord;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> successRequestDuplicateKeyCount;
  private final MetricEntityStateBase clientFutureTimeout;
  private final MetricEntityStateBase appTimedOutRequestResultRatio;

  public static ClientStats getClientStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      ClientConfig clientConfig,
      ClientType clientType) {
    String prefix = clientConfig == null ? null : clientConfig.getStatsPrefix();
    String metricName = prefix == null || prefix.isEmpty() ? storeName : prefix + "." + storeName;
    return new ClientStats(metricsRepository, metricName, requestType, clientType);
  }

  protected ClientStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      ClientType clientType) {
    super(metricsRepository, storeName, requestType, clientType);

    /**
     * Check java doc of function: {@link TehutiUtils.RatioStat} to understand why choosing {@link Rate} instead of
     * {@link io.tehuti.metrics.stats.SampledStat}.
     */
    Rate requestRetryCountRate = new OccurrenceRate();

    errorRetryRequest = MetricEntityStateOneEnum.create(
        RETRY_CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClientTehutiMetricName.REQUEST_RETRY_COUNT,
        Collections.singletonList(requestRetryCountRate),
        baseDimensionsMap,
        RequestRetryType.class);

    successRequestDuplicateKeyCount = MetricEntityStateOneEnum.create(
        ClientMetricEntity.REQUEST_DUPLICATE_KEY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        SUCCESS_REQUEST_DUPLICATE_KEY_COUNT,
        Collections.singletonList(new Rate()),
        baseDimensionsMap,
        VeniceResponseStatusCategory.class);
    /**
     * The time it took to serialize the request, to be sent to the router. This is done in a blocking fashion
     * on the caller's thread.
     */
    requestSerializationTime = MetricEntityStateBase.create(
        ClientMetricEntity.REQUEST_SERIALIZATION_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.REQUEST_SERIALIZATION_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    /**
     * The time it took between sending the request to the router and beginning to process the response.
     */
    requestSubmissionToResponseHandlingTime = MetricEntityStateBase.create(
        ClientMetricEntity.CALL_SUBMISSION_TO_HANDLING_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.REQUEST_SUBMISSION_TO_RESPONSE_HANDLING_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    /**
     * The total time it took to process the response (deserialization).
     */
    responseDeserializationTime = MetricEntityStateBase.create(
        ClientMetricEntity.RESPONSE_DESERIALIZATION_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_DESERIALIZATION_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    // response decompression time
    responseDecompressionTime = MetricEntityStateBase.create(
        ClientMetricEntity.RESPONSE_DECOMPRESSION_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_DECOMPRESSION_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    /**
     * Metrics to track the latency of each proportion of results received.
     */
    batchStreamProgressTimeToReceiveFirstRecord = MetricEntityStateOneEnum.create(
        ClientMetricEntity.RESPONSE_BATCH_STREAM_PROGRESS_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_TTFR,
        Collections.singletonList(new Avg()),
        baseDimensionsMap,
        StreamProgress.class);
    // TT50PR
    batchStreamProgressTimeToReceiveP50thRecord = MetricEntityStateOneEnum.create(
        ClientMetricEntity.RESPONSE_BATCH_STREAM_PROGRESS_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_TT50PR,
        Collections.singletonList(new Avg()),
        baseDimensionsMap,
        StreamProgress.class);
    // TT90PR
    batchStreamProgressTimeToReceiveP90thRecord = MetricEntityStateOneEnum.create(
        ClientMetricEntity.RESPONSE_BATCH_STREAM_PROGRESS_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_TT90PR,
        Collections.singletonList(new Avg()),
        baseDimensionsMap,
        StreamProgress.class);

    /**
     * Metrics to track the timed-out requests.
     * Just to be aware of that the timeout request here is not actually D2 timeout, but just the timeout when Venice
     * customers are retrieving Venice response in this way:
     * client.streamingBatchGet(keys).get(timeout, unit);
     *
     * This timeout behavior could actually happen before the D2 timeout, which is specified/configured in a different way.
     */
    appTimedOutRequestCount = MetricEntityStateBase.create(
        ClientMetricEntity.REQUEST_TIMEOUT_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        APP_TIMED_OUT_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    appTimedOutRequestResultRatio = MetricEntityStateBase.create(
        ClientMetricEntity.REQUEST_TIMEOUT_PARTIAL_RESPONSE_RATIO.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        APP_TIMED_OUT_REQUEST_RESULT_RATIO,
        Arrays.asList(new Avg(), new Min(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    clientFutureTimeout = MetricEntityStateBase.create(
        ClientMetricEntity.REQUEST_TIMEOUT_REQUESTED_DURATION.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        CLIENT_FUTURE_TIMEOUT,
        Arrays.asList(new Avg(), new Min(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    /* Metrics relevant to track long tail retry efficacy for batch get*/
    Rate retryRequestKeyCount = new Rate();
    Rate retryRequestSuccessKeyCount = new Rate();

    retryKeyCount = MetricEntityStateBase.create(
        ClientMetricEntity.RETRY_REQUEST_KEY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClientTehutiMetricName.RETRY_REQUEST_KEY_COUNT,
        Arrays.asList(retryRequestKeyCount, new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    retrySuccessKeyCount = MetricEntityStateBase.create(
        ClientMetricEntity.RETRY_RESPONSE_KEY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClientTehutiMetricName.RETRY_REQUEST_SUCCESS_KEY_COUNT,
        Arrays.asList(retryRequestSuccessKeyCount, new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    retryKeySuccessRatioSensor = registerSensor(
        new TehutiUtils.SimpleRatioStat(
            retryRequestSuccessKeyCount,
            getSuccessRequestKeyCountRate(),
            "retry_key_success_ratio"));
    multiGetFallbackSensor = registerSensor("multiget_fallback", new OccurrenceRate());
  }

  public void recordHttpRequest(int httpStatus) {
    httpStatusSensorMap
        .computeIfAbsent(httpStatus, status -> registerSensor("http_" + httpStatus + "_request", new OccurrenceRate()))
        .record();
  }

  public void recordErrorRetryRequest() {
    errorRetryRequest.record(1, ERROR_RETRY);
  }

  public void recordSuccessDuplicateRequestKeyCount(int duplicateKeyCount) {
    successRequestDuplicateKeyCount.record(duplicateKeyCount, SUCCESS);
  }

  public void recordRequestSerializationTime(double latency) {
    requestSerializationTime.record(latency);
  }

  public void recordRequestSubmissionToResponseHandlingTime(double latency) {
    requestSubmissionToResponseHandlingTime.record(latency);
  }

  public void recordResponseDeserializationTime(double latency) {
    responseDeserializationTime.record(latency);
  }

  public void recordResponseDecompressionTime(double latency) {
    responseDecompressionTime.record(latency);
  }

  public void recordStreamingResponseTimeToReceiveFirstRecord(double latency) {
    batchStreamProgressTimeToReceiveFirstRecord.record(latency, FIRST);
  }

  public void recordStreamingResponseTimeToReceive50PctRecord(double latency) {
    batchStreamProgressTimeToReceiveP50thRecord.record(latency, PCT_50);
  }

  public void recordStreamingResponseTimeToReceive90PctRecord(double latency) {
    batchStreamProgressTimeToReceiveP90thRecord.record(latency, PCT_90);
  }

  public void recordAppTimedOutRequest() {
    appTimedOutRequestCount.record(1);
  }

  public void recordAppTimedOutRequestResultRatio(double ratio) {
    appTimedOutRequestResultRatio.record(ratio);
  }

  public void recordClientFutureTimeout(long timeout) {
    clientFutureTimeout.record(timeout);
  }

  public void recordRetryRequestKeyCount(int numberOfKeysSentInRetryRequest) {
    retryKeyCount.record(numberOfKeysSentInRetryRequest);
  }

  public void recordRetryRequestSuccessKeyCount(int numberOfKeysCompletedInRetryRequest) {
    retrySuccessKeyCount.record(numberOfKeysCompletedInRetryRequest);
  }

  public void recordMultiGetFallback(int keyCount) {
    multiGetFallbackSensor.record(keyCount);
  }

  /**
   * Metric names for tehuti metrics used in this class.
   */
  public enum ClientTehutiMetricName implements com.linkedin.venice.stats.metrics.TehutiMetricNameEnum {
    REQUEST_RETRY_COUNT, RETRY_REQUEST_KEY_COUNT, RETRY_REQUEST_SUCCESS_KEY_COUNT, SUCCESS_REQUEST_DUPLICATE_KEY_COUNT,
    CLIENT_FUTURE_TIMEOUT, APP_TIMED_OUT_REQUEST_RESULT_RATIO, APP_TIMED_OUT_REQUEST, REQUEST_SERIALIZATION_TIME,
    REQUEST_SUBMISSION_TO_RESPONSE_HANDLING_TIME, RESPONSE_DECOMPRESSION_TIME, RESPONSE_DESERIALIZATION_TIME,
    RESPONSE_TTFR, RESPONSE_TT50PR, RESPONSE_TT90PR;

    private final String metricName;

    ClientTehutiMetricName() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }
}
