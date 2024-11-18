package com.linkedin.venice.router.stats;

import static com.linkedin.venice.router.RouterServer.ROUTER_SERVICE_METRIC_PREFIX;
import static com.linkedin.venice.router.RouterServer.ROUTER_SERVICE_NAME;
import static com.linkedin.venice.stats.AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT;
import static com.linkedin.venice.stats.dimensions.VeniceHttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_VALIDATION_OUTCOME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.LambdaStat;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceRequestRetryAbortReason;
import com.linkedin.venice.stats.dimensions.VeniceRequestRetryType;
import com.linkedin.venice.stats.dimensions.VeniceRequestValidationOutcome;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.Metric;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.Total;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.MissingArgumentException;


public class RouterHttpRequestStats extends AbstractVeniceHttpStats {
  private static final MetricConfig METRIC_CONFIG = new MetricConfig().timeWindow(10, TimeUnit.SECONDS);
  private static final VeniceMetricsRepository localMetricRepo;

  static {
    try {
      localMetricRepo = new VeniceMetricsRepository(
          new VeniceMetricsConfig.Builder().setServiceName(ROUTER_SERVICE_NAME)
              .setMetricPrefix(ROUTER_SERVICE_METRIC_PREFIX)
              .setTehutiMetricConfig(METRIC_CONFIG)
              .build());
    } catch (MissingArgumentException e) {
      throw new RuntimeException(e);
    }
  }

  private final static Sensor totalInflightRequestSensor = localMetricRepo.sensor("total_inflight_request");
  static {
    totalInflightRequestSensor.add("total_inflight_request_count", new Rate());
  }

  /** metrics to track incoming requests */
  private final Sensor incomingRequestSensor;
  private final LongCounter incomingRequestSensorOtel;

  /** metrics to track response handling */
  private final Sensor healthySensor;
  private final Sensor unhealthySensor;
  private final Sensor tardySensor;
  private final Sensor healthyRequestRateSensor;
  private final Sensor tardyRequestRatioSensor;
  private final Sensor throttleSensor;
  private final Sensor badRequestSensor;
  private final LongCounter requestSensorOtel;

  /** latency metrics */
  private final Sensor latencySensor;
  private final Sensor healthyRequestLatencySensor;
  private final Sensor unhealthyRequestLatencySensor;
  private final Sensor tardyRequestLatencySensor;
  private final Sensor throttledRequestLatencySensor;
  private final DoubleHistogram latencySensorOtel;

  /** retry metrics */
  private final Sensor errorRetryCountSensor;
  private final LongCounter retryTriggeredSensorOtel;
  private final Sensor allowedRetryRequestSensor;
  private final LongCounter allowedRetryRequestSensorOtel;
  private final Sensor disallowedRetryRequestSensor;
  private final LongCounter disallowedRetryRequestSensorOtel;
  private final Sensor retryDelaySensor;
  private final DoubleHistogram retryDelaySensorOtel;

  /** retry aborted metrics */
  private final Sensor delayConstraintAbortedRetryRequest;
  private final Sensor slowRouteAbortedRetryRequest;
  private final Sensor retryRouteLimitAbortedRetryRequest;
  private final Sensor noAvailableReplicaAbortedRetryRequest;
  private final LongCounter abortedRetrySensorOtel;

  /** key count metrics */
  private final Sensor keyNumSensor;
  private final Sensor badRequestKeyCountSensor;
  private final DoubleHistogram keyCountSensorOtel;

  /** OTel metrics yet to be added */
  private final Sensor requestSizeSensor;
  private final Sensor compressedResponseSizeSensor;
  private final Sensor responseSizeSensor;
  private final Sensor requestThrottledByRouterCapacitySensor;
  private final Sensor decompressionTimeSensor;
  private final Sensor routerResponseWaitingTimeSensor;
  private final Sensor fanoutRequestCountSensor;
  private final Sensor quotaSensor;
  private final Sensor findUnhealthyHostRequestSensor;
  // Reflect the real request usage, e.g count each key as an unit of request usage.
  private final Sensor requestUsageSensor;
  private final Sensor requestParsingLatencySensor;
  private final Sensor requestRoutingLatencySensor;
  private final Sensor unAvailableRequestSensor;
  private final Sensor readQuotaUsageSensor;
  private final Sensor inFlightRequestSensor;
  private final AtomicInteger currentInFlightRequest;
  private final Sensor unavailableReplicaStreamingRequestSensor;
  private final Sensor multiGetFallbackSensor;
  private final Sensor metaStoreShadowReadSensor;
  private Sensor keySizeSensor;

  /** TODO: Need to clarify the usage and add new OTel metrics or add it as a part of existing ones */
  private final Sensor errorRetryAttemptTriggeredByPendingRequestCheckSensor;

  private final String systemStoreName;
  private final Attributes commonMetricDimensions;
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricNamingFormat openTelemetryMetricFormat;

  // QPS metrics
  public RouterHttpRequestStats(
      VeniceMetricsRepository metricsRepository,
      String storeName,
      String clusterName,
      RequestType requestType,
      ScatterGatherStats scatterGatherStats,
      boolean isKeyValueProfilingEnabled) {
    super(metricsRepository, storeName, requestType);
    emitOpenTelemetryMetrics = metricsRepository.getVeniceMetricsConfig().emitOtelMetrics();
    openTelemetryMetricFormat = metricsRepository.getVeniceMetricsConfig().getMetricNamingFormat();
    commonMetricDimensions = Attributes.builder()
        .put(getDimensionName(VENICE_STORE_NAME), storeName)
        .put(getDimensionName(VENICE_REQUEST_METHOD), requestType.name().toLowerCase())
        .put(getDimensionName(VENICE_CLUSTER_NAME), clusterName)
        .build();

    this.systemStoreName = VeniceSystemStoreUtils.extractSystemStoreType(storeName);
    Rate requestRate = new OccurrenceRate();
    Rate healthyRequestRate = new OccurrenceRate();
    Rate tardyRequestRate = new OccurrenceRate();

    incomingRequestSensor = registerSensor("request", new Count(), requestRate);
    incomingRequestSensorOtel = metricsRepository.getOpenTelemetryMetricsRepository()
        .getCounter("incoming_call_count", "Number", "Count of all incoming requests");

    healthySensor = registerSensor("healthy_request", new Count(), healthyRequestRate);
    unhealthySensor = registerSensor("unhealthy_request", new Count());
    tardySensor = registerSensor("tardy_request", new Count(), tardyRequestRate);
    throttleSensor = registerSensor("throttled_request", new Count());
    healthyRequestRateSensor =
        registerSensor(new TehutiUtils.SimpleRatioStat(healthyRequestRate, requestRate, "healthy_request_ratio"));
    tardyRequestRatioSensor =
        registerSensor(new TehutiUtils.SimpleRatioStat(tardyRequestRate, requestRate, "tardy_request_ratio"));
    badRequestSensor = registerSensor("bad_request", new Count());
    requestSensorOtel = metricsRepository.getOpenTelemetryMetricsRepository()
        .getCounter("call_count", "Number", "Count of all requests with response details");

    errorRetryCountSensor = registerSensor("error_retry", new Count());
    retryTriggeredSensorOtel = metricsRepository.getOpenTelemetryMetricsRepository()
        .getCounter("retry_call_count", "Number", "Count of retries triggered");
    allowedRetryRequestSensor = registerSensor("allowed_retry_request_count", new OccurrenceRate());
    allowedRetryRequestSensorOtel = metricsRepository.getOpenTelemetryMetricsRepository()
        .getCounter("allowed_retry_call_count", "Number", "Count of allowed retry requests");
    disallowedRetryRequestSensor = registerSensor("disallowed_retry_request_count", new OccurrenceRate());
    disallowedRetryRequestSensorOtel = metricsRepository.getOpenTelemetryMetricsRepository()
        .getCounter("disallowed_retry_call_count", "Number", "Count of disallowed retry requests");
    errorRetryAttemptTriggeredByPendingRequestCheckSensor =
        registerSensor("error_retry_attempt_triggered_by_pending_request_check", new OccurrenceRate());
    retryDelaySensor = registerSensor("retry_delay", new Avg(), new Max());
    retryDelaySensorOtel = metricsRepository.getOpenTelemetryMetricsRepository()
        .getHistogramWithoutBuckets("retry_delay", TimeUnit.MILLISECONDS.name(), "Retry delay time");

    delayConstraintAbortedRetryRequest = registerSensor("delay_constraint_aborted_retry_request", new Count());
    slowRouteAbortedRetryRequest = registerSensor("slow_route_aborted_retry_request", new Count());
    retryRouteLimitAbortedRetryRequest = registerSensor("retry_route_limit_aborted_retry_request", new Count());
    noAvailableReplicaAbortedRetryRequest = registerSensor("no_available_replica_aborted_retry_request", new Count());
    abortedRetrySensorOtel = metricsRepository.getOpenTelemetryMetricsRepository()
        .getCounter("aborted_retry_call_count", "Number", "Count of aborted retry requests");

    unavailableReplicaStreamingRequestSensor = registerSensor("unavailable_replica_streaming_request", new Count());
    requestThrottledByRouterCapacitySensor = registerSensor("request_throttled_by_router_capacity", new Count());
    fanoutRequestCountSensor = registerSensor("fanout_request_count", new Avg(), new Max(0));

    latencySensor = registerSensorWithDetailedPercentiles("latency", new Avg(), new Max(0));
    healthyRequestLatencySensor =
        registerSensorWithDetailedPercentiles("healthy_request_latency", new Avg(), new Max(0));
    unhealthyRequestLatencySensor = registerSensor("unhealthy_request_latency", new Avg(), new Max(0));
    tardyRequestLatencySensor = registerSensor("tardy_request_latency", new Avg(), new Max(0));
    throttledRequestLatencySensor = registerSensor("throttled_request_latency", new Avg(), new Max(0));
    latencySensorOtel = metricsRepository.getOpenTelemetryMetricsRepository()
        .getHistogram("call_time", TimeUnit.MILLISECONDS.name(), "Latency based on all responses");

    routerResponseWaitingTimeSensor = registerSensor(
        "response_waiting_time",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_waiting_time")));
    requestSizeSensor = registerSensor(
        "request_size",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("request_size")),
        new Avg());
    compressedResponseSizeSensor = registerSensor(
        "compressed_response_size",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("compressed_response_size")),
        new Avg(),
        new Max());

    decompressionTimeSensor = registerSensor(
        "decompression_time",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("decompression_time")),
        new Avg());
    quotaSensor = registerSensor("read_quota_per_router", new Gauge());
    findUnhealthyHostRequestSensor = registerSensor("find_unhealthy_host_request", new OccurrenceRate());

    registerSensor(new LambdaStat((ignored, ignored2) -> scatterGatherStats.getTotalRetries(), "retry_count"));
    registerSensor(new LambdaStat((ignored, ignored2) -> scatterGatherStats.getTotalRetriedKeys(), "retry_key_count"));
    registerSensor(
        new LambdaStat(
            (ignored, ignored2) -> scatterGatherStats.getTotalRetriesDiscarded(),
            "retry_slower_than_original_count"));
    registerSensor(
        new LambdaStat((ignored, ignored2) -> scatterGatherStats.getTotalRetriesError(), "retry_error_count"));
    registerSensor(
        new LambdaStat(
            (ignored, ignored2) -> scatterGatherStats.getTotalRetriesWinner(),
            "retry_faster_than_original_count"));

    keyNumSensor = registerSensor("key_num", new Avg(), new Max(0));
    badRequestKeyCountSensor = registerSensor("bad_request_key_count", new OccurrenceRate(), new Avg(), new Max());
    keyCountSensorOtel = metricsRepository.getOpenTelemetryMetricsRepository()
        .getHistogramWithoutBuckets("call_key_count", "Number", "Count of keys in multi key requests");

    /**
     * request_usage.Total is incoming KPS while request_usage.OccurrenceRate is QPS
     */
    requestUsageSensor = registerSensor("request_usage", new Total(), new OccurrenceRate());
    multiGetFallbackSensor = registerSensor("multiget_fallback", new Total(), new OccurrenceRate());

    requestParsingLatencySensor = registerSensor("request_parse_latency", new Avg());
    requestRoutingLatencySensor = registerSensor("request_route_latency", new Avg());

    unAvailableRequestSensor = registerSensor("unavailable_request", new Count());

    readQuotaUsageSensor = registerSensor("read_quota_usage_kps", new Total());

    inFlightRequestSensor = registerSensor("in_flight_request_count", new Min(), new Max(0), new Avg());

    String responseSizeSensorName = "response_size";
    if (isKeyValueProfilingEnabled && storeName.equals(STORE_NAME_FOR_TOTAL_STAT)) {
      String keySizeSensorName = "key_size_in_byte";
      keySizeSensor = registerSensor(
          keySizeSensorName,
          new Avg(),
          new Max(),
          TehutiUtils.getFineGrainedPercentileStat(getName(), getFullMetricName(keySizeSensorName)));
      responseSizeSensor = registerSensor(
          responseSizeSensorName,
          new Avg(),
          new Max(),
          TehutiUtils.getFineGrainedPercentileStat(getName(), getFullMetricName(responseSizeSensorName)));
    } else {
      responseSizeSensor = registerSensor(
          responseSizeSensorName,
          new Avg(),
          new Max(),
          TehutiUtils.getPercentileStat(getName(), getFullMetricName(responseSizeSensorName)));
    }
    currentInFlightRequest = new AtomicInteger();

    metaStoreShadowReadSensor = registerSensor("meta_store_shadow_read", new OccurrenceRate());
  }

  private String getDimensionName(VeniceMetricsDimensions dimension) {
    return dimension.getDimensionName(openTelemetryMetricFormat);
  }

  /**
   * We record this at the beginning of request handling, so we don't know the latency yet... All specific
   * types of requests also have their latencies logged at the same time.
   */
  public void recordIncomingRequest() {
    incomingRequestSensor.record();
    inFlightRequestSensor.record(currentInFlightRequest.incrementAndGet());
    totalInflightRequestSensor.record();
    if (emitOpenTelemetryMetrics) {
      incomingRequestSensorOtel.add(1, commonMetricDimensions);
    }
  }

  public void recordHealthyRequest(Double latency, HttpResponseStatus responseStatus) {
    healthySensor.record();
    recordRequestSensorOtel(responseStatus, VeniceResponseStatusCategory.HEALTHY);
    if (latency != null) {
      healthyRequestLatencySensor.record(latency);
      recordLatencySensorOtel(latency, responseStatus, VeniceResponseStatusCategory.HEALTHY);
    }
  }

  public void recordUnhealthyRequest(HttpResponseStatus responseStatus) {
    unhealthySensor.record();
    recordRequestSensorOtel(responseStatus, VeniceResponseStatusCategory.UNHEALTHY);
  }

  public void recordUnhealthyRequest(double latency, HttpResponseStatus responseStatus) {
    recordUnhealthyRequest(responseStatus);
    unhealthyRequestLatencySensor.record(latency);
    recordLatencySensorOtel(latency, responseStatus, VeniceResponseStatusCategory.UNHEALTHY);
  }

  public void recordUnavailableReplicaStreamingRequest() {
    unavailableReplicaStreamingRequestSensor.record();
  }

  /**
   * Record read quota usage based on healthy KPS.
   * @param quotaUsage
   */
  public void recordReadQuotaUsage(int quotaUsage) {
    readQuotaUsageSensor.record(quotaUsage);
  }

  public void recordTardyRequest(double latency, HttpResponseStatus responseStatus) {
    tardySensor.record();
    recordRequestSensorOtel(responseStatus, VeniceResponseStatusCategory.TARDY);
    tardyRequestLatencySensor.record(latency);
    recordLatencySensorOtel(latency, responseStatus, VeniceResponseStatusCategory.TARDY);
  }

  public void recordThrottledRequest(double latency, HttpResponseStatus responseStatus) {
    recordThrottledRequest(responseStatus);
    throttledRequestLatencySensor.record(latency);
    recordLatencySensorOtel(latency, responseStatus, VeniceResponseStatusCategory.THROTTLED);
  }

  /**
   * Once we stop reporting throttled requests in {@link com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils},
   * and we only report them in {@link com.linkedin.venice.router.api.VeniceResponseAggregator} then we will always have
   * a latency and we'll be able to remove this overload.
   *
   * TODO: Remove this overload after fixing the above.
   */
  public void recordThrottledRequest(HttpResponseStatus responseStatus) {
    throttleSensor.record();
    recordRequestSensorOtel(responseStatus, VeniceResponseStatusCategory.THROTTLED);
  }

  public void recordErrorRetryCount() {
    errorRetryCountSensor.record();
    recordRetryTriggeredSensorOtel(VeniceRequestRetryType.ERROR_RETRY);
  }

  public void recordRetryTriggeredSensorOtel(VeniceRequestRetryType retryType) {
    if (emitOpenTelemetryMetrics) {
      Attributes dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          .put(getDimensionName(VENICE_REQUEST_RETRY_TYPE), retryType.getRetryType())
          .build();
      retryTriggeredSensorOtel.add(1, dimensions);
    }
  }

  public void recordAbortedRetrySensorOtel(VeniceRequestRetryAbortReason abortReason) {
    if (emitOpenTelemetryMetrics) {
      Attributes dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          .put(getDimensionName(VENICE_REQUEST_RETRY_ABORT_REASON), abortReason.getAbortReason())
          .build();
      abortedRetrySensorOtel.add(1, dimensions);
    }
  }

  public void recordBadRequest(HttpResponseStatus responseStatus) {
    badRequestSensor.record();
    recordRequestSensorOtel(responseStatus, VeniceResponseStatusCategory.BAD_REQUEST);
  }

  public void recordBadRequestKeyCount(int keyCount) {
    badRequestKeyCountSensor.record(keyCount);
    if (emitOpenTelemetryMetrics) {
      recordKeyCountSensorOtel(keyCount, VeniceRequestValidationOutcome.INVALID_KEY_COUNT_LIMIT_EXCEEDED);
    }
  }

  public void recordRequestThrottledByRouterCapacity() {
    requestThrottledByRouterCapacitySensor.record();
  }

  public void recordFanoutRequestCount(int count) {
    if (!getRequestType().equals(RequestType.SINGLE_GET)) {
      fanoutRequestCountSensor.record(count);
    }
  }

  public void recordLatency(double latency) {
    latencySensor.record(latency);
  }

  public void recordLatencySensorOtel(
      double latency,
      HttpResponseStatus responseStatus,
      VeniceResponseStatusCategory veniceResponseStatusCategory) {
    if (emitOpenTelemetryMetrics) {
      Attributes dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          // only add HTTP_RESPONSE_STATUS_CODE_CATEGORY to reduce the cardinality for histogram
          .put(
              getDimensionName(HTTP_RESPONSE_STATUS_CODE_CATEGORY),
              getVeniceHttpResponseStatusCodeCategory(responseStatus))
          .put(getDimensionName(VENICE_RESPONSE_STATUS_CODE_CATEGORY), veniceResponseStatusCategory.getCategory())
          .build();
      latencySensorOtel.record(latency, dimensions);
    }
  }

  public void recordRequestSensorOtel(
      HttpResponseStatus responseStatus,
      VeniceResponseStatusCategory veniceResponseStatusCategory) {
    if (emitOpenTelemetryMetrics) {
      Attributes dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          .put(
              getDimensionName(HTTP_RESPONSE_STATUS_CODE_CATEGORY),
              getVeniceHttpResponseStatusCodeCategory(responseStatus))
          .put(getDimensionName(VENICE_RESPONSE_STATUS_CODE_CATEGORY), veniceResponseStatusCategory.getCategory())
          .put(getDimensionName(HTTP_RESPONSE_STATUS_CODE), responseStatus.codeAsText().toString())
          .build();
      requestSensorOtel.add(1, dimensions);
    }
  }

  public void recordResponseWaitingTime(double waitingTime) {
    routerResponseWaitingTimeSensor.record(waitingTime);
  }

  public void recordRequestSize(double requestSize) {
    requestSizeSensor.record(requestSize);
  }

  public void recordCompressedResponseSize(double compressedResponseSize) {
    compressedResponseSizeSensor.record(compressedResponseSize);
  }

  public void recordResponseSize(double responseSize) {
    responseSizeSensor.record(responseSize);
  }

  public void recordDecompressionTime(double decompressionTime) {
    decompressionTimeSensor.record(decompressionTime);
  }

  public void recordQuota(double quota) {
    quotaSensor.record(quota);
  }

  public void recordFindUnhealthyHostRequest() {
    findUnhealthyHostRequestSensor.record();
  }

  public void recordKeyNum(int keyNum) {
    keyNumSensor.record(keyNum);
    if (emitOpenTelemetryMetrics) {
      recordKeyCountSensorOtel(keyNum, VeniceRequestValidationOutcome.VALID);
    }
  }

  public void recordKeyCountSensorOtel(int keyNum, VeniceRequestValidationOutcome outcome) {
    keyNumSensor.record(keyNum);
    if (emitOpenTelemetryMetrics) {
      Attributes dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          .put(getDimensionName(VENICE_REQUEST_VALIDATION_OUTCOME), outcome.getOutcome())
          .build();
      keyCountSensorOtel.record(keyNum, dimensions);
    }
  }

  public void recordRequestUsage(int usage) {
    requestUsageSensor.record(usage);
  }

  public void recordMultiGetFallback(int keyCount) {
    multiGetFallbackSensor.record(keyCount);
  }

  public void recordRequestParsingLatency(double latency) {
    requestParsingLatencySensor.record(latency);
  }

  public void recordRequestRoutingLatency(double latency) {
    requestRoutingLatencySensor.record(latency);
  }

  public void recordUnavailableRequest() {
    unAvailableRequestSensor.record();
  }

  public void recordDelayConstraintAbortedRetryRequest() {
    delayConstraintAbortedRetryRequest.record();
    recordAbortedRetrySensorOtel(VeniceRequestRetryAbortReason.RETRY_ABORTED_BY_DELAY_CONSTRAINT);
  }

  public void recordSlowRouteAbortedRetryRequest() {
    slowRouteAbortedRetryRequest.record();
    recordAbortedRetrySensorOtel(VeniceRequestRetryAbortReason.RETRY_ABORTED_BY_SLOW_ROUTE);
  }

  public void recordRetryRouteLimitAbortedRetryRequest() {
    retryRouteLimitAbortedRetryRequest.record();
    recordAbortedRetrySensorOtel(VeniceRequestRetryAbortReason.RETRY_ABORTED_BY_MAX_RETRY_ROUTE_LIMIT);
  }

  public void recordNoAvailableReplicaAbortedRetryRequest() {
    noAvailableReplicaAbortedRetryRequest.record();
    recordAbortedRetrySensorOtel(VeniceRequestRetryAbortReason.RETRY_ABORTED_BY_NO_AVAILABLE_REPLICA);
  }

  public void recordKeySizeInByte(long keySize) {
    if (keySizeSensor != null) {
      keySizeSensor.record(keySize);
    }
  }

  public void recordResponse() {
    /**
     * We already report into the sensor when the request starts, in {@link #recordIncomingRequest()}, so at response time
     * there is no need to record into the sensor again. We just want to maintain the bookkeeping.
     */
    currentInFlightRequest.decrementAndGet();
    totalInflightRequestSensor.record(-1);
  }

  public void recordAllowedRetryRequest() {
    allowedRetryRequestSensor.record();
    allowedRetryRequestSensorOtel.add(1, commonMetricDimensions);
  }

  public void recordDisallowedRetryRequest() {
    disallowedRetryRequestSensor.record();
    disallowedRetryRequestSensorOtel.add(1, commonMetricDimensions);
  }

  public void recordErrorRetryAttemptTriggeredByPendingRequestCheck() {
    errorRetryAttemptTriggeredByPendingRequestCheckSensor.record();
  }

  public void recordRetryDelay(double delay) {
    retryDelaySensor.record(delay);
    if (emitOpenTelemetryMetrics) {
      retryDelaySensorOtel.record(delay, commonMetricDimensions);
    }
  }

  public void recordMetaStoreShadowRead() {
    metaStoreShadowReadSensor.record();
  }

  @Override
  protected Sensor registerSensor(String sensorName, MeasurableStat... stats) {
    return super.registerSensor(systemStoreName == null ? sensorName : systemStoreName, null, stats);
  }

  static public boolean hasInFlightRequests() {
    Metric metric = localMetricRepo.getMetric("total_inflight_request_count");
    // max return -infinity when there are no samples. validate only against finite value
    return Double.isFinite(metric.value()) ? metric.value() > 0.0 : false;
  }
}
