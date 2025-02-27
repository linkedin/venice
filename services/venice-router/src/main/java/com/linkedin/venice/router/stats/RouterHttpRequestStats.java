package com.linkedin.venice.router.stats;

import static com.linkedin.venice.router.stats.RouterMetricEntity.ABORTED_RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.ALLOWED_RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.CALL_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.CALL_TIME;
import static com.linkedin.venice.router.stats.RouterMetricEntity.DISALLOWED_RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.KEY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.RETRY_DELAY;
import static com.linkedin.venice.stats.AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.DELAY_CONSTRAINT;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.MAX_RETRY_ROUTE_LIMIT;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.NO_AVAILABLE_REPLICA;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.SLOW_ROUTE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static java.util.Collections.singletonList;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.VeniceResponseAggregator;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.LambdaStat;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.RequestRetryAbortReason;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntityState;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.Total;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class RouterHttpRequestStats extends AbstractVeniceHttpStats {
  /** metrics to track incoming requests */
  private final Sensor requestSensor;

  /** metrics to track response handling */
  private final MetricEntityState healthyRequestMetric;
  private final MetricEntityState unhealthyRequestMetric;
  private final MetricEntityState tardyRequestMetric;
  private final MetricEntityState throttledRequestMetric;
  private final MetricEntityState badRequestMetric;

  private final Sensor healthyRequestRateSensor;
  private final Sensor tardyRequestRatioSensor;

  /** latency metrics */
  private final Sensor latencyTehutiSensor; // This can be removed while removing tehuti
  private final MetricEntityState healthyLatencyMetric;
  private final MetricEntityState unhealthyLatencyMetric;
  private final MetricEntityState tardyLatencyMetric;
  private final MetricEntityState throttledLatencyMetric;

  /** retry metrics */
  private final MetricEntityState retryCountMetric;
  private final MetricEntityState allowedRetryCountMetric;
  private final MetricEntityState disallowedRetryCountMetric;
  private final MetricEntityState retryDelayMetric;

  /** retry aborted metrics */
  private final MetricEntityState delayConstraintAbortedRetryCountMetric;
  private final MetricEntityState slowRouteAbortedRetryCountMetric;
  private final MetricEntityState retryRouteLimitAbortedRetryCountMetric;
  private final MetricEntityState noAvailableReplicaAbortedRetryCountMetric;

  /** key count metrics */
  private final MetricEntityState keyCountMetric;
  private final Sensor keyNumSensor;
  private final Sensor badRequestKeyCountSensor;

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
  private final Sensor totalInFlightRequestSensor;
  private static VeniceMetricsRepository inflightMetricRepo = null;

  // QPS metrics
  public RouterHttpRequestStats(
      MetricsRepository metricsRepository,
      String storeName,
      String clusterName,
      RequestType requestType,
      ScatterGatherStats scatterGatherStats,
      boolean isKeyValueProfilingEnabled,
      Sensor totalInFlightRequestSensor) {
    super(metricsRepository, storeName, requestType);
    VeniceOpenTelemetryMetricsRepository otelRepository = null;
    if (metricsRepository instanceof VeniceMetricsRepository) {
      VeniceMetricsRepository veniceMetricsRepository = (VeniceMetricsRepository) metricsRepository;
      VeniceMetricsConfig veniceMetricsConfig = veniceMetricsRepository.getVeniceMetricsConfig();
      // total stats won't be emitted by OTel
      emitOpenTelemetryMetrics = veniceMetricsConfig.emitOtelMetrics() && !isTotalStats();
      openTelemetryMetricFormat = veniceMetricsConfig.getMetricNamingFormat();
      if (emitOpenTelemetryMetrics) {
        otelRepository = veniceMetricsRepository.getOpenTelemetryMetricsRepository();
        AttributesBuilder attributesBuilder = Attributes.builder()
            .put(getDimensionName(VENICE_STORE_NAME), storeName)
            .put(getDimensionName(VENICE_REQUEST_METHOD), requestType.name().toLowerCase())
            .put(getDimensionName(VENICE_CLUSTER_NAME), clusterName);
        // add custom dimensions passed in by the user
        for (Map.Entry<String, String> entry: veniceMetricsConfig.getOtelCustomDimensionsMap().entrySet()) {
          attributesBuilder.put(entry.getKey(), entry.getValue());
        }
        commonMetricDimensions = attributesBuilder.build();
      } else {
        commonMetricDimensions = null;
      }
    } else {
      emitOpenTelemetryMetrics = false;
      openTelemetryMetricFormat = VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat();
      commonMetricDimensions = null;
    }

    this.systemStoreName = VeniceSystemStoreUtils.extractSystemStoreType(storeName);
    Rate requestRate = new OccurrenceRate();
    Rate healthyRequestRate = new OccurrenceRate();
    Rate tardyRequestRate = new OccurrenceRate();
    requestSensor = registerSensor("request", new Count(), requestRate);

    healthyRequestRateSensor =
        registerSensor(new TehutiUtils.SimpleRatioStat(healthyRequestRate, requestRate, "healthy_request_ratio"));
    tardyRequestRatioSensor =
        registerSensor(new TehutiUtils.SimpleRatioStat(tardyRequestRate, requestRate, "tardy_request_ratio"));
    keyNumSensor = registerSensor("key_num", new Avg(), new Max(0));
    badRequestKeyCountSensor = registerSensor("bad_request_key_count", new OccurrenceRate(), new Avg(), new Max());

    healthyRequestMetric = new MetricEntityState(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.HEALTHY_REQUEST,
        Arrays.asList(new Count(), healthyRequestRate));

    unhealthyRequestMetric = new MetricEntityState(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.UNHEALTHY_REQUEST,
        singletonList(new Count()));

    tardyRequestMetric = new MetricEntityState(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.TARDY_REQUEST,
        Arrays.asList(new Count(), tardyRequestRate));

    throttledRequestMetric = new MetricEntityState(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.THROTTLED_REQUEST,
        singletonList(new Count()));

    badRequestMetric = new MetricEntityState(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.BAD_REQUEST,
        singletonList(new Count()));

    latencyTehutiSensor = registerSensorWithDetailedPercentiles("latency", new Avg(), new Max(0));
    healthyLatencyMetric = new MetricEntityState(
        CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.HEALTHY_REQUEST_LATENCY,
        Arrays.asList(
            new Avg(),
            new Max(0),
            TehutiUtils.getPercentileStatForNetworkLatency(
                getName(),
                getFullMetricName(RouterTehutiMetricNameEnum.HEALTHY_REQUEST_LATENCY.getMetricName()))));

    unhealthyLatencyMetric = new MetricEntityState(
        CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.UNHEALTHY_REQUEST_LATENCY,
        Arrays.asList(new Avg(), new Max(0)));

    tardyLatencyMetric = new MetricEntityState(
        CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.TARDY_REQUEST_LATENCY,
        Arrays.asList(new Avg(), new Max(0)));

    throttledLatencyMetric = new MetricEntityState(
        CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.THROTTLED_REQUEST_LATENCY,
        Arrays.asList(new Avg(), new Max(0)));

    retryCountMetric = new MetricEntityState(
        RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.ERROR_RETRY,
        singletonList(new Count()));

    allowedRetryCountMetric = new MetricEntityState(
        ALLOWED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.ALLOWED_RETRY_REQUEST_COUNT,
        singletonList(new OccurrenceRate()));

    disallowedRetryCountMetric = new MetricEntityState(
        DISALLOWED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.DISALLOWED_RETRY_REQUEST_COUNT,
        singletonList(new OccurrenceRate()));

    retryDelayMetric = new MetricEntityState(
        RETRY_DELAY.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.RETRY_DELAY,
        Arrays.asList(new Avg(), new Max()));

    delayConstraintAbortedRetryCountMetric = new MetricEntityState(
        ABORTED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.DELAY_CONSTRAINT_ABORTED_RETRY_REQUEST,
        singletonList(new Count()));

    slowRouteAbortedRetryCountMetric = new MetricEntityState(
        ABORTED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.SLOW_ROUTE_ABORTED_RETRY_REQUEST,
        singletonList(new Count()));

    retryRouteLimitAbortedRetryCountMetric = new MetricEntityState(
        ABORTED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.RETRY_ROUTE_LIMIT_ABORTED_RETRY_REQUEST,
        singletonList(new Count()));

    noAvailableReplicaAbortedRetryCountMetric = new MetricEntityState(
        ABORTED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.NO_AVAILABLE_REPLICA_ABORTED_RETRY_REQUEST,
        singletonList(new Count()));

    keyCountMetric = new MetricEntityState(KEY_COUNT.getMetricEntity(), otelRepository);

    errorRetryAttemptTriggeredByPendingRequestCheckSensor =
        registerSensor("error_retry_attempt_triggered_by_pending_request_check", new OccurrenceRate());

    unavailableReplicaStreamingRequestSensor = registerSensor("unavailable_replica_streaming_request", new Count());
    requestThrottledByRouterCapacitySensor = registerSensor("request_throttled_by_router_capacity", new Count());
    fanoutRequestCountSensor = registerSensor("fanout_request_count", new Avg(), new Max(0));

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
    this.totalInFlightRequestSensor = totalInFlightRequestSensor;
  }

  private String getDimensionName(VeniceMetricsDimensions dimension) {
    return dimension.getDimensionName(openTelemetryMetricFormat);
  }

  /**
   * We record this at the beginning of request handling, so we don't know the latency yet... All specific
   * types of requests also have their latencies logged at the same time.
   */
  public void recordIncomingRequest() {
    requestSensor.record(1);
    inFlightRequestSensor.record(currentInFlightRequest.incrementAndGet());
    totalInFlightRequestSensor.record();
  }

  Attributes getRequestMetricDimensions(
      HttpResponseStatus responseStatus,
      VeniceResponseStatusCategory veniceResponseStatusCategory) {
    return Attributes.builder()
        .putAll(commonMetricDimensions)
        .put(getDimensionName(HTTP_RESPONSE_STATUS_CODE), responseStatus.codeAsText().toString())
        .put(
            getDimensionName(HTTP_RESPONSE_STATUS_CODE_CATEGORY),
            getVeniceHttpResponseStatusCodeCategory(responseStatus))
        .put(getDimensionName(VENICE_RESPONSE_STATUS_CODE_CATEGORY), veniceResponseStatusCategory.getCategory())
        .build();
  }

  public void recordHealthyRequest(Double latency, HttpResponseStatus responseStatus, int keyNum) {
    Attributes dimensions = getRequestMetricDimensions(responseStatus, VeniceResponseStatusCategory.SUCCESS);
    healthyRequestMetric.record(1, dimensions);
    keyCountMetric.record(keyNum, dimensions);
    if (latency != null) {
      healthyLatencyMetric.record(latency, dimensions);
    }
  }

  public void recordUnhealthyRequest(HttpResponseStatus responseStatus) {
    Attributes dimensions = getRequestMetricDimensions(responseStatus, VeniceResponseStatusCategory.FAIL);
    unhealthyRequestMetric.record(1, dimensions);
  }

  public void recordUnhealthyRequest(double latency, HttpResponseStatus responseStatus, int keyNum) {
    Attributes dimensions = getRequestMetricDimensions(responseStatus, VeniceResponseStatusCategory.FAIL);
    unhealthyRequestMetric.record(1, dimensions);
    keyCountMetric.record(keyNum, dimensions);
    unhealthyLatencyMetric.record(latency, dimensions);
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

  public void recordTardyRequest(double latency, HttpResponseStatus responseStatus, int keyNum) {
    Attributes dimensions = getRequestMetricDimensions(responseStatus, VeniceResponseStatusCategory.SUCCESS);
    tardyRequestMetric.record(1, dimensions);
    keyCountMetric.record(keyNum, dimensions);
    tardyLatencyMetric.record(latency, dimensions);
  }

  public void recordThrottledRequest(double latency, HttpResponseStatus responseStatus, int keyNum) {
    Attributes dimensions = getRequestMetricDimensions(responseStatus, VeniceResponseStatusCategory.FAIL);
    throttledRequestMetric.record(1, dimensions);
    keyCountMetric.record(keyNum, dimensions);
    throttledLatencyMetric.record(latency, dimensions);
  }

  /**
   * Once we stop reporting throttled requests in {@link RouterExceptionAndTrackingUtils},
   * and we only report them in {@link VeniceResponseAggregator} then we will always have
   * a latency and we'll be able to remove this overload.
   *
   * TODO: Remove this overload after fixing the above.
   */
  public void recordThrottledRequest(HttpResponseStatus responseStatus) {
    Attributes dimensions = getRequestMetricDimensions(responseStatus, VeniceResponseStatusCategory.FAIL);
    throttledRequestMetric.record(1, dimensions);
  }

  public void recordErrorRetryCount() {
    recordRetryTriggeredSensorOtel(RequestRetryType.ERROR_RETRY);
  }

  public void recordRetryTriggeredSensorOtel(RequestRetryType retryType) {
    Attributes dimensions = null;
    if (emitOpenTelemetryMetrics) {
      dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          .put(getDimensionName(VENICE_REQUEST_RETRY_TYPE), retryType.getRetryType())
          .build();
    }
    retryCountMetric.record(1, dimensions);
  }

  private Attributes getRetryRequestAbortDimensions(RequestRetryAbortReason abortReason) {
    Attributes dimensions = null;
    if (emitOpenTelemetryMetrics) {
      dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          .put(getDimensionName(VENICE_REQUEST_RETRY_ABORT_REASON), abortReason.getAbortReason())
          .build();
    }
    return dimensions;
  }

  public void recordDelayConstraintAbortedRetryCountMetric() {
    Attributes dimensions = getRetryRequestAbortDimensions(DELAY_CONSTRAINT);
    delayConstraintAbortedRetryCountMetric.record(1, dimensions);
  }

  public void recordSlowRouteAbortedRetryCountMetric() {
    Attributes dimensions = getRetryRequestAbortDimensions(SLOW_ROUTE);
    slowRouteAbortedRetryCountMetric.record(1, dimensions);
  }

  public void recordRetryRouteLimitAbortedRetryCountMetric() {
    Attributes dimensions = getRetryRequestAbortDimensions(MAX_RETRY_ROUTE_LIMIT);
    retryRouteLimitAbortedRetryCountMetric.record(1, dimensions);
  }

  public void recordNoAvailableReplicaAbortedRetryCountMetric() {
    Attributes dimensions = getRetryRequestAbortDimensions(NO_AVAILABLE_REPLICA);
    noAvailableReplicaAbortedRetryCountMetric.record(1, dimensions);
  }

  public void recordBadRequest(HttpResponseStatus responseStatus) {
    Attributes dimensions = getRequestMetricDimensions(responseStatus, VeniceResponseStatusCategory.FAIL);
    badRequestMetric.record(1, dimensions);
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
    latencyTehutiSensor.record(latency);
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

  public void recordIncomingKeyCountMetric(int keyNum) {
    keyNumSensor.record(keyNum);
  }

  public void recordIncomingBadRequestKeyCountMetric(HttpResponseStatus responseStatus, int keyNum) {
    badRequestKeyCountSensor.record(keyNum);
    keyCountMetric.record(keyNum, getRequestMetricDimensions(responseStatus, VeniceResponseStatusCategory.FAIL));
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
  }

  public void recordAllowedRetryRequest() {
    allowedRetryCountMetric.record(1, commonMetricDimensions);
  }

  public void recordDisallowedRetryRequest() {
    disallowedRetryCountMetric.record(1, commonMetricDimensions);
  }

  public void recordErrorRetryAttemptTriggeredByPendingRequestCheck() {
    errorRetryAttemptTriggeredByPendingRequestCheckSensor.record();
  }

  public void recordRetryDelay(double delay) {
    retryDelayMetric.record(delay, commonMetricDimensions);
  }

  public void recordMetaStoreShadowRead() {
    metaStoreShadowReadSensor.record();
  }

  @Override
  protected Sensor registerSensor(String sensorName, MeasurableStat... stats) {
    return super.registerSensor(systemStoreName == null ? sensorName : systemStoreName, null, stats);
  }

  /**
   * This method will be passed to the constructor of {@link MetricEntityState} to register tehuti sensor.
   * Only private/static/final methods can be passed onto the constructor.
   */
  private Sensor registerSensorFinal(String sensorName, MeasurableStat... stats) {
    return this.registerSensor(sensorName, stats);
  }

  /** used only for testing */
  boolean emitOpenTelemetryMetrics() {
    return this.emitOpenTelemetryMetrics;
  }

  /** used only for testing */
  VeniceOpenTelemetryMetricNamingFormat getOpenTelemetryMetricsFormat() {
    return this.openTelemetryMetricFormat;
  }

  /** used only for testing */
  Attributes getCommonMetricDimensions() {
    return this.commonMetricDimensions;
  }

  /**
   * Metric names for tehuti metrics used in this class
   */
  enum RouterTehutiMetricNameEnum implements TehutiMetricNameEnum {
    /** for {@link RouterMetricEntity#CALL_COUNT} */
    HEALTHY_REQUEST, UNHEALTHY_REQUEST, TARDY_REQUEST, THROTTLED_REQUEST, BAD_REQUEST,
    /** for {@link RouterMetricEntity#CALL_TIME} */
    HEALTHY_REQUEST_LATENCY, UNHEALTHY_REQUEST_LATENCY, TARDY_REQUEST_LATENCY, THROTTLED_REQUEST_LATENCY,
    /** for {@link RouterMetricEntity#RETRY_COUNT} */
    ERROR_RETRY,
    /** for {@link RouterMetricEntity#ALLOWED_RETRY_COUNT} */
    ALLOWED_RETRY_REQUEST_COUNT,
    /** for {@link RouterMetricEntity#DISALLOWED_RETRY_COUNT} */
    DISALLOWED_RETRY_REQUEST_COUNT,
    /** for {@link RouterMetricEntity#RETRY_DELAY} */
    RETRY_DELAY,
    /** for {@link RouterMetricEntity#ABORTED_RETRY_COUNT} */
    DELAY_CONSTRAINT_ABORTED_RETRY_REQUEST, SLOW_ROUTE_ABORTED_RETRY_REQUEST, RETRY_ROUTE_LIMIT_ABORTED_RETRY_REQUEST,
    NO_AVAILABLE_REPLICA_ABORTED_RETRY_REQUEST;

    private final String metricName;

    RouterTehutiMetricNameEnum() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }
}
