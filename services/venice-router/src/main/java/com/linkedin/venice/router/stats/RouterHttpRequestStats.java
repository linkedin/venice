package com.linkedin.venice.router.stats;

import static com.linkedin.venice.router.stats.RouterMetricEntity.ABORTED_RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.ALLOWED_RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.CALL_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.CALL_SIZE;
import static com.linkedin.venice.router.stats.RouterMetricEntity.CALL_TIME;
import static com.linkedin.venice.router.stats.RouterMetricEntity.DISALLOWED_RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.KEY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.KEY_SIZE;
import static com.linkedin.venice.router.stats.RouterMetricEntity.RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.RETRY_DELAY;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.transformHttpResponseStatusToHttpResponseStatusEnum;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.DELAY_CONSTRAINT;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.MAX_RETRY_ROUTE_LIMIT;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.NO_AVAILABLE_REPLICA;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.SLOW_ROUTE;
import static java.util.Collections.singletonList;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.VeniceResponseAggregator;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.LambdaStat;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.MessageType;
import com.linkedin.venice.stats.dimensions.RequestRetryAbortReason;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntityState;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.common.Attributes;
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
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> healthyRequestMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> unhealthyRequestMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> tardyRequestMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> throttledRequestMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> badRequestMetric;

  private final Sensor healthyRequestRateSensor;
  private final Sensor tardyRequestRatioSensor;

  /** latency metrics */
  private final Sensor latencyTehutiSensor; // This can be removed while removing tehuti
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> healthyLatencyMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> unhealthyLatencyMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> tardyLatencyMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> throttledLatencyMetric;

  /** retry metrics */
  private final MetricEntityStateOneEnum<RequestRetryType> retryCountMetric;
  private final MetricEntityStateBase allowedRetryCountMetric;
  private final MetricEntityStateBase disallowedRetryCountMetric;
  private final MetricEntityStateBase retryDelayMetric;

  /** retry aborted metrics */
  private final MetricEntityStateOneEnum<RequestRetryAbortReason> delayConstraintAbortedRetryCountMetric;
  private final MetricEntityStateOneEnum<RequestRetryAbortReason> slowRouteAbortedRetryCountMetric;
  private final MetricEntityStateOneEnum<RequestRetryAbortReason> retryRouteLimitAbortedRetryCountMetric;
  private final MetricEntityStateOneEnum<RequestRetryAbortReason> noAvailableReplicaAbortedRetryCountMetric;

  /** key count metrics */
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> keyCountMetric;
  private final Sensor keyNumSensor;
  private final Sensor badRequestKeyCountSensor;

  /** request size metrics */
  private final MetricEntityStateOneEnum<MessageType> requestSizeMetric;

  /** response size metrics */
  private final MetricEntityStateOneEnum<MessageType> responseSizeMetric;

  /** key size metrics */
  private final MetricEntityStateBase keySizeMetric;

  /** OTel metrics yet to be added */
  private final Sensor compressedResponseSizeSensor;
  private final Sensor decompressedResponseSizeSensor;

  private final Sensor requestThrottledByRouterCapacitySensor;
  private final Sensor decompressionTimeSensor;
  private final Sensor routerResponseWaitingTimeSensor;
  private final Sensor fanoutRequestCountSensor;
  private final Sensor quotaSensor;
  private final Sensor findUnhealthyHostRequestSensor;
  // Reflect the real request usage, e.g count each key as an unit of request usage.
  private final Sensor requestUsageSensor;
  private final Sensor requestCallCountSensor;
  private final Sensor requestParsingLatencySensor;
  private final Sensor requestRoutingLatencySensor;
  private final Sensor pipelineLatencySensor;
  private final Sensor scatterLatencySensor;
  private final Sensor queueLatencySensor;
  private final Sensor dispatchLatencySensor;
  private final Sensor unAvailableRequestSensor;
  private final Sensor readQuotaUsageSensor;
  private final Sensor inFlightRequestSensor;
  private final AtomicInteger currentInFlightRequest;
  private final Sensor unavailableReplicaStreamingRequestSensor;
  private final Sensor multiGetFallbackSensor;
  private final Sensor metaStoreShadowReadSensor;

  /** TODO: Need to clarify the usage and add new OTel metrics or add it as a part of existing ones */
  private final Sensor errorRetryAttemptTriggeredByPendingRequestCheckSensor;

  private final String systemStoreName;
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Sensor totalInFlightRequestSensor;
  private final Attributes baseAttributes;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

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

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .isTotalStats(isTotalStats())
            // set all base dimensions for this stats class and build
            .setStoreName(storeName)
            .setClusterName(clusterName)
            .setRequestType(requestType)
            .build();

    this.emitOpenTelemetryMetrics = otelData.emitOpenTelemetryMetrics();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
    this.baseAttributes = otelData.getBaseAttributes();

    this.systemStoreName = VeniceSystemStoreUtils.extractSystemStoreType(storeName);
    Rate requestRate = new OccurrenceRate();
    Rate healthyRequestRate = new OccurrenceRate();
    Rate tardyRequestRate = new OccurrenceRate();

    requestSensor = registerSensor("request", new Count(), requestRate);

    healthyRequestRateSensor =
        registerSensor(new TehutiUtils.SimpleRatioStat(healthyRequestRate, requestRate, "healthy_request_ratio"));
    tardyRequestRatioSensor =
        registerSensor(new TehutiUtils.SimpleRatioStat(tardyRequestRate, requestRate, "tardy_request_ratio"));

    keyNumSensor = RequestType.isSingleGet(requestType) ? null : registerSensor("key_num", new Avg(), new Max(0));

    badRequestKeyCountSensor = registerSensor("bad_request_key_count", new OccurrenceRate(), new Avg(), new Max());

    requestSizeMetric = MetricEntityStateOneEnum.create(
        CALL_SIZE.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.REQUEST_SIZE,
        Arrays.asList(
            new Avg(),
            TehutiUtils.getPercentileStat(
                getName(),
                getFullMetricName(RouterTehutiMetricNameEnum.REQUEST_SIZE.getMetricName()))),
        baseDimensionsMap,
        MessageType.class);

    healthyRequestMetric = MetricEntityStateThreeEnums.create(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.HEALTHY_REQUEST,
        Arrays.asList(new Count(), healthyRequestRate),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    unhealthyRequestMetric = MetricEntityStateThreeEnums.create(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.UNHEALTHY_REQUEST,
        singletonList(new Count()),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    tardyRequestMetric = MetricEntityStateThreeEnums.create(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.TARDY_REQUEST,
        Arrays.asList(new Count(), tardyRequestRate),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    throttledRequestMetric = MetricEntityStateThreeEnums.create(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.THROTTLED_REQUEST,
        singletonList(new Count()),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    badRequestMetric = MetricEntityStateThreeEnums.create(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.BAD_REQUEST,
        singletonList(new Count()),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    latencyTehutiSensor = registerSensorWithDetailedPercentiles("latency", new Avg(), new Max(0));
    healthyLatencyMetric = MetricEntityStateThreeEnums.create(
        CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.HEALTHY_REQUEST_LATENCY,
        Arrays.asList(
            new Avg(),
            new Max(0),
            TehutiUtils.getPercentileStatForNetworkLatency(
                getName(),
                getFullMetricName(RouterTehutiMetricNameEnum.HEALTHY_REQUEST_LATENCY.getMetricName()))),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);
    unhealthyLatencyMetric = MetricEntityStateThreeEnums.create(
        CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.UNHEALTHY_REQUEST_LATENCY,
        Arrays.asList(new Avg(), new Max(0)),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    tardyLatencyMetric = MetricEntityStateThreeEnums.create(
        CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.TARDY_REQUEST_LATENCY,
        Arrays.asList(new Avg(), new Max(0)),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    throttledLatencyMetric = MetricEntityStateThreeEnums.create(
        CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.THROTTLED_REQUEST_LATENCY,
        Arrays.asList(new Avg(), new Max(0)),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    retryCountMetric = MetricEntityStateOneEnum.create(
        RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.ERROR_RETRY,
        singletonList(new Count()),
        baseDimensionsMap,
        RequestRetryType.class);
    allowedRetryCountMetric = MetricEntityStateBase.create(
        ALLOWED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.ALLOWED_RETRY_REQUEST_COUNT,
        singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    disallowedRetryCountMetric = MetricEntityStateBase.create(
        DISALLOWED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.DISALLOWED_RETRY_REQUEST_COUNT,
        singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    retryDelayMetric = MetricEntityStateBase.create(
        RETRY_DELAY.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.RETRY_DELAY,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    delayConstraintAbortedRetryCountMetric = MetricEntityStateOneEnum.create(
        ABORTED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.DELAY_CONSTRAINT_ABORTED_RETRY_REQUEST,
        singletonList(new Count()),
        baseDimensionsMap,
        RequestRetryAbortReason.class);

    slowRouteAbortedRetryCountMetric = MetricEntityStateOneEnum.create(
        ABORTED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.SLOW_ROUTE_ABORTED_RETRY_REQUEST,
        singletonList(new Count()),
        baseDimensionsMap,
        RequestRetryAbortReason.class);

    retryRouteLimitAbortedRetryCountMetric = MetricEntityStateOneEnum.create(
        ABORTED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.RETRY_ROUTE_LIMIT_ABORTED_RETRY_REQUEST,
        singletonList(new Count()),
        baseDimensionsMap,
        RequestRetryAbortReason.class);

    noAvailableReplicaAbortedRetryCountMetric = MetricEntityStateOneEnum.create(
        ABORTED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.NO_AVAILABLE_REPLICA_ABORTED_RETRY_REQUEST,
        singletonList(new Count()),
        baseDimensionsMap,
        RequestRetryAbortReason.class);

    keyCountMetric = MetricEntityStateThreeEnums.create(
        KEY_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    errorRetryAttemptTriggeredByPendingRequestCheckSensor =
        registerSensor("error_retry_attempt_triggered_by_pending_request_check", new OccurrenceRate());

    unavailableReplicaStreamingRequestSensor = !RequestType.isStreaming(requestType)
        ? null
        : registerSensor("unavailable_replica_streaming_request", new Count());
    requestThrottledByRouterCapacitySensor = registerSensor("request_throttled_by_router_capacity", new Count());
    fanoutRequestCountSensor =
        RequestType.isSingleGet(requestType) ? null : registerSensor("fanout_request_count", new Avg(), new Max(0));

    routerResponseWaitingTimeSensor = registerSensor(
        "response_waiting_time",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_waiting_time")));

    compressedResponseSizeSensor = registerSensor(
        "compressed_response_size",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("compressed_response_size")),
        new Avg(),
        new Max());

    decompressedResponseSizeSensor = registerSensor(
        "decompressed_response_size",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("decompressed_response_size")),
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
    requestUsageSensor = RequestType.isSingleGet(requestType)
        ? registerSensor("request_usage", new Total(), new OccurrenceRate())
        : null;

    /**
     * A count version of this sensor is needed, as an internal system depends on the sensor to be
     * of type Count to measure QPS.
     */
    requestCallCountSensor =
        RequestType.isSingleGet(requestType) ? registerSensor("request_call_count", new Count()) : null;

    multiGetFallbackSensor = !RequestType.isCompute(requestType)
        ? null
        : registerSensor("multiget_fallback", new Total(), new OccurrenceRate());

    requestParsingLatencySensor = registerSensor("request_parse_latency", new Avg());
    requestRoutingLatencySensor = registerSensor("request_route_latency", new Avg());

    pipelineLatencySensor = registerSensor(
        "pipeline_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("pipeline_latency")));
    scatterLatencySensor = registerSensor(
        "scatter_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("scatter_latency")));
    queueLatencySensor =
        registerSensor("queue_latency", TehutiUtils.getPercentileStat(getName(), getFullMetricName("queue_latency")));
    dispatchLatencySensor = registerSensor(
        "dispatch_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("dispatch_latency")));

    unAvailableRequestSensor = registerSensor("unavailable_request", new Count());

    readQuotaUsageSensor =
        RequestType.isSingleGet(requestType) ? registerSensor("read_quota_usage_kps", new Total()) : null;

    inFlightRequestSensor = registerSensor("in_flight_request_count", new Min(), new Max(0), new Avg());

    if (isKeyValueProfilingEnabled) {
      // 1. emit otel metric if the store is not total: otelRepository will be null for total
      // 2. emit tehuti metrics if the store is total (keeping existing behavior)
      keySizeMetric = MetricEntityStateBase.create(
          KEY_SIZE.getMetricEntity(),
          otelRepository,
          this::registerSensorFinal,
          RouterTehutiMetricNameEnum.KEY_SIZE_IN_BYTE,
          !isTotalStats()
              ? null
              : Arrays.asList(
                  new Avg(),
                  new Max(),
                  TehutiUtils.getPercentileStat(
                      getName(),
                      getFullMetricName(RouterTehutiMetricNameEnum.KEY_SIZE_IN_BYTE.getMetricName()))),
          baseDimensionsMap,
          baseAttributes);
    } else {
      keySizeMetric = null;
    }
    responseSizeMetric = MetricEntityStateOneEnum.create(
        CALL_SIZE.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        RouterTehutiMetricNameEnum.RESPONSE_SIZE,
        Arrays.asList(
            new Avg(),
            new Max(),
            TehutiUtils.getPercentileStat(
                getName(),
                getFullMetricName(RouterTehutiMetricNameEnum.RESPONSE_SIZE.getMetricName()))),
        baseDimensionsMap,
        MessageType.class);

    // Initialize the in-flight request counter
    currentInFlightRequest = new AtomicInteger();

    metaStoreShadowReadSensor = registerSensor("meta_store_shadow_read", new OccurrenceRate());
    this.totalInFlightRequestSensor = totalInFlightRequestSensor;
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

  public void recordHealthyRequest(double latency, HttpResponseStatus responseStatus, int keyNum) {
    recordRequestMetrics(
        keyNum,
        latency,
        responseStatus,
        VeniceResponseStatusCategory.SUCCESS,
        healthyRequestMetric,
        healthyLatencyMetric);
  }

  public void recordUnhealthyRequest(HttpResponseStatus responseStatus) {
    unhealthyRequestMetric.record(
        1,
        transformHttpResponseStatusToHttpResponseStatusEnum(responseStatus),
        getVeniceHttpResponseStatusCodeCategory(responseStatus),
        VeniceResponseStatusCategory.FAIL);
  }

  public void recordUnhealthyRequest(double latency, HttpResponseStatus responseStatus, int keyNum) {
    recordRequestMetrics(
        keyNum,
        latency,
        responseStatus,
        VeniceResponseStatusCategory.FAIL,
        unhealthyRequestMetric,
        unhealthyLatencyMetric);
  }

  public void recordRequestSize(double requestSize) {
    requestSizeMetric.record(requestSize, MessageType.REQUEST);
  }

  public void recordResponseSize(double responseSize) {
    responseSizeMetric.record(responseSize, MessageType.RESPONSE);
  }

  public void recordKeySizeInByte(long keySize) {
    if (keySizeMetric != null) {
      keySizeMetric.record(keySize);
    }
  }

  private void recordRequestMetrics(
      int keyNum,
      double latency,
      HttpResponseStatus responseStatus,
      VeniceResponseStatusCategory veniceResponseStatusCategory,
      MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> requestMetric,
      MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> latencyMetric) {
    HttpResponseStatusEnum httpResponseStatusEnum = transformHttpResponseStatusToHttpResponseStatusEnum(responseStatus);
    HttpResponseStatusCodeCategory httpResponseStatusCodeCategory =
        getVeniceHttpResponseStatusCodeCategory(responseStatus);
    requestMetric.record(1, httpResponseStatusEnum, httpResponseStatusCodeCategory, veniceResponseStatusCategory);
    keyCountMetric.record(keyNum, httpResponseStatusEnum, httpResponseStatusCodeCategory, veniceResponseStatusCategory);
    latencyMetric.record(latency, httpResponseStatusEnum, httpResponseStatusCodeCategory, veniceResponseStatusCategory);
  }

  public void recordUnavailableReplicaStreamingRequest() {
    if (unavailableReplicaStreamingRequestSensor != null) {
      unavailableReplicaStreamingRequestSensor.record();
    }
  }

  /**
   * Record read quota usage based on healthy KPS.
   * @param quotaUsage
   */
  public void recordReadQuotaUsage(int quotaUsage) {
    if (readQuotaUsageSensor != null) {
      readQuotaUsageSensor.record(quotaUsage);
    }
  }

  public void recordTardyRequest(double latency, HttpResponseStatus responseStatus, int keyNum) {
    recordRequestMetrics(
        keyNum,
        latency,
        responseStatus,
        VeniceResponseStatusCategory.SUCCESS,
        tardyRequestMetric,
        tardyLatencyMetric);
  }

  public void recordThrottledRequest(double latency, HttpResponseStatus responseStatus, int keyNum) {
    recordRequestMetrics(
        keyNum,
        latency,
        responseStatus,
        VeniceResponseStatusCategory.FAIL,
        throttledRequestMetric,
        throttledLatencyMetric);
  }

  /**
   * Once we stop reporting throttled requests in {@link RouterExceptionAndTrackingUtils},
   * and we only report them in {@link VeniceResponseAggregator} then we will always have
   * a latency and we'll be able to remove this overload.
   *
   * TODO: Remove this overload after fixing the above.
   */
  public void recordThrottledRequest(HttpResponseStatus responseStatus) {
    throttledRequestMetric.record(
        1,
        transformHttpResponseStatusToHttpResponseStatusEnum(responseStatus),
        getVeniceHttpResponseStatusCodeCategory(responseStatus),
        VeniceResponseStatusCategory.FAIL);
  }

  public void recordErrorRetryCount() {
    recordRetryTriggeredSensorOtel(RequestRetryType.ERROR_RETRY);
  }

  public void recordRetryTriggeredSensorOtel(RequestRetryType retryType) {
    retryCountMetric.record(1, retryType);
  }

  public void recordDelayConstraintAbortedRetryCountMetric() {
    delayConstraintAbortedRetryCountMetric.record(1, DELAY_CONSTRAINT);
  }

  public void recordSlowRouteAbortedRetryCountMetric() {
    slowRouteAbortedRetryCountMetric.record(1, SLOW_ROUTE);
  }

  public void recordRetryRouteLimitAbortedRetryCountMetric() {
    retryRouteLimitAbortedRetryCountMetric.record(1, MAX_RETRY_ROUTE_LIMIT);
  }

  public void recordNoAvailableReplicaAbortedRetryCountMetric() {
    noAvailableReplicaAbortedRetryCountMetric.record(1, NO_AVAILABLE_REPLICA);
  }

  public void recordBadRequest(HttpResponseStatus responseStatus) {
    badRequestMetric.record(
        1,
        transformHttpResponseStatusToHttpResponseStatusEnum(responseStatus),
        getVeniceHttpResponseStatusCodeCategory(responseStatus),
        VeniceResponseStatusCategory.FAIL);
  }

  public void recordRequestThrottledByRouterCapacity() {
    requestThrottledByRouterCapacitySensor.record();
  }

  public void recordFanoutRequestCount(int count) {
    if (fanoutRequestCountSensor != null) {
      fanoutRequestCountSensor.record(count);
    }
  }

  public void recordLatency(double latency) {
    latencyTehutiSensor.record(latency);
  }

  public void recordResponseWaitingTime(double waitingTime) {
    routerResponseWaitingTimeSensor.record(waitingTime);
  }

  public void recordCompressedResponseSize(double compressedResponseSize) {
    compressedResponseSizeSensor.record(compressedResponseSize);
  }

  public void recordDecompressedResponseSize(double decompressedResponseSize) {
    decompressedResponseSizeSensor.record(decompressedResponseSize);
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
    if (keyNumSensor != null) {
      keyNumSensor.record(keyNum);
    }
  }

  public void recordIncomingBadRequestKeyCountMetric(HttpResponseStatus responseStatus, int keyNum) {
    badRequestKeyCountSensor.record(keyNum);
    keyCountMetric.record(
        keyNum,
        transformHttpResponseStatusToHttpResponseStatusEnum(responseStatus),
        getVeniceHttpResponseStatusCodeCategory(responseStatus),
        VeniceResponseStatusCategory.FAIL);
  }

  public void recordRequestUsage(int usage) {
    if (requestUsageSensor != null) {
      requestUsageSensor.record(usage);
    }
    if (requestCallCountSensor != null) {
      requestCallCountSensor.record();
    }
  }

  public void recordMultiGetFallback(int keyCount) {
    if (multiGetFallbackSensor != null) {
      multiGetFallbackSensor.record(keyCount);
    }
  }

  public void recordRequestParsingLatency(double latency) {
    requestParsingLatencySensor.record(latency);
  }

  public void recordRequestRoutingLatency(double latency) {
    requestRoutingLatencySensor.record(latency);
  }

  public void recordPipelineLatency(double latency) {
    pipelineLatencySensor.record(latency);
  }

  public void recordScatterLatency(double latency) {
    scatterLatencySensor.record(latency);
  }

  public void recordQueueLatency(double latency) {
    queueLatencySensor.record(latency);
  }

  public void recordDispatchLatency(double latency) {
    dispatchLatencySensor.record(latency);
  }

  public void recordUnavailableRequest() {
    unAvailableRequestSensor.record();
  }

  public void recordResponse() {
    /**
     * We already report into the sensor when the request starts, in {@link #recordIncomingRequest()}, so at response time
     * there is no need to record into the sensor again. We just want to maintain the bookkeeping.
     */
    currentInFlightRequest.decrementAndGet();
  }

  public void recordAllowedRetryRequest() {
    allowedRetryCountMetric.record(1);
  }

  public void recordDisallowedRetryRequest() {
    disallowedRetryCountMetric.record(1);
  }

  public void recordErrorRetryAttemptTriggeredByPendingRequestCheck() {
    errorRetryAttemptTriggeredByPendingRequestCheckSensor.record();
  }

  public void recordRetryDelay(double delay) {
    retryDelayMetric.record(delay);
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

  /** visible for testing */
  boolean emitOpenTelemetryMetrics() {
    return this.emitOpenTelemetryMetrics;
  }

  /** visible for testing */
  VeniceOpenTelemetryMetricsRepository getOtelRepository() {
    return this.otelRepository;
  }

  /** visible for testing */
  Attributes getBaseAttributes() {
    return this.baseAttributes;
  }

  /** visible for testing */
  Map<VeniceMetricsDimensions, String> getBaseDimensionsMap() {
    return this.baseDimensionsMap;
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
    /** for {@link RouterMetricEntity#CALL_SIZE} */
    REQUEST_SIZE, RESPONSE_SIZE,
    /** for {@link RouterMetricEntity#KEY_SIZE} */
    KEY_SIZE_IN_BYTE,
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
