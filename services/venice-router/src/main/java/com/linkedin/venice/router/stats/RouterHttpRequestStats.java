package com.linkedin.venice.router.stats;

import static com.linkedin.venice.router.RouterServer.ROUTER_SERVICE_METRIC_PREFIX;
import static com.linkedin.venice.router.RouterServer.ROUTER_SERVICE_NAME;
import static com.linkedin.venice.router.stats.RouterMetricEntity.ABORTED_RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.ALLOWED_RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.CALL_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.CALL_KEY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.CALL_TIME;
import static com.linkedin.venice.router.stats.RouterMetricEntity.DISALLOWED_RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.INCOMING_CALL_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.RETRY_COUNT;
import static com.linkedin.venice.router.stats.RouterMetricEntity.RETRY_DELAY;
import static com.linkedin.venice.stats.AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
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
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.RequestRetryAbortReason;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.RequestValidationOutcome;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntityState;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.tehuti.Metric;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricConfig;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class RouterHttpRequestStats extends AbstractVeniceHttpStats {
  private static final MetricConfig METRIC_CONFIG = new MetricConfig().timeWindow(10, TimeUnit.SECONDS);
  private static final VeniceMetricsRepository localMetricRepo = new VeniceMetricsRepository(
      new VeniceMetricsConfig.Builder().setServiceName(ROUTER_SERVICE_NAME)
          .setMetricPrefix(ROUTER_SERVICE_METRIC_PREFIX)
          .setTehutiMetricConfig(METRIC_CONFIG)
          .build());

  private final static Sensor totalInflightRequestSensor = localMetricRepo.sensor("total_inflight_request");
  static {
    totalInflightRequestSensor.add("total_inflight_request_count", new Rate());
  }

  /** metrics to track incoming requests */
  private final MetricEntityState incomingRequestMetric;

  /** metrics to track response handling */
  private final MetricEntityState requestMetric;
  private final Sensor healthyRequestRateSensor;
  private final Sensor tardyRequestRatioSensor;

  /** latency metrics */
  private final Sensor latencyTehutiSensor; // This can be removed while removing tehuti
  private final MetricEntityState latencyMetric;

  /** retry metrics */
  private final MetricEntityState retryCountMetric;
  private final MetricEntityState allowedRetryCountMetric;
  private final MetricEntityState disallowedRetryCountMetric;
  private final MetricEntityState retryDelayMetric;

  /** retry aborted metrics */
  private final MetricEntityState abortedRetryCountMetric;

  /** key count metrics */
  private final MetricEntityState keyCountMetric;

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
      MetricsRepository metricsRepository,
      String storeName,
      String clusterName,
      RequestType requestType,
      ScatterGatherStats scatterGatherStats,
      boolean isKeyValueProfilingEnabled) {
    super(metricsRepository, storeName, requestType);
    VeniceOpenTelemetryMetricsRepository otelRepository;
    if (metricsRepository instanceof VeniceMetricsRepository) {
      VeniceMetricsRepository veniceMetricsRepository = (VeniceMetricsRepository) metricsRepository;
      VeniceMetricsConfig veniceMetricsConfig = veniceMetricsRepository.getVeniceMetricsConfig();
      emitOpenTelemetryMetrics = veniceMetricsConfig.emitOtelMetrics();
      openTelemetryMetricFormat = veniceMetricsConfig.getMetricNamingFormat();
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
      emitOpenTelemetryMetrics = false;
      openTelemetryMetricFormat = VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE;
      commonMetricDimensions = null;
      otelRepository = null;
    }

    this.systemStoreName = VeniceSystemStoreUtils.extractSystemStoreType(storeName);
    Rate requestRate = new OccurrenceRate();
    Rate healthyRequestRate = new OccurrenceRate();
    Rate tardyRequestRate = new OccurrenceRate();

    incomingRequestMetric = new MetricEntityState(
        INCOMING_CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        new HashMap<String, List<MeasurableStat>>() {
          {
            put(MetricNamesInTehuti.INCOMING_REQUEST, Arrays.asList(new Count(), requestRate));
          }
        });

    healthyRequestRateSensor =
        registerSensor(new TehutiUtils.SimpleRatioStat(healthyRequestRate, requestRate, "healthy_request_ratio"));
    tardyRequestRatioSensor =
        registerSensor(new TehutiUtils.SimpleRatioStat(tardyRequestRate, requestRate, "tardy_request_ratio"));
    requestMetric = new MetricEntityState(
        CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        new HashMap<String, List<MeasurableStat>>() {
          {
            put(MetricNamesInTehuti.HEALTHY_REQUEST, Arrays.asList(new Count(), healthyRequestRate));
            put(MetricNamesInTehuti.UNHEALTHY_REQUEST, Collections.singletonList(new Count()));
            put(MetricNamesInTehuti.TARDY_REQUEST, Arrays.asList(new Count(), tardyRequestRate));
            put(MetricNamesInTehuti.THROTTLED_REQUEST, Collections.singletonList(new Count()));
            put(MetricNamesInTehuti.BAD_REQUEST, Collections.singletonList(new Count()));
          }
        });

    latencyTehutiSensor = registerSensorWithDetailedPercentiles("latency", new Avg(), new Max(0));
    latencyMetric = new MetricEntityState(
        CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        new HashMap<String, List<MeasurableStat>>() {
          {
            put(
                MetricNamesInTehuti.HEALTHY_REQUEST_LATENCY,
                Arrays.asList(
                    new Avg(),
                    new Max(0),
                    TehutiUtils.getPercentileStatForNetworkLatency(
                        getName(),
                        getFullMetricName(MetricNamesInTehuti.HEALTHY_REQUEST_LATENCY))));
            put(MetricNamesInTehuti.UNHEALTHY_REQUEST_LATENCY, Arrays.asList(new Avg(), new Max(0)));
            put(MetricNamesInTehuti.TARDY_REQUEST_LATENCY, Arrays.asList(new Avg(), new Max(0)));
            put(MetricNamesInTehuti.THROTTLED_REQUEST_LATENCY, Arrays.asList(new Avg(), new Max(0)));
          }
        });

    retryCountMetric = new MetricEntityState(
        RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        new HashMap<String, List<MeasurableStat>>() {
          {
            put(MetricNamesInTehuti.ERROR_RETRY, Collections.singletonList(new Count()));
          }
        });

    allowedRetryCountMetric = new MetricEntityState(
        ALLOWED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        new HashMap<String, List<MeasurableStat>>() {
          {
            put(MetricNamesInTehuti.ALLOWED_RETRY_REQUEST, Collections.singletonList(new OccurrenceRate()));
          }
        });

    disallowedRetryCountMetric = new MetricEntityState(
        DISALLOWED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        new HashMap<String, List<MeasurableStat>>() {
          {
            put(MetricNamesInTehuti.DISALLOWED_RETRY_REQUEST, Collections.singletonList(new OccurrenceRate()));
          }
        });

    retryDelayMetric = new MetricEntityState(
        RETRY_DELAY.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        new HashMap<String, List<MeasurableStat>>() {
          {
            put(MetricNamesInTehuti.RETRY_DELAY, Arrays.asList(new Avg(), new Max()));
          }
        });

    abortedRetryCountMetric = new MetricEntityState(
        ABORTED_RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        new HashMap<String, List<MeasurableStat>>() {
          {
            put(MetricNamesInTehuti.DELAY_CONSTRAINT_ABORTED_RETRY_REQUEST, Collections.singletonList(new Count()));
            put(MetricNamesInTehuti.SLOW_ROUTE_ABORTED_RETRY_REQUEST, Collections.singletonList(new Count()));
            put(MetricNamesInTehuti.RETRY_ROUTE_LIMIT_ABORTED_RETRY_REQUEST, Collections.singletonList(new Count()));
            put(MetricNamesInTehuti.NO_AVAILABLE_REPLICA_ABORTED_RETRY_REQUEST, Collections.singletonList(new Count()));
          }
        });

    keyCountMetric = new MetricEntityState(
        CALL_KEY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorFinal,
        new HashMap<String, List<MeasurableStat>>() {
          {
            put(MetricNamesInTehuti.KEY_NUM, Arrays.asList(new OccurrenceRate(), new Avg(), new Max(0)));
            put(MetricNamesInTehuti.BAD_REQUEST_KEY_COUNT, Arrays.asList(new OccurrenceRate(), new Avg(), new Max(0)));
          }
        });

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
  }

  private String getDimensionName(VeniceMetricsDimensions dimension) {
    return dimension.getDimensionName(openTelemetryMetricFormat);
  }

  /**
   * We record this at the beginning of request handling, so we don't know the latency yet... All specific
   * types of requests also have their latencies logged at the same time.
   */
  public void recordIncomingRequest() {
    incomingRequestMetric.record(MetricNamesInTehuti.INCOMING_REQUEST, 1, commonMetricDimensions);
    inFlightRequestSensor.record(currentInFlightRequest.incrementAndGet());
    totalInflightRequestSensor.record();
  }

  public void recordHealthyRequest(Double latency, HttpResponseStatus responseStatus) {
    String metricNameInTehuti = MetricNamesInTehuti.HEALTHY_REQUEST;
    VeniceResponseStatusCategory veniceResponseStatusCategory = VeniceResponseStatusCategory.HEALTHY;
    recordRequestMetric(metricNameInTehuti, responseStatus, veniceResponseStatusCategory);
    if (latency != null) {
      recordLatencyMetric(metricNameInTehuti, latency, responseStatus, veniceResponseStatusCategory);
    }
  }

  public void recordUnhealthyRequest(HttpResponseStatus responseStatus) {
    recordRequestMetric(MetricNamesInTehuti.UNHEALTHY_REQUEST, responseStatus, VeniceResponseStatusCategory.UNHEALTHY);
  }

  public void recordUnhealthyRequest(double latency, HttpResponseStatus responseStatus) {
    recordUnhealthyRequest(responseStatus);
    recordLatencyMetric(
        MetricNamesInTehuti.UNHEALTHY_REQUEST_LATENCY,
        latency,
        responseStatus,
        VeniceResponseStatusCategory.UNHEALTHY);
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
    String metricNameInTehuti = MetricNamesInTehuti.TARDY_REQUEST;
    VeniceResponseStatusCategory veniceResponseStatusCategory = VeniceResponseStatusCategory.TARDY;
    recordRequestMetric(metricNameInTehuti, responseStatus, veniceResponseStatusCategory);
    recordLatencyMetric(metricNameInTehuti, latency, responseStatus, veniceResponseStatusCategory);
  }

  public void recordThrottledRequest(double latency, HttpResponseStatus responseStatus) {
    recordThrottledRequest(responseStatus);
    recordLatencyMetric(
        MetricNamesInTehuti.THROTTLED_REQUEST_LATENCY,
        latency,
        responseStatus,
        VeniceResponseStatusCategory.THROTTLED);
  }

  /**
   * Once we stop reporting throttled requests in {@link com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils},
   * and we only report them in {@link com.linkedin.venice.router.api.VeniceResponseAggregator} then we will always have
   * a latency and we'll be able to remove this overload.
   *
   * TODO: Remove this overload after fixing the above.
   */
  public void recordThrottledRequest(HttpResponseStatus responseStatus) {
    recordRequestMetric(MetricNamesInTehuti.THROTTLED_REQUEST, responseStatus, VeniceResponseStatusCategory.THROTTLED);
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
    retryCountMetric.record(MetricNamesInTehuti.ERROR_RETRY, 1, dimensions);
  }

  public void recordAbortedRetrySensorOtel(String tehutiMetricName, RequestRetryAbortReason abortReason) {
    Attributes dimensions = null;
    if (emitOpenTelemetryMetrics) {
      dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          .put(getDimensionName(VENICE_REQUEST_RETRY_ABORT_REASON), abortReason.getAbortReason())
          .build();
    }
    abortedRetryCountMetric.record(tehutiMetricName, 1, dimensions);
  }

  public void recordBadRequest(HttpResponseStatus responseStatus) {
    recordRequestMetric(MetricNamesInTehuti.BAD_REQUEST, responseStatus, VeniceResponseStatusCategory.BAD_REQUEST);
  }

  public void recordBadRequestKeyCount(int keyCount) {
    recordKeyCountMetric(keyCount, RequestValidationOutcome.INVALID_KEY_COUNT_LIMIT_EXCEEDED);
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

  public void recordLatencyMetric(
      String tehutiMetricName,
      double latency,
      HttpResponseStatus responseStatus,
      VeniceResponseStatusCategory veniceResponseStatusCategory) {
    Attributes dimensions = null;
    if (emitOpenTelemetryMetrics) {
      dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          // Don't add HTTP_RESPONSE_STATUS_CODE to reduce the cardinality for histogram
          .put(
              getDimensionName(HTTP_RESPONSE_STATUS_CODE_CATEGORY),
              getVeniceHttpResponseStatusCodeCategory(responseStatus))
          .put(getDimensionName(VENICE_RESPONSE_STATUS_CODE_CATEGORY), veniceResponseStatusCategory.getCategory())
          .build();
    }
    latencyMetric.record(tehutiMetricName, latency, dimensions);
  }

  public void recordRequestMetric(
      String tehutiMetricName,
      HttpResponseStatus responseStatus,
      VeniceResponseStatusCategory veniceResponseStatusCategory) {
    Attributes dimensions = null;
    if (emitOpenTelemetryMetrics) {
      dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          .put(
              getDimensionName(HTTP_RESPONSE_STATUS_CODE_CATEGORY),
              getVeniceHttpResponseStatusCodeCategory(responseStatus))
          .put(getDimensionName(VENICE_RESPONSE_STATUS_CODE_CATEGORY), veniceResponseStatusCategory.getCategory())
          .put(getDimensionName(HTTP_RESPONSE_STATUS_CODE), responseStatus.codeAsText().toString())
          .build();
    }
    requestMetric.record(tehutiMetricName, 1, dimensions);
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
    recordKeyCountMetric(keyNum, RequestValidationOutcome.VALID);
  }

  public void recordKeyCountMetric(int keyNum, RequestValidationOutcome outcome) {
    Attributes dimensions = null;
    if (emitOpenTelemetryMetrics) {
      dimensions = Attributes.builder()
          .putAll(commonMetricDimensions)
          .put(getDimensionName(VENICE_REQUEST_VALIDATION_OUTCOME), outcome.getOutcome())
          .build();
    }
    keyCountMetric.record(MetricNamesInTehuti.KEY_NUM, keyNum, dimensions);
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
    recordAbortedRetrySensorOtel(
        MetricNamesInTehuti.DELAY_CONSTRAINT_ABORTED_RETRY_REQUEST,
        RequestRetryAbortReason.DELAY_CONSTRAINT);
  }

  public void recordSlowRouteAbortedRetryRequest() {
    recordAbortedRetrySensorOtel(
        MetricNamesInTehuti.SLOW_ROUTE_ABORTED_RETRY_REQUEST,
        RequestRetryAbortReason.SLOW_ROUTE);
  }

  public void recordRetryRouteLimitAbortedRetryRequest() {
    recordAbortedRetrySensorOtel(
        MetricNamesInTehuti.RETRY_ROUTE_LIMIT_ABORTED_RETRY_REQUEST,
        RequestRetryAbortReason.MAX_RETRY_ROUTE_LIMIT);
  }

  public void recordNoAvailableReplicaAbortedRetryRequest() {
    recordAbortedRetrySensorOtel(
        MetricNamesInTehuti.NO_AVAILABLE_REPLICA_ABORTED_RETRY_REQUEST,
        RequestRetryAbortReason.NO_AVAILABLE_REPLICA);
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
    allowedRetryCountMetric.record(MetricNamesInTehuti.ALLOWED_RETRY_REQUEST, 1, commonMetricDimensions);
  }

  public void recordDisallowedRetryRequest() {
    disallowedRetryCountMetric.record(MetricNamesInTehuti.DISALLOWED_RETRY_REQUEST, 1, commonMetricDimensions);
  }

  public void recordErrorRetryAttemptTriggeredByPendingRequestCheck() {
    errorRetryAttemptTriggeredByPendingRequestCheckSensor.record();
  }

  public void recordRetryDelay(double delay) {
    retryDelayMetric.record(MetricNamesInTehuti.RETRY_DELAY, delay, commonMetricDimensions);
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

  static public boolean hasInFlightRequests() {
    Metric metric = localMetricRepo.getMetric("total_inflight_request_count");
    // max return -infinity when there are no samples. validate only against finite value
    return Double.isFinite(metric.value()) ? metric.value() > 0.0 : false;
  }

  /**
   * Metric names for tehuti metrics used in this class
   */
  private static class MetricNamesInTehuti {
    /** for {@link RouterMetricEntity#INCOMING_CALL_COUNT} */
    private final static String INCOMING_REQUEST = "request";

    /** for {@link RouterMetricEntity#CALL_COUNT} */
    private final static String HEALTHY_REQUEST = "healthy_request";
    private final static String UNHEALTHY_REQUEST = "unhealthy_request";
    private final static String TARDY_REQUEST = "tardy_request";
    private final static String THROTTLED_REQUEST = "throttled_request";
    private final static String BAD_REQUEST = "bad_request";

    /** for {@link RouterMetricEntity#CALL_TIME} */
    private final static String HEALTHY_REQUEST_LATENCY = "healthy_request_latency";
    private final static String UNHEALTHY_REQUEST_LATENCY = "unhealthy_request_latency";
    private final static String TARDY_REQUEST_LATENCY = "tardy_request_latency";
    private final static String THROTTLED_REQUEST_LATENCY = "throttled_request_latency";

    /** for {@link RouterMetricEntity#RETRY_COUNT} */
    private final static String ERROR_RETRY = "error_retry";

    /** for {@link RouterMetricEntity#ALLOWED_RETRY_COUNT} */
    private final static String ALLOWED_RETRY_REQUEST = "allowed_retry_request_count";

    /** for {@link RouterMetricEntity#DISALLOWED_RETRY_COUNT} */
    private final static String DISALLOWED_RETRY_REQUEST = "disallowed_retry_request_count";

    /** for {@link RouterMetricEntity#RETRY_DELAY} */
    private final static String RETRY_DELAY = "retry_delay";

    /** for {@link RouterMetricEntity#ABORTED_RETRY_COUNT} */
    private final static String DELAY_CONSTRAINT_ABORTED_RETRY_REQUEST = "delay_constraint_aborted_retry_request";
    private final static String SLOW_ROUTE_ABORTED_RETRY_REQUEST = "slow_route_aborted_retry_request";
    private final static String RETRY_ROUTE_LIMIT_ABORTED_RETRY_REQUEST = "retry_route_limit_aborted_retry_request";
    private final static String NO_AVAILABLE_REPLICA_ABORTED_RETRY_REQUEST =
        "no_available_replica_aborted_retry_request";

    /** for {@link RouterMetricEntity#CALL_KEY_COUNT} */
    private final static String KEY_NUM = "key_num";
    private final static String BAD_REQUEST_KEY_COUNT = "bad_request_key_count";
  }
}
