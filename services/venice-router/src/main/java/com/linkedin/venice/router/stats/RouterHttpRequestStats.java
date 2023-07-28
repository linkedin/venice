package com.linkedin.venice.router.stats;

import static com.linkedin.venice.stats.AbstractVeniceAggStats.*;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.LambdaStat;
import com.linkedin.venice.stats.TehutiUtils;
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
import java.util.concurrent.atomic.AtomicInteger;


public class RouterHttpRequestStats extends AbstractVeniceHttpStats {
  private Sensor requestSensor = null;
  private Sensor healthySensor = null;
  private Sensor unhealthySensor = null;
  private Sensor tardySensor = null;
  private Sensor healthyRequestRateSensor = null;
  private Sensor tardyRequestRatioSensor = null;
  private Sensor throttleSensor = null;
  private Sensor latencySensor = null;
  private Sensor healthyRequestLatencySensor = null;
  private Sensor unhealthyRequestLatencySensor = null;
  private Sensor tardyRequestLatencySensor = null;
  private Sensor throttledRequestLatencySensor = null;
  private Sensor requestSizeSensor = null;
  private Sensor compressedResponseSizeSensor = null;
  private Sensor responseSizeSensor = null;
  private Sensor badRequestSensor = null;
  private Sensor badRequestKeyCountSensor = null;
  private Sensor requestThrottledByRouterCapacitySensor = null;
  private Sensor decompressionTimeSensor = null;
  private Sensor routerResponseWaitingTimeSensor = null;
  private Sensor fanoutRequestCountSensor = null;
  private Sensor quotaSensor = null;
  private Sensor findUnhealthyHostRequestSensor = null;
  private Sensor keyNumSensor = null;
  // Reflect the real request usage, e.g count each key as an unit of request usage.
  private Sensor requestUsageSensor = null;
  private Sensor requestParsingLatencySensor = null;
  private Sensor requestRoutingLatencySensor = null;
  private Sensor unAvailableRequestSensor = null;
  private Sensor delayConstraintAbortedRetryRequest = null;
  private Sensor slowRouteAbortedRetryRequest = null;
  private Sensor retryRouteLimitAbortedRetryRequest = null;
  private Sensor noAvailableReplicaAbortedRetryRequest = null;
  private Sensor readQuotaUsageSensor = null;
  private Sensor inFlightRequestSensor = null;
  private AtomicInteger currentInFlightRequest = null;
  private Sensor unavailableReplicaStreamingRequestSensor = null;
  private Sensor allowedRetryRequestSensor = null;
  private Sensor disallowedRetryRequestSensor = null;
  private Sensor errorRetryAttemptTriggeredByPendingRequestCheckSensor = null;
  private Sensor retryDelaySensor = null;
  private Sensor multiGetFallbackSensor = null;
  private Sensor metaStoreShadowReadSensor = null;
  private Sensor keySizeSensor;

  private boolean isSystemStore;

  // QPS metrics
  public RouterHttpRequestStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      ScatterGatherStats scatterGatherStats,
      boolean isKeyValueProfilingEnabled) {
    super(metricsRepository, storeName, requestType);
    this.isSystemStore = VeniceSystemStoreUtils.isSystemStore(storeName);

    if (!isSystemStore) {
      Rate requestRate = new OccurrenceRate();
      Rate healthyRequestRate = new OccurrenceRate();
      Rate tardyRequestRate = new OccurrenceRate();
      requestSensor = registerSensor("request", new Count(), requestRate);
      healthySensor = registerSensor("healthy_request", new Count(), healthyRequestRate);
      unhealthySensor = registerSensor("unhealthy_request", new Count());
      unavailableReplicaStreamingRequestSensor = registerSensor("unavailable_replica_streaming_request", new Count());
      tardySensor = registerSensor("tardy_request", new Count(), tardyRequestRate);
      healthyRequestRateSensor =
          registerSensor("healthy_request_ratio", new TehutiUtils.SimpleRatioStat(healthyRequestRate, requestRate));
      tardyRequestRatioSensor =
          registerSensor("tardy_request_ratio", new TehutiUtils.SimpleRatioStat(tardyRequestRate, requestRate));
      throttleSensor = registerSensor("throttled_request", new Count());
      badRequestSensor = registerSensor("bad_request", new Count());
      badRequestKeyCountSensor = registerSensor("bad_request_key_count", new OccurrenceRate(), new Avg(), new Max());
      requestThrottledByRouterCapacitySensor = registerSensor("request_throttled_by_router_capacity", new Count());
      fanoutRequestCountSensor = registerSensor("fanout_request_count", new Avg(), new Max(0));
      latencySensor = registerSensorWithDetailedPercentiles("latency", new Avg(), new Max(0));
      healthyRequestLatencySensor =
          registerSensorWithDetailedPercentiles("healthy_request_latency", new Avg(), new Max(0));
      unhealthyRequestLatencySensor =
          registerSensorWithDetailedPercentiles("unhealthy_request_latency", new Avg(), new Max(0));
      tardyRequestLatencySensor = registerSensorWithDetailedPercentiles("tardy_request_latency", new Avg(), new Max(0));
      throttledRequestLatencySensor =
          registerSensorWithDetailedPercentiles("throttled_request_latency", new Avg(), new Max(0));
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

      registerSensor("retry_count", new LambdaStat(() -> scatterGatherStats.getTotalRetries()));
      registerSensor("retry_key_count", new LambdaStat(() -> scatterGatherStats.getTotalRetriedKeys()));
      registerSensor(
          "retry_slower_than_original_count",
          new LambdaStat(() -> scatterGatherStats.getTotalRetriesDiscarded()));
      registerSensor("retry_error_count", new LambdaStat(() -> scatterGatherStats.getTotalRetriesError()));
      registerSensor(
          "retry_faster_than_original_count",
          new LambdaStat(() -> scatterGatherStats.getTotalRetriesWinner()));

      keyNumSensor = registerSensor("key_num", new Avg(), new Max(0));
      /**
       * request_usage.Total is incoming KPS while request_usage.OccurrenceRate is QPS
       */
      requestUsageSensor = registerSensor("request_usage", new Total(), new OccurrenceRate());
      multiGetFallbackSensor = registerSensor("multiget_fallback", new Total(), new OccurrenceRate());

      requestParsingLatencySensor = registerSensor("request_parse_latency", new Avg());
      requestRoutingLatencySensor = registerSensor("request_route_latency", new Avg());

      unAvailableRequestSensor = registerSensor("unavailable_request", new Count());

      delayConstraintAbortedRetryRequest = registerSensor("delay_constraint_aborted_retry_request", new Count());
      slowRouteAbortedRetryRequest = registerSensor("slow_route_aborted_retry_request", new Count());
      retryRouteLimitAbortedRetryRequest = registerSensor("retry_route_limit_aborted_retry_request", new Count());
      noAvailableReplicaAbortedRetryRequest = registerSensor("no_available_replica_aborted_retry_request", new Count());

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

      allowedRetryRequestSensor = registerSensor("allowed_retry_request_count", new OccurrenceRate());
      disallowedRetryRequestSensor = registerSensor("disallowed_retry_request_count", new OccurrenceRate());
      errorRetryAttemptTriggeredByPendingRequestCheckSensor =
          registerSensor("error_retry_attempt_triggered_by_pending_request_check", new OccurrenceRate());
      retryDelaySensor = registerSensor("retry_delay", new Avg(), new Max());
      metaStoreShadowReadSensor = registerSensor("meta_store_shadow_read", new OccurrenceRate());
    }
  }

  /**
   * We record this at the beginning of request handling, so we don't know the latency yet... All specific
   * types of requests also have their latencies logged at the same time.
   */
  public void recordRequest() {
    if (isSystemStore) {
      return;
    }
    requestSensor.record();
    inFlightRequestSensor.record(currentInFlightRequest.incrementAndGet());
  }

  public void recordHealthyRequest(Double latency) {
    if (isSystemStore) {
      return;
    }
    healthySensor.record();
    if (latency != null) {
      healthyRequestLatencySensor.record(latency);
    }
  }

  public void recordUnhealthyRequest() {
    if (isSystemStore) {
      return;
    }
    unhealthySensor.record();
  }

  public void recordUnavailableReplicaStreamingRequest() {
    if (isSystemStore) {
      return;
    }
    unavailableReplicaStreamingRequestSensor.record();
  }

  public void recordUnhealthyRequest(double latency) {
    if (isSystemStore) {
      return;
    }
    recordUnhealthyRequest();
    unhealthyRequestLatencySensor.record(latency);
  }

  /**
   * Record read quota usage based on healthy KPS.
   * @param quotaUsage
   */
  public void recordReadQuotaUsage(int quotaUsage) {
    if (isSystemStore) {
      return;
    }
    readQuotaUsageSensor.record(quotaUsage);
  }

  public void recordTardyRequest(double latency) {
    if (isSystemStore) {
      return;
    }
    tardySensor.record();
    tardyRequestLatencySensor.record(latency);
  }

  public void recordThrottledRequest(double latency) {
    if (isSystemStore) {
      return;
    }
    recordThrottledRequest();
    throttledRequestLatencySensor.record(latency);
  }

  /**
   * Once we stop reporting throttled requests in {@link com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils},
   * and we only report them in {@link com.linkedin.venice.router.api.VeniceResponseAggregator} then we will always have
   * a latency and we'll be able to remove this overload.
   *
   * TODO: Remove this overload after fixing the above.
   */
  public void recordThrottledRequest() {
    if (isSystemStore) {
      return;
    }
    throttleSensor.record();
  }

  public void recordBadRequest() {
    if (isSystemStore) {
      return;
    }
    badRequestSensor.record();
  }

  public void recordBadRequestKeyCount(int keyCount) {
    if (isSystemStore) {
      return;
    }
    badRequestKeyCountSensor.record(keyCount);
  }

  public void recordRequestThrottledByRouterCapacity() {
    if (isSystemStore) {
      return;
    }
    requestThrottledByRouterCapacitySensor.record();
  }

  public void recordFanoutRequestCount(int count) {
    if (isSystemStore) {
      return;
    }
    if (!getRequestType().equals(RequestType.SINGLE_GET)) {
      fanoutRequestCountSensor.record(count);
    }
  }

  public void recordLatency(double latency) {
    if (isSystemStore) {
      return;
    }
    latencySensor.record(latency);
  }

  public void recordResponseWaitingTime(double waitingTime) {
    if (isSystemStore) {
      return;
    }
    routerResponseWaitingTimeSensor.record(waitingTime);
  }

  public void recordRequestSize(double requestSize) {
    if (isSystemStore) {
      return;
    }
    requestSizeSensor.record(requestSize);
  }

  public void recordCompressedResponseSize(double compressedResponseSize) {
    if (isSystemStore) {
      return;
    }
    compressedResponseSizeSensor.record(compressedResponseSize);
  }

  public void recordResponseSize(double responseSize) {
    if (isSystemStore) {
      return;
    }
    responseSizeSensor.record(responseSize);
  }

  public void recordDecompressionTime(double decompressionTime) {
    if (isSystemStore) {
      return;
    }
    decompressionTimeSensor.record(decompressionTime);
  }

  public void recordQuota(double quota) {
    if (isSystemStore) {
      return;
    }
    quotaSensor.record(quota);
  }

  public void recordFindUnhealthyHostRequest() {
    if (isSystemStore) {
      return;
    }
    findUnhealthyHostRequestSensor.record();
  }

  public void recordKeyNum(int keyNum) {
    if (isSystemStore) {
      return;
    }
    keyNumSensor.record(keyNum);
  }

  public void recordRequestUsage(int usage) {
    if (isSystemStore) {
      return;
    }
    requestUsageSensor.record(usage);
  }

  public void recordMultiGetFallback(int keyCount) {
    if (isSystemStore) {
      return;
    }
    multiGetFallbackSensor.record(keyCount);
  }

  public void recordRequestParsingLatency(double latency) {
    if (isSystemStore) {
      return;
    }
    requestParsingLatencySensor.record(latency);
  }

  public void recordRequestRoutingLatency(double latency) {
    if (isSystemStore) {
      return;
    }
    requestRoutingLatencySensor.record(latency);
  }

  public void recordUnavailableRequest() {
    if (isSystemStore) {
      return;
    }
    unAvailableRequestSensor.record();
  }

  public void recordDelayConstraintAbortedRetryRequest() {
    if (isSystemStore) {
      return;
    }
    delayConstraintAbortedRetryRequest.record();
  }

  public void recordSlowRouteAbortedRetryRequest() {
    if (isSystemStore) {
      return;
    }
    slowRouteAbortedRetryRequest.record();
  }

  public void recordRetryRouteLimitAbortedRetryRequest() {
    if (isSystemStore) {
      return;
    }
    retryRouteLimitAbortedRetryRequest.record();
  }

  public void recordNoAvailableReplicaAbortedRetryRequest() {
    if (isSystemStore) {
      return;
    }
    noAvailableReplicaAbortedRetryRequest.record();
  }

  public void recordKeySizeInByte(long keySize) {
    if (isSystemStore) {
      return;
    }
    if (keySizeSensor != null) {
      keySizeSensor.record(keySize);
    }
  }

  public void recordResponse() {
    if (isSystemStore) {
      return;
    }
    /**
     * We already report into the sensor when the request starts, in {@link #recordRequest()}, so at response time
     * there is no need to record into the sensor again. We just want to maintain the bookkeeping.
     */
    currentInFlightRequest.decrementAndGet();
  }

  public void recordAllowedRetryRequest() {
    if (isSystemStore) {
      return;
    }
    allowedRetryRequestSensor.record();
  }

  public void recordDisallowedRetryRequest() {
    if (isSystemStore) {
      return;
    }
    disallowedRetryRequestSensor.record();
  }

  public void recordErrorRetryAttemptTriggeredByPendingRequestCheck() {
    if (isSystemStore) {
      return;
    }
    errorRetryAttemptTriggeredByPendingRequestCheckSensor.record();
  }

  public void recordRetryDelay(double delay) {
    if (isSystemStore) {
      return;
    }
    retryDelaySensor.record(delay);
  }

  public void recordMetaStoreShadowRead() {
    if (isSystemStore) {
      return;
    }
    metaStoreShadowReadSensor.record();
  }
}
