package com.linkedin.venice.router.stats;

import com.linkedin.ddsstorage.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.LambdaStat;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;


public class RouterHttpRequestStats extends AbstractVeniceHttpStats {
  private final Sensor requestSensor;
  private final Sensor healthySensor;
  private final Sensor unhealthySensor;
  private final Sensor throttleSensor;
  private final Sensor latencySensor;
  private final Sensor requestSizeSensor;
  private final Sensor compressedResponseSizeSensor;
  private final Sensor responseSizeSensor;
  private final Sensor badRequestSensor;
  private final Sensor decompressionTimeSensor;
  private final Sensor routerResponseWaitingTimeSensor;
  private final Sensor fanoutRequestCountSensor;
  private final Sensor quotaSensor;
  private final Sensor findUnhealthyHostRequestSensor;
  private final Sensor cacheLookupRequestSensor;
  private final Sensor cacheHitRequestSensor;
  private final Sensor cacheHitRatioSensor;
  private final Sensor cacheLookupLatencySensor;
  private final Sensor cacheLookupLatencyForEachKeyInMultiGetSensor;
  private final Sensor cachePutRequestSensor;
  private final Sensor cachePutLatencySensor;
  private final Sensor cacheUpdateLatencyForMultiGetSensor;
  private final Sensor keyNumSensor;
  // Reflect the real request usage, e.g count each key as an unit of request usage.
  private final Sensor requestUsageSensor;

  private final Sensor cacheResultSerializationLatencySensor;
  private final Sensor responseResultsDeserializationLatencySensor;

  private final Sensor requestParsingLatencySensor;
  private final Sensor requestRoutingLatencySensor;

  private final Sensor unAvailableRequestSensor;

  private final Sensor delayConstraintAbortedRetryRequest;
  private final Sensor slowRouteAbortedRetryRequest;

  private final Sensor nettyClientFirstResponseLatencySensor;
  private final Sensor nettyClientLastResponseLatencySensor;

  private final Sensor nettyClientAcquireChannelFutureLatencySensor;
  //QPS metrics
  public RouterHttpRequestStats(MetricsRepository metricsRepository, String storeName, RequestType requestType,
      ScatterGatherStats scatterGatherStats) {
    super(metricsRepository, storeName, requestType);

    requestSensor = registerSensor("request", new Count(), new OccurrenceRate());
    healthySensor = registerSensor("healthy_request", new Count());
    unhealthySensor = registerSensor("unhealthy_request", new Count());
    throttleSensor = registerSensor("throttled_request", new Count());
    badRequestSensor = registerSensor("bad_request", new Count());
    fanoutRequestCountSensor = registerSensor("fanout_request_count", new Avg(), new Max());

    //we have to explicitly pass the name again for PercentilesStat here.
    //TODO: remove the redundancy once Tehuti library is updated.
    latencySensor = registerSensor("latency", TehutiUtils.getPercentileStatForNetworkLatency(getName(),
        getFullMetricName("latency")));
    routerResponseWaitingTimeSensor = registerSensor("response_waiting_time",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_waiting_time")));
    requestSizeSensor = registerSensor("request_size", TehutiUtils.getPercentileStat(getName(), getFullMetricName("request_size")), new Avg());
    compressedResponseSizeSensor = registerSensor("compressed_response_size",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("compressed_response_size")), new Avg());
    responseSizeSensor = registerSensor("response_size", TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_size")), new Avg());
    decompressionTimeSensor = registerSensor("decompression_time",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("decompression_time")), new Avg());
    quotaSensor = registerSensor("read_quota_per_router", new Gauge());
    findUnhealthyHostRequestSensor = registerSensor("find_unhealthy_host_request", new OccurrenceRate());

    registerSensor("retry_count", new LambdaStat(() -> scatterGatherStats.getTotalRetries()));
    registerSensor("retry_key_count", new LambdaStat(() -> scatterGatherStats.getTotalRetriedKeys()));
    registerSensor("retry_slower_than_original_count", new LambdaStat( () -> scatterGatherStats.getTotalRetriesDiscarded()));
    registerSensor("retry_error_count", new LambdaStat( () -> scatterGatherStats.getTotalRetriesError()));
    registerSensor("retry_faster_than_original_count", new LambdaStat( () -> scatterGatherStats.getTotalRetriesWinner()));

    Rate cacheLookupRequestRate = new OccurrenceRate();
    Rate cacheHitRequestRate = new OccurrenceRate();
    cacheLookupRequestSensor = registerSensor("cache_lookup_request", new Count(), cacheLookupRequestRate);
    cacheHitRequestSensor = registerSensor("cache_hit_request", new Count(), cacheHitRequestRate);
    cacheHitRatioSensor = registerSensor("cache_hit_ratio",
        new TehutiUtils.SimpleRatioStat(cacheHitRequestRate, cacheLookupRequestRate));
    cacheLookupLatencySensor = registerSensor("cache_lookup_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("cache_lookup_latency")));
    cacheLookupLatencyForEachKeyInMultiGetSensor = registerSensor("cache_lookup_latency_for_each_key_in_multiget",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("cache_lookup_latency_for_each_key_in_multiget")));
    cacheResultSerializationLatencySensor = registerSensor("cache_result_serialization_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("cache_result_serialization_latency")));
    responseResultsDeserializationLatencySensor = registerSensor("response_results_deserialization_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_results_deserialization_latency")));
    cacheUpdateLatencyForMultiGetSensor = registerSensor("cache_update_latency_for_multiget",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("cache_update_latency_for_multiget")));
    cachePutRequestSensor = registerSensor("cache_put_request", new Count());
    cachePutLatencySensor = registerSensor("cache_put_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("cache_put_latency")));

    keyNumSensor = registerSensor("key_num", new Avg(), new Max());
    requestUsageSensor = registerSensor("request_usage", new Count(), new OccurrenceRate());

    requestParsingLatencySensor = registerSensor("request_parse_latency", new Avg());
    requestRoutingLatencySensor = registerSensor("request_route_latency", new Avg());

    unAvailableRequestSensor = registerSensor("unavailable_request", new Count());

    delayConstraintAbortedRetryRequest = registerSensor("delay_constraint_aborted_retry_request", new Count());
    slowRouteAbortedRetryRequest = registerSensor("slow_route_aborted_retry_request", new Count());

    /**
     * For each response, the response consumer will first accept a `HttpResponse` that doesn't contain any content;
     * then it will accept one or multiple `HttpContent` that contains the response content; finally, it will receive
     * a `LastHttpContent` which indicates the end of a response.
     *
     * The metric below tracks the latency from receiving a request in dispatcher to receiving the `HttpResponse` message.
     */
    nettyClientFirstResponseLatencySensor = registerSensor("netty_client_first_response_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("netty_client_first_response_latency")));
    /**
     * The metric below tracks the latency from receiving a request in dispacher to receiving the `LastHttpContent`
     * message. The gap between this metric and the previous metric can tell us how much time is spent on concatenating
     * all the response contents.
     */
    nettyClientLastResponseLatencySensor = registerSensor("netty_client_last_response_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("netty_client_last_response_latency")));

    nettyClientAcquireChannelFutureLatencySensor = registerSensor("netty_client_acquire_channel_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("netty_client_acquire_channel_latency")));
  }

  public void recordRequest() {
    requestSensor.record();
  }

  public void recordHealthyRequest() {
    healthySensor.record();
  }

  public void recordUnhealthyRequest() {
    unhealthySensor.record();
  }

  public void recordThrottledRequest() {
    throttleSensor.record();
  }

  public void recordBadRequest() {
    badRequestSensor.record();
  }

  public void recordFanoutRequestCount(int count) {
    if (getRequestType().equals(RequestType.MULTI_GET)) {
      fanoutRequestCountSensor.record(count);
    }
  }

  public void recordLatency(double latency) {
    latencySensor.record(latency);
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
  };

  public void recordDecompressionTime(double decompressionTime) {
    decompressionTimeSensor.record(decompressionTime);
  }

  public void recordQuota(double quota){
    quotaSensor.record(quota);
  }

  public void recordFindUnhealthyHostRequest() {
    findUnhealthyHostRequestSensor.record();
  }

  public void recordCacheHitRequest() {
    cacheHitRequestSensor.record();
  }

  public void recordCacheLookupRequest() {
    cacheLookupRequestSensor.record();
  }

  public void recordCacheLookupLatency(double latency) {
    cacheLookupLatencySensor.record(latency);
  }

  public void recordCacheLookupLatencyForEachKeyInMultiGet(double latency) {
    cacheLookupLatencyForEachKeyInMultiGetSensor.record(latency);
  }

  public void recordCacheResultSerializationLatency(double latency) {
    cacheResultSerializationLatencySensor.record(latency);
  }

  public void recordResponseResultsDeserializationLatency(double latency) {
    responseResultsDeserializationLatencySensor.record(latency);
  }

  public void recordCacheUpdateLatencyForMultiGet(double latency) {
    cacheUpdateLatencyForMultiGetSensor.record(latency);
  }

  public void recordCachePutRequest() {
    cachePutRequestSensor.record();
  }

  public void recordCachePutLatency(double latency) {
    cachePutLatencySensor.record(latency);
  }

  public void recordNettyClientFirstResponseLatency(double latency) {
    nettyClientFirstResponseLatencySensor.record(latency);
  }

  public void recordNettyClientLastResponseLatency(double latency) {
    nettyClientLastResponseLatencySensor.record(latency);
  }

  public void recordNettyClientAcquireChannelLatency(double latency) {
    nettyClientAcquireChannelFutureLatencySensor.record(latency);
  }

  public void recordKeyNum(int keyNum) {
    keyNumSensor.record(keyNum);
  }

  public void recordRequestUsage(int usage) {
    requestUsageSensor.record(usage);
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
  }

  public void recordSlowRouteAbortedRetryRequest() {
    slowRouteAbortedRetryRequest.record();
  }
}
