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
  private final Sensor cachePutRequestSensor;
  private final Sensor cachePutLatencySensor;
  private final Sensor keyNumSensor;

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
    cachePutRequestSensor = registerSensor("cache_put_request", new Count());
    cachePutLatencySensor = registerSensor("cache_put_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("cache_put_latency")));

    keyNumSensor = registerSensor("key_num", new Avg(), new Max());
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

  public void recordCachePutRequest() {
    cachePutRequestSensor.record();
  }

  public void recordCachePutLatency(double latency) {
    cachePutLatencySensor.record(latency);
  }

  public void recordKeyNum(int keyNum) {
    keyNumSensor.record(keyNum);
  }
}
