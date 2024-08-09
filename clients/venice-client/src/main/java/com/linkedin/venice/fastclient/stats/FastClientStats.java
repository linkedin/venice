package com.linkedin.venice.fastclient.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.StatsUtils;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class FastClientStats extends com.linkedin.venice.client.stats.ClientStats {
  private static final Logger LOGGER = LogManager.getLogger(FastClientStats.class);

  private final String storeName;

  private final Sensor noAvailableReplicaRequestCountSensor;
  private final Lazy<Sensor> dualReadFastClientSlowerRequestCountSensor;
  private final Sensor dualReadFastClientSlowerRequestRatioSensor;
  private final Lazy<Sensor> dualReadFastClientErrorThinClientSucceedRequestCountSensor;
  private final Sensor dualReadFastClientErrorThinClientSucceedRequestRatioSensor;
  private final Lazy<Sensor> dualReadThinClientFastClientLatencyDeltaSensor;

  private final Sensor leakedRequestCountSensor;

  private final Sensor longTailRetryRequestSensor;
  private final Sensor errorRetryRequestSensor;
  private final Sensor retryRequestWinSensor;

  private final Sensor metadataStalenessSensor;
  private long cacheTimeStampInMs = 0;

  // Routing stats
  private final Map<String, RouteStats> perRouteStats = new VeniceConcurrentHashMap<>();

  public static FastClientStats getClientStats(
      MetricsRepository metricsRepository,
      String statsPrefix,
      String storeName,
      RequestType requestType) {
    String metricName = statsPrefix.isEmpty() ? storeName : statsPrefix + "." + storeName;
    return new FastClientStats(metricsRepository, metricName, requestType);
  }

  private FastClientStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    super(metricsRepository, storeName, requestType);

    this.storeName = storeName;
    this.noAvailableReplicaRequestCountSensor =
        registerSensor("no_available_replica_request_count", new OccurrenceRate());

    Rate requestRate = getRequestRate();
    Rate fastClientSlowerRequestRate = new OccurrenceRate();
    this.dualReadFastClientSlowerRequestCountSensor =
        registerLazySensor("dual_read_fastclient_slower_request_count", fastClientSlowerRequestRate);
    this.dualReadFastClientSlowerRequestRatioSensor = registerSensor(
        new TehutiUtils.SimpleRatioStat(
            fastClientSlowerRequestRate,
            requestRate,
            "dual_read_fastclient_slower_request_ratio"));
    Rate fastClientErrorThinClientSucceedRequestRate = new OccurrenceRate();
    this.dualReadFastClientErrorThinClientSucceedRequestCountSensor = registerLazySensor(
        "dual_read_fastclient_error_thinclient_succeed_request_count",
        fastClientErrorThinClientSucceedRequestRate);
    this.dualReadFastClientErrorThinClientSucceedRequestRatioSensor = registerSensor(
        new TehutiUtils.SimpleRatioStat(
            fastClientErrorThinClientSucceedRequestRate,
            requestRate,
            "dual_read_fastclient_error_thinclient_succeed_request_ratio"));
    this.dualReadThinClientFastClientLatencyDeltaSensor = registerLazySensorWithDetailedPercentiles(
        "dual_read_thinclient_fastclient_latency_delta",
        new Max(),
        new Avg());
    this.leakedRequestCountSensor = registerSensor("leaked_request_count", new OccurrenceRate());
    this.longTailRetryRequestSensor = registerSensor("long_tail_retry_request", new OccurrenceRate());
    this.errorRetryRequestSensor = registerSensor("error_retry_request", new OccurrenceRate());
    this.retryRequestWinSensor = registerSensor("retry_request_win", new OccurrenceRate());

    this.metadataStalenessSensor = registerSensor(new AsyncGauge((ignored, ignored2) -> {
      if (this.cacheTimeStampInMs == 0) {
        return Double.NaN;
      } else {
        return System.currentTimeMillis() - this.cacheTimeStampInMs;
      }
    }, "metadata_staleness_high_watermark_ms"));
  }

  public void recordNoAvailableReplicaRequest() {
    noAvailableReplicaRequestCountSensor.record();
  }

  public void recordFastClientSlowerRequest() {
    dualReadFastClientSlowerRequestCountSensor.get().record();
  }

  public void recordFastClientErrorThinClientSucceedRequest() {
    dualReadFastClientErrorThinClientSucceedRequestCountSensor.get().record();
  }

  public void recordThinClientFastClientLatencyDelta(double latencyDelta) {
    dualReadThinClientFastClientLatencyDeltaSensor.get().record(latencyDelta);
  }

  private RouteStats getRouteStats(String instanceUrl) {
    return perRouteStats.computeIfAbsent(instanceUrl, k -> {
      String instanceName = instanceUrl;
      try {
        URL url = new URL(instanceUrl);
        instanceName = url.getHost() + "_" + url.getPort();
      } catch (MalformedURLException e) {
        LOGGER.error("Invalid instance url: {}", instanceUrl);
      }
      return new RouteStats(getMetricsRepository(), storeName, instanceName);
    });
  }

  public void recordRequest(String instance) {
    getRouteStats(instance).recordRequest();
  }

  public void recordResponseWaitingTime(String instance, double latency) {
    getRouteStats(instance).recordResponseWaitingTime(latency);
  }

  public void recordHealthyRequest(String instance) {
    getRouteStats(instance).recordHealthyRequest();
  }

  public void recordQuotaExceededRequest(String instance) {
    getRouteStats(instance).recordQuotaExceededRequest();
  }

  public void recordInternalServerErrorRequest(String instance) {
    getRouteStats(instance).recordInternalServerErrorRequest();
  }

  public void recordServiceUnavailableRequest(String instance) {
    getRouteStats(instance).recordServiceUnavailableRequest();
  }

  public void recordLeakedRequest(String instance) {
    leakedRequestCountSensor.record();
    getRouteStats(instance).recordLeakedRequest();
  }

  public void recordOtherErrorRequest(String instance) {
    getRouteStats(instance).recordOtherErrorRequest();
  }

  public void recordLongTailRetryRequest() {
    longTailRetryRequestSensor.record();
  }

  public void recordErrorRetryRequest() {
    errorRetryRequestSensor.record();
  }

  public void recordRetryRequestWin() {
    retryRequestWinSensor.record();
  }

  public void updateCacheTimestamp(long cacheTimeStampInMs) {
    this.cacheTimeStampInMs = cacheTimeStampInMs;
  }

  /**
   * This method is a utility method to build concise summaries useful in tests
   * and for logging. It generates a single string for all metrics for a sensor
   * @return
   * @param sensorName
   */
  public String buildSensorStatSummary(String sensorName, String... stats) {
    List<Double> metricValues = getMetricValues(sensorName, stats);
    StringBuilder builder = new StringBuilder();
    String sensorFullName = getSensorFullName(sensorName);
    builder.append(sensorFullName).append(":");
    builder.append(
        IntStream.range(0, stats.length)
            .mapToObj((statIdx) -> stats[statIdx] + "=" + metricValues.get(statIdx))
            .collect(Collectors.joining(",")));
    return builder.toString();
  }

  /**
   * This method is a utility method to get metric values useful in tests
   * and for logging.
   * @return
   * @param sensorName
   */
  public List<Double> getMetricValues(String sensorName, String... stats) {
    String sensorFullName = getSensorFullName(sensorName);
    List<Double> collect = Arrays.stream(stats).map((stat) -> {
      Metric metric = getMetricsRepository().getMetric(sensorFullName + "." + stat);
      return (metric != null ? metric.value() : Double.NaN);
    }).collect(Collectors.toList());
    return collect;
  }

  /**
   * Per-route request metrics.
   */
  private static class RouteStats extends AbstractVeniceStats {
    private final Lazy<Sensor> requestCountSensor;
    private final Lazy<Sensor> responseWaitingTimeSensor;
    private final Lazy<Sensor> healthyRequestCountSensor;
    private final Lazy<Sensor> quotaExceededRequestCountSensor;
    private final Lazy<Sensor> internalServerErrorRequestCountSensor;
    private final Lazy<Sensor> serviceUnavailableRequestCountSensor;
    private final Lazy<Sensor> leakedRequestCountSensor;
    private final Lazy<Sensor> otherErrorRequestCountSensor;

    public RouteStats(MetricsRepository metricsRepository, String storeName, String instanceName) {
      super(metricsRepository, storeName + "." + StatsUtils.convertHostnameToMetricName(instanceName));
      this.requestCountSensor = registerLazySensor("request_count", new OccurrenceRate());
      this.responseWaitingTimeSensor = registerLazySensor(
          "response_waiting_time",
          TehutiUtils.getPercentileStat(getName(), "response_waiting_time"));
      this.healthyRequestCountSensor = registerLazySensor("healthy_request_count", new OccurrenceRate());
      this.quotaExceededRequestCountSensor = registerLazySensor("quota_exceeded_request_count", new OccurrenceRate());
      this.internalServerErrorRequestCountSensor =
          registerLazySensor("internal_server_error_request_count", new OccurrenceRate());
      this.serviceUnavailableRequestCountSensor =
          registerLazySensor("service_unavailable_request_count", new OccurrenceRate());
      this.leakedRequestCountSensor = registerLazySensor("leaked_request_count", new OccurrenceRate());
      this.otherErrorRequestCountSensor = registerLazySensor("other_error_request_count", new OccurrenceRate());
    }

    public void recordRequest() {
      requestCountSensor.get().record();
    }

    public void recordResponseWaitingTime(double latency) {
      responseWaitingTimeSensor.get().record(latency);
    }

    public void recordHealthyRequest() {
      healthyRequestCountSensor.get().record();
    }

    public void recordQuotaExceededRequest() {
      quotaExceededRequestCountSensor.get().record();
    }

    public void recordInternalServerErrorRequest() {
      internalServerErrorRequestCountSensor.get().record();
    }

    public void recordServiceUnavailableRequest() {
      serviceUnavailableRequestCountSensor.get().record();
    }

    public void recordLeakedRequest() {
      leakedRequestCountSensor.get().record();
    }

    public void recordOtherErrorRequest() {
      otherErrorRequestCountSensor.get().record();
    }
  }
}
