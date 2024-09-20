package com.linkedin.venice.fastclient.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.StatsUtils;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ClusterRouteStats {
  private static final Logger LOGGER = LogManager.getLogger(ClusterRouteStats.class);

  private static final ClusterRouteStats DEFAULT = new ClusterRouteStats();

  private final Map<String, RouteStats> perRouteStatMap = new VeniceConcurrentHashMap<>();

  public static ClusterRouteStats get() {
    return DEFAULT;
  }

  private ClusterRouteStats() {
  }

  public RouteStats getRouteStats(
      MetricsRepository metricsRepository,
      String clusterName,
      String instanceUrl,
      RequestType requestType) {
    String combinedKey = clusterName + "-" + instanceUrl + "-" + requestType.toString();
    return perRouteStatMap.computeIfAbsent(combinedKey, ignored -> {
      String instanceName = instanceUrl;
      try {
        URL url = new URL(instanceUrl);
        instanceName = url.getHost();
      } catch (MalformedURLException e) {
        LOGGER.error("Invalid instance url: {}", instanceUrl);
      }
      return new RouteStats(metricsRepository, clusterName, instanceName, requestType);
    });
  }

  /**
   * Per-route request metrics.
   */
  public static class RouteStats extends AbstractVeniceHttpStats {
    private final Sensor requestCountSensor;
    private final Sensor responseWaitingTimeSensor;
    private final Sensor healthyRequestCountSensor;
    private final Sensor quotaExceededRequestCountSensor;
    private final Sensor internalServerErrorRequestCountSensor;
    private final Sensor serviceUnavailableRequestCountSensor;
    private final Sensor leakedRequestCountSensor;
    private final Sensor otherErrorRequestCountSensor;

    public RouteStats(
        MetricsRepository metricsRepository,
        String clusterName,
        String instanceName,
        RequestType requestType) {
      super(metricsRepository, clusterName + "." + StatsUtils.convertHostnameToMetricName(instanceName), requestType);
      this.requestCountSensor = registerSensor("request_count", new OccurrenceRate());
      this.responseWaitingTimeSensor = registerSensor(
          "response_waiting_time",
          TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_waiting_time")));
      this.healthyRequestCountSensor = registerSensor("healthy_request_count", new OccurrenceRate());
      this.quotaExceededRequestCountSensor = registerSensor("quota_exceeded_request_count", new OccurrenceRate());
      this.internalServerErrorRequestCountSensor =
          registerSensor("internal_server_error_request_count", new OccurrenceRate());
      this.serviceUnavailableRequestCountSensor =
          registerSensor("service_unavailable_request_count", new OccurrenceRate());
      this.leakedRequestCountSensor = registerSensor("leaked_request_count", new OccurrenceRate());
      this.otherErrorRequestCountSensor = registerSensor("other_error_request_count", new OccurrenceRate());
    }

    public void recordRequest() {
      requestCountSensor.record();
    }

    public void recordResponseWaitingTime(double latency) {
      responseWaitingTimeSensor.record(latency);
    }

    public void recordHealthyRequest() {
      healthyRequestCountSensor.record();
    }

    public void recordQuotaExceededRequest() {
      quotaExceededRequestCountSensor.record();
    }

    public void recordInternalServerErrorRequest() {
      internalServerErrorRequestCountSensor.record();
    }

    public void recordServiceUnavailableRequest() {
      serviceUnavailableRequestCountSensor.record();
    }

    public void recordLeakedRequest() {
      leakedRequestCountSensor.record();
    }

    public void recordOtherErrorRequest() {
      otherErrorRequestCountSensor.record();
    }
  }
}
