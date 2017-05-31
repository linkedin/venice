package com.linkedin.venice.client.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ClientStats extends AbstractVeniceStats {
  public enum RequestType {
    SINGLE_GET(""),
    MULTI_GET("multiget_");

    private String metricPrefix;

    RequestType(String metricPrefix) {
      this.metricPrefix = metricPrefix;
    }

    public String getMetricPrefix() {
      return this.metricPrefix;
    }
  };

  private final Sensor requestSensor;
  private final Sensor healthySensor;
  private final Sensor unhealthySensor;
  private final Sensor healthyRequestLatencySensor;
  private final Sensor unhealthyRequestLatencySensor;
  private final Map<Integer, Sensor> httpStatusSensorMap = new ConcurrentHashMap<>();
  private final String metricPrefix;

  public ClientStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    super(metricsRepository, storeName);
    metricPrefix = requestType.getMetricPrefix();

    requestSensor = registerSensor(metricPrefix + "request", new OccurrenceRate());
    healthySensor = registerSensor(metricPrefix + "healthy_request", new OccurrenceRate());
    unhealthySensor = registerSensor(metricPrefix + "unhealthy_request", new OccurrenceRate());
    healthyRequestLatencySensor = registerSensor(metricPrefix + "healthy_request_latency",
        TehutiUtils.getPercentileStat(getName(), metricPrefix + "healthy_request_latency"));
    unhealthyRequestLatencySensor = registerSensor(metricPrefix + "unhealthy_request_latency",
        TehutiUtils.getPercentileStat(getName(), metricPrefix + "unhealthy_request_latency"));
  }

  public void recordRequest() {
    requestSensor.record();
  }

  public void recordHealthyRequest() {
    recordRequest();
    healthySensor.record();
  }

  public void recordUnhealthyRequest() {
    recordRequest();
    unhealthySensor.record();
  }

  public void recordHttpRequest(int httpStatus) {
    httpStatusSensorMap.computeIfAbsent(httpStatus,
        status -> registerSensor(metricPrefix + "http_" + httpStatus + "_request", new OccurrenceRate()))
    .record();
  }

  public void recordHealthyLatency(double latency) {
    healthyRequestLatencySensor.record(latency);
  }

  public void recordUnhealthyLatency(double latency) {
    unhealthyRequestLatencySensor.record(latency);
  }
}
