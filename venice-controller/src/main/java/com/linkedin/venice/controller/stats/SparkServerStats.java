package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;


public class SparkServerStats extends AbstractVeniceStats {
  final private Sensor requestSensor;
  final private Sensor successfulRequest;
  final private Sensor failedRequest;
  final private Sensor successfulRequestLatencySensor;
  final private Sensor failedRequestLatency;

  public SparkServerStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    requestSensor = registerSensor("request", new Count(), new OccurrenceRate());
    successfulRequest = registerSensor("successful_request", new Count());
    failedRequest = registerSensor("failed_request", new Count());
    successfulRequestLatencySensor = registerSensor("successful_request_latency",
        TehutiUtils.getPercentileStat(getName(), "successful_request_latency"));
    failedRequestLatency = registerSensor("failed_request_latency",
        TehutiUtils.getPercentileStat(getName(), "failed_request_latency"));
  }

  public void recordRequest() {
    requestSensor.record();
  }

  public void recordSuccessfulRequest() {
    successfulRequest.record();
  }

  public void recordFailedRequest() {
    failedRequest.record();
  }

  public void recordSuccessfulRequestLatency(double latency) {
    successfulRequestLatencySensor.record();
  }

  public void recordFailedRequestLatency(double latency) {
    failedRequestLatency.record();
  }
}