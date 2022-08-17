package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Total;


public class SparkServerStats extends AbstractVeniceStats {
  final private Sensor requests;
  final private Sensor finishedRequests;
  final private Sensor successfulRequest;
  final private Sensor failedRequest;
  final private Sensor successfulRequestLatency;
  final private Sensor failedRequestLatency;
  final private Sensor currentInFlightRequestTotal;

  public SparkServerStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    requests = registerSensor("request", new Count(), new OccurrenceRate());
    finishedRequests = registerSensor("finished_request", new Count(), new OccurrenceRate());
    currentInFlightRequestTotal = registerSensor("current_in_flight_request", new Total());
    successfulRequest = registerSensor("successful_request", new Count());
    failedRequest = registerSensor("failed_request", new Count());
    successfulRequestLatency = registerSensor(
        "successful_request_latency",
        TehutiUtils.getPercentileStat(getName(), "successful_request_latency"));
    failedRequestLatency =
        registerSensor("failed_request_latency", TehutiUtils.getPercentileStat(getName(), "failed_request_latency"));
  }

  public void recordRequest() {
    requests.record();
    currentInFlightRequestTotal.record(1);
  }

  public void recordSuccessfulRequestLatency(double latency) {
    finishRequest();
    successfulRequest.record();
    successfulRequestLatency.record(latency);
  }

  public void recordFailedRequestLatency(double latency) {
    finishRequest();
    failedRequest.record();
    failedRequestLatency.record(latency);
  }

  private void finishRequest() {
    finishedRequests.record();
    currentInFlightRequestTotal.record(-1);
  }
}
