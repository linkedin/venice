package com.linkedin.venice.controller.stats;


import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.SampledCount;

public class ControllerStats extends AbstractVeniceStats {
  final private Sensor requestSensor;
  final private Sensor successfulRequest;
  final private Sensor failedRequest;
  final private Sensor successfulRequestLatencySensor;
  final private Sensor failedRequestLatency;
  final private Sensor adminConsumeFailCount;

  private static ControllerStats instance;

  public static synchronized void init(MetricsRepository metricsRepository) {
    if (metricsRepository == null) {
      throw new IllegalArgumentException("metricsRepository is null");
    }
    if (instance == null) {
      instance = new ControllerStats(metricsRepository, "total");
    }
  }

  public static ControllerStats getInstance() {
    if (instance == null) {
      throw new VeniceException("ControllerStats has not been initialized yet");
    }
    return instance;
  }

  public ControllerStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    requestSensor = registerSensor("request", new SampledCount(), new OccurrenceRate());
    successfulRequest = registerSensor("successful_request", new SampledCount());
    failedRequest = registerSensor("failed_request", new SampledCount());
    successfulRequestLatencySensor = registerSensor("successful_request_latency",
      TehutiUtils.getPercentileStat(getName() + "_" + "successful_request_latency"));
    failedRequestLatency = registerSensor("failed_request_latency",
      TehutiUtils.getPercentileStat(getName() + "_" + "failed_request_latency"));
    adminConsumeFailCount = registerSensor("failed_admin_messages", new Max());
  }

  public void recordRequest() {
    record(requestSensor);
  }

  public void recordSuccessfulRequest() {
    record(successfulRequest);
  }

  public void recordFailedRequest() {
    record(failedRequest);
  }

  public void recordSuccessfulRequestLatency(double latency) {
    record(successfulRequestLatencySensor, latency);
  }

  public void recordFailedRequestLatency(double latency) {record(failedRequestLatency, latency);}

  /**
   * @param retryCount the number of times that a failure has consecutively triggered a retry
   */
  public void recordFailedAdminConsumption(double retryCount) {
    record(adminConsumeFailCount, retryCount);
  }
}