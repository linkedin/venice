package com.linkedin.venice.client.stats;

import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.SampledCount;

public class ClientStats extends AbstractVeniceStats {
  private final Sensor requestSensor;
  private final Sensor healthySensor;
  private final Sensor unhealthySensor;
  private final Sensor healthyRequestLatencySensor;
  private final Sensor unhealthyRequestLatencySensor;

  public ClientStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);

    requestSensor = registerSensor("request", new SampledCount(), new OccurrenceRate());
    healthySensor = registerSensor("healthy_request", new SampledCount());
    unhealthySensor = registerSensor("unhealthy_request", new SampledCount());
    healthyRequestLatencySensor =
        registerSensor("healthy_request_latency", TehutiUtils.getPercentileStat(getName(), "healthy_request_latency"));
    unhealthyRequestLatencySensor =
        registerSensor("unhealthy_request_latency", TehutiUtils.getPercentileStat(getName(), "unhealthy_request_latency"));
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

  public void recordHealthyLatency(double latency) {
    healthyRequestLatencySensor.record(latency);
  }

  public void recordUnhealthyLatency(double latency) {
    unhealthyRequestLatencySensor.record(latency);
  }
}
