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

  private static ClientStats instance;

  public static synchronized void init(MetricsRepository metricsRepository) {
    if (metricsRepository == null)
      throw new IllegalArgumentException("metricsRepository is null");

    if (instance == null)
      instance = new ClientStats(metricsRepository, AbstractAvroStoreClient.VENICE_CLIENT_NAME);
  }

  public static ClientStats getInstance() {
    if (instance == null)
      throw new VeniceException("ClientStats has not been initialized yet");

    return instance;
  }

  public ClientStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    requestSensor = registerSensor("request", new SampledCount(), new OccurrenceRate());
    healthySensor = registerSensor("healthy_request", new SampledCount());
    unhealthySensor = registerSensor("unhealthy_request", new SampledCount());
    healthyRequestLatencySensor =
        registerSensor("healthy_request_latency", TehutiUtils.getPercentileStat(getName(), "healthy_request_latency"));
    unhealthyRequestLatencySensor =
        registerSensor("unhealthy_request_latency", TehutiUtils.getPercentileStat(getName(), "unhealthy_request_latency"));
  }

  public void recordRequest() {
    record(requestSensor);
  }

  public void recordHealthyRequest() {
    record(healthySensor);
  }

  public void recordUnhealthyRequest() {
    record(unhealthySensor);
  }

  public void recordHealthyLatency(double latency) {
    record(healthyRequestLatencySensor, latency);
  }

  public void recordUnhealthyLatency(double latency) {
    record(unhealthyRequestLatencySensor, latency);
  }
}
