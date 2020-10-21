package com.linkedin.davinci.client;

import com.linkedin.venice.stats.AbstractVeniceStats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;

public class ClientStats extends AbstractVeniceStats {
  private final Sensor unhealthyRequestSensor;

  public ClientStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
    unhealthyRequestSensor = registerSensor("unhealthy_request", new OccurrenceRate());
  }

  public void recordUnhealthyRequest() {
    unhealthyRequestSensor.record();
  }
}
