package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class HealthCheckStats extends AbstractVeniceStats {
  private final Sensor healthCheckRequestSensor;
  private final Sensor errorHealthCheckRequestSensor;

  public HealthCheckStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    healthCheckRequestSensor = registerSensor("healthcheck_request", new Count());
    errorHealthCheckRequestSensor = registerSensor("error_healthcheck_request", new Count());
  }

  public void recordHealthCheck() {
    healthCheckRequestSensor.record();
  }

  public void recordErrorHealthCheck() {
    errorHealthCheckRequestSensor.record();
  }
}
