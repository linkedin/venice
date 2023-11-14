package com.linkedin.venice.router.stats;

import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Count;


public class AdminOperationsStats extends AbstractVeniceStats {
  private final Sensor adminRequestSensor;
  private final Sensor errorAdminRequestSensor;

  public AdminOperationsStats(MetricsRepository metricsRepository, String name, VeniceRouterConfig config) {
    super(metricsRepository, name);
    adminRequestSensor = registerSensorIfAbsent("admin_request", new Count());
    errorAdminRequestSensor = registerSensorIfAbsent("error_admin_request", new Count());

    registerSensorIfAbsent(
        new AsyncGauge(
            (c, t) -> config.isReadThrottlingEnabled() || config.isEarlyThrottleEnabled() ? 1 : 0,
            "read_quota_throttle"));
  }

  public void recordAdminRequest() {
    adminRequestSensor.record();
  }

  public void recordErrorAdminRequest() {
    errorAdminRequestSensor.record();
  }
}
