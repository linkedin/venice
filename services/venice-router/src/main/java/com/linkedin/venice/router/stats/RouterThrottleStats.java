package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class RouterThrottleStats extends AbstractVeniceStats {
  private final Sensor routerThrottleSensor;

  public RouterThrottleStats(MetricsRepository repository, String name) {
    super(repository, name);
    routerThrottleSensor = registerSensor("router_throttled_request", new Count());

  }

  public void recordRouterThrottledRequest() {
    routerThrottleSensor.record();
  }
}
