package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Total;


public class HttpClient5ClientStats extends AbstractVeniceStats {
  private final Sensor clientRepairSensor;


  public HttpClient5ClientStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    this.clientRepairSensor = registerSensor("client_repair", new Count(), new Total());
  }

  public void recordClientRepair() {
    clientRepairSensor.record();
  }
}
