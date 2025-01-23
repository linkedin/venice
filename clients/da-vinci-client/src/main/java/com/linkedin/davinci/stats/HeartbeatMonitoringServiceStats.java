package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class HeartbeatMonitoringServiceStats extends AbstractVeniceStats {
  private static final String HB_SUFFIX = "-heartbeat-monitor-service";
  private final Sensor heartbeatExceptionCountSensor;

  public HeartbeatMonitoringServiceStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, clusterName + HB_SUFFIX);
    heartbeatExceptionCountSensor = registerSensorIfAbsent("heartbeat-monitor-service-exception-count", new Count());
  }

  public void recordHeartbeatExceptionCountSensor() {
    heartbeatExceptionCountSensor.record();
  }

}
