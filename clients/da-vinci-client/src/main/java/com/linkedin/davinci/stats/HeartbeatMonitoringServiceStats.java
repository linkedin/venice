package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;


public class HeartbeatMonitoringServiceStats extends AbstractVeniceStats {
  private static final String HB_SUFFIX = "-heartbeat-monitor-service";
  private final Sensor heartbeatExceptionCountSensor;
  private final Sensor heartbeatReporterSensor;
  private final Sensor heartbeatLoggingSensor;

  public HeartbeatMonitoringServiceStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, clusterName + HB_SUFFIX);
    heartbeatExceptionCountSensor = registerSensorIfAbsent("heartbeat-monitor-service-exception-count", new Count());
    heartbeatReporterSensor = registerSensorIfAbsent("heartbeat-reporter", new OccurrenceRate());
    heartbeatLoggingSensor = registerSensorIfAbsent("heartbeat-logger", new OccurrenceRate());
  }

  public void recordHeartbeatExceptionCount() {
    heartbeatExceptionCountSensor.record();
  }

  public void recordReporterHeartbeat() {
    heartbeatReporterSensor.record();
  }

  public void recordLoggerHeartbeat() {
    heartbeatLoggingSensor.record();
  }
}
