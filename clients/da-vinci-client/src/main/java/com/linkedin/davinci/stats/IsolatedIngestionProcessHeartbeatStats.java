package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.OccurrenceRate;


public class IsolatedIngestionProcessHeartbeatStats extends AbstractVeniceStats {
  private static final String METRICS_PREFIX = "ingestion_isolation_heartbeat";
  // Delay in millis since last successful heartbeat query.
  private final Sensor heartbeatAgeSensor;
  private final Sensor forkedProcessRestartSensor;

  public IsolatedIngestionProcessHeartbeatStats(MetricsRepository metricsRepository) {
    super(metricsRepository, METRICS_PREFIX);
    heartbeatAgeSensor = registerSensor("heartbeat_age", new Gauge());
    forkedProcessRestartSensor = registerSensor("forked_process_restart", new OccurrenceRate());
  }

  public void recordHeartbeatAge(long heartbeatAgeInMs) {
    heartbeatAgeSensor.record(heartbeatAgeInMs);
  }

  public void recordForkedProcessRestart() {
    forkedProcessRestartSensor.record();
  }
}
