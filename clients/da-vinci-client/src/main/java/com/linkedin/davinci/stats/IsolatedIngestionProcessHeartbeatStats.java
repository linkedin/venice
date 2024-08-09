package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.OccurrenceRate;


public class IsolatedIngestionProcessHeartbeatStats extends AbstractVeniceStats {
  private static final String METRICS_PREFIX = "ingestion_isolation_heartbeat";
  // Delay in millis since last successful heartbeat query.
  private final Lazy<Sensor> heartbeatAgeSensor;
  private final Sensor forkedProcessRestartSensor;

  public IsolatedIngestionProcessHeartbeatStats(MetricsRepository metricsRepository) {
    super(metricsRepository, METRICS_PREFIX);
    heartbeatAgeSensor = registerLazySensor("heartbeat_age", new Gauge());
    forkedProcessRestartSensor = registerSensor("forked_process_restart", new OccurrenceRate());
  }

  public void recordHeartbeatAge(long heartbeatAgeInMs) {
    heartbeatAgeSensor.get().record(heartbeatAgeInMs);
  }

  public void recordForkedProcessRestart() {
    forkedProcessRestartSensor.record();
  }
}
