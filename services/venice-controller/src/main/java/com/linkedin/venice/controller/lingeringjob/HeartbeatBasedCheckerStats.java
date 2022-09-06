package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class HeartbeatBasedCheckerStats extends AbstractVeniceStats {
  private static final String STATS_NAME = "controller-batch-job-heartbeat-checker";

  // Error sensors
  private final Sensor checkJobHasHeartbeatFailedSensor;

  // Happy path sensors
  private final Sensor timeoutHeartbeatCheckSensor;
  private final Sensor noTimeoutHeartbeatCheckSensor;

  public HeartbeatBasedCheckerStats(MetricsRepository metricsRepository) {
    super(metricsRepository, STATS_NAME);
    checkJobHasHeartbeatFailedSensor = registerSensor("check_job_has_heartbeat_failed", new Count());
    timeoutHeartbeatCheckSensor = registerSensor("timeout_heartbeat_check", new Count());
    noTimeoutHeartbeatCheckSensor = registerSensor("non_timeout_heartbeat_check", new Count());
  }

  void recordCheckJobHasHeartbeatFailed() {
    checkJobHasHeartbeatFailedSensor.record();
  }

  void recordTimeoutHeartbeatCheck() {
    timeoutHeartbeatCheckSensor.record();
  }

  void recordNoTimeoutHeartbeatCheck() {
    noTimeoutHeartbeatCheckSensor.record();
  }
}
