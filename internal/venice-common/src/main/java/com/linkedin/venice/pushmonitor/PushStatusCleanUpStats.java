package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Rate;


public class PushStatusCleanUpStats extends AbstractVeniceStats {
  private final Sensor successfulLeakedPushStatusCleanUpCountSensor;
  private final Sensor failedLeakedPushStatusCleanUpCountSensor;
  private final Sensor leakedPushStatusCleanUpServiceStateSensor;
  private final Sensor leakedPushStatusCountSensor;

  public PushStatusCleanUpStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
    leakedPushStatusCountSensor = registerSensorIfAbsent("leaked_push_status_count", new Gauge());
    failedLeakedPushStatusCleanUpCountSensor =
        registerSensorIfAbsent("failed_leaked_push_status_clean_up_count", new Rate());
    successfulLeakedPushStatusCleanUpCountSensor =
        registerSensorIfAbsent("successful_leaked_push_status_clean_up_count", new Rate());
    leakedPushStatusCleanUpServiceStateSensor =
        registerSensorIfAbsent("leaked_push_status_clean_up_service_state", new Gauge());
  }

  public void recordLeakedPushStatusCount(int count) {
    leakedPushStatusCountSensor.record(count);
  }

  public void recordSuccessfulLeakedPushStatusCleanUpCount(int count) {
    successfulLeakedPushStatusCleanUpCountSensor.record(count);
  }

  public void recordFailedLeakedPushStatusCleanUpCount(int count) {
    failedLeakedPushStatusCleanUpCountSensor.record(count);
  }

  /**
   * 3 possible metric values:
   * 1: the leaked push status clean-up service is up and running
   * 0: the leaked push status clean-up service is stopped gracefully or stopped by InterruptException
   * -1: the leaked push status clean-up service crashes while the Venice service is still running
   */
  public void recordLeakedPushStatusCleanUpServiceState(PushStatusCleanUpServiceState state) {
    leakedPushStatusCleanUpServiceStateSensor.record(state.getValue());
  }
}
