package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;


public class PushHealthStats extends AbstractVeniceStats {
  private final Sensor failedPushSensor;
  private final Sensor successfulPushSensor;
  private final Sensor successfulPushDurationSensor;
  private final Sensor failedPushDurationSensor;
  private final Sensor pushPreparationDurationSensor;

  public PushHealthStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
    synchronized (PushHealthStats.class) {

      failedPushSensor =
          getSensorIfPresent("failed_push", () -> registerSensor("failed_push", new Count()));
      successfulPushSensor = getSensorIfPresent("successful_push",
          () -> registerSensor("successful_push", new Count()));

      failedPushDurationSensor = getSensorIfPresent("failed_push_duration_sec",
          () -> registerSensor("failed_push_duration_sec", new Avg(), new Max()));
      successfulPushDurationSensor = getSensorIfPresent("successful_push_duration_sec",
          () -> registerSensor("successful_push_duration_sec", new Avg(), new Max()));
      pushPreparationDurationSensor = getSensorIfPresent("push_preparation_duration_sec",
          () -> registerSensor("push_preparation_duration_sec", new Avg(), new Max()));
    }
  }

  public void recordFailedPush(long durationInSec) {
    failedPushSensor.record();
    failedPushDurationSensor.record(durationInSec);
  }

  public void recordSuccessfulPush(long durationInSec) {
    successfulPushSensor.record();
    successfulPushDurationSensor.record(durationInSec);
  }

  public void recordPushPreparationDuration(long durationInSec){
    pushPreparationDurationSensor.record(durationInSec);
  }
}
