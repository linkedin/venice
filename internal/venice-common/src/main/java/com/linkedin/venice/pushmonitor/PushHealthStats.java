package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;


public class PushHealthStats extends AbstractVeniceStats {
  private final Sensor successfulPushDurationSensor;
  private final Sensor failedPushDurationSensor;
  private final Sensor pushPreparationDurationSensor;

  private final Sensor successfulPushDurationSensorGauge;

  public PushHealthStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
    failedPushDurationSensor = registerSensorIfAbsent("failed_push_duration_sec", new Avg(), new Max());
    successfulPushDurationSensor = registerSensorIfAbsent("successful_push_duration_sec", new Avg(), new Max());
    pushPreparationDurationSensor = registerSensorIfAbsent("push_preparation_duration_sec", new Avg(), new Max());
    successfulPushDurationSensorGauge = registerSensorIfAbsent("successful_push_duration_sec_gauge", new Gauge());
  }

  public void recordFailedPush(long durationInSec) {
    failedPushDurationSensor.record(durationInSec);
  }

  public void recordSuccessfulPush(long durationInSec) {
    successfulPushDurationSensor.record(durationInSec);
  }

  public void recordPushPreparationDuration(long durationInSec) {
    pushPreparationDurationSensor.record(durationInSec);
  }

  public void recordSuccessfulPushGauge(long durationInSec) {
    successfulPushDurationSensorGauge.record(durationInSec);
  }
}
