package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.CountSinceLastMeasurement;


public class PushJobStatusStats extends AbstractVeniceStats {
  private final Sensor batchPushSuccessSensor;
  private final Sensor batchPushFailureDueToUserErrorSensor;
  private final Sensor batchPushFailureDueToNonUserErrorSensor;
  private final Sensor incrementalPushSuccessSensor;
  private final Sensor incrementalPushFailureDueToUserErrorSensor;
  private final Sensor incrementalPushFailureDueToNonUserErrorSensor;

  public PushJobStatusStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    batchPushSuccessSensor =
        registerSensorIfAbsent("batch_push_job_success", new Count(), new CountSinceLastMeasurement());
    batchPushFailureDueToUserErrorSensor =
        registerSensorIfAbsent("batch_push_job_failed_user_error", new Count(), new CountSinceLastMeasurement());
    batchPushFailureDueToNonUserErrorSensor =
        registerSensorIfAbsent("batch_push_job_failed_non_user_error", new Count(), new CountSinceLastMeasurement());
    incrementalPushSuccessSensor =
        registerSensorIfAbsent("incremental_push_job_success", new Count(), new CountSinceLastMeasurement());
    incrementalPushFailureDueToUserErrorSensor =
        registerSensorIfAbsent("incremental_push_job_failed_user_error", new Count(), new CountSinceLastMeasurement());
    incrementalPushFailureDueToNonUserErrorSensor = registerSensorIfAbsent(
        "incremental_push_job_failed_non_user_error",
        new Count(),
        new CountSinceLastMeasurement());
  }

  // record all metrics
  public void recordBatchPushSuccessSensor() {
    batchPushSuccessSensor.record();
  }

  public void recordBatchPushFailureDueToUserErrorSensor() {
    batchPushFailureDueToUserErrorSensor.record();
  }

  public void recordBatchPushFailureNotDueToUserErrorSensor() {
    batchPushFailureDueToNonUserErrorSensor.record();
  }

  public void recordIncrementalPushSuccessSensor() {
    incrementalPushSuccessSensor.record();
  }

  public void recordIncrementalPushFailureDueToUserErrorSensor() {
    incrementalPushFailureDueToUserErrorSensor.record();
  }

  public void recordIncrementalPushFailureNotDueToUserErrorSensor() {
    incrementalPushFailureDueToNonUserErrorSensor.record();
  }
}
