package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Total;


public class ErrorPartitionStats extends AbstractVeniceStats {
  private final Sensor currentVersionErrorPartitionResetAttempt;
  private final Sensor currentVersionErrorPartitionResetAttemptErrored;
  private final Sensor currentVersionErrorPartitionRecoveredFromReset;
  private final Sensor currentVersionErrorPartitionUnrecoverableFromReset;
  private final Sensor errorPartitionProcessingTime;

  public ErrorPartitionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    currentVersionErrorPartitionResetAttempt =
        registerSensorIfAbsent("current_version_error_partition_reset_attempt", new Total());
    currentVersionErrorPartitionResetAttemptErrored =
        registerSensorIfAbsent("current_version_error_partition_reset_attempt_errored", new Count());
    currentVersionErrorPartitionRecoveredFromReset =
        registerSensorIfAbsent("current_version_error_partition_recovered_from_reset", new Total());
    currentVersionErrorPartitionUnrecoverableFromReset =
        registerSensorIfAbsent("current_version_error_partition_unrecoverable_from_reset", new Total());
    errorPartitionProcessingTime = registerSensorIfAbsent("error_partition_processing_time", new Avg(), new Max());
  }

  public void recordErrorPartitionResetAttempt(double value) {
    currentVersionErrorPartitionResetAttempt.record(value);
  }

  public void recordErrorPartitionResetAttemptErrored() {
    currentVersionErrorPartitionResetAttemptErrored.record();
  }

  public void recordErrorPartitionRecoveredFromReset() {
    currentVersionErrorPartitionRecoveredFromReset.record();
  }

  public void recordErrorPartitionUnrecoverableFromReset() {
    currentVersionErrorPartitionUnrecoverableFromReset.record();
  }

  public void recordErrorPartitionProcessingTime(double value) {
    errorPartitionProcessingTime.record(value);
  }
}
