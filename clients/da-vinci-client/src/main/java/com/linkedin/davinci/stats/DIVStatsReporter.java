package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.function.DoubleSupplier;
import java.util.function.Function;


/**
 * This class is the reporting class for stats class {@link DIVStats}.
 * Metrics reporting logics are registered into {@link MetricsRepository} here and send out to external metrics
 * collection/visualization system.
 */
public class DIVStatsReporter extends AbstractVeniceStatsReporter<DIVStats> {
  public DIVStatsReporter(MetricsRepository metricsRepository, String storeName, String clusterName) {
    super(metricsRepository, storeName);
  }

  @Override
  protected void registerStats() {
    registerSensor(new DIVStatsGauge(this, () -> (double) getStats().getDuplicateMsg(), "duplicate_msg"));
    registerSensor(new DIVStatsGauge(this, () -> (double) getStats().getMissingMsg(), "missing_msg"));
    registerSensor(new DIVStatsGauge(this, () -> (double) getStats().getCorruptedMsg(), "corrupted_msg"));
    registerSensor(new DIVStatsGauge(this, () -> (double) getStats().getSuccessMsg(), "success_msg"));
    registerSensor(
        new DIVStatsGauge(
            this,
            () -> (double) getStats().getBenignLeaderOffsetRewindCount(),
            "benign_leader_offset_rewind_count"));
    registerSensor(
        new DIVStatsGauge(
            this,
            () -> (double) getStats().getPotentiallyLossyLeaderOffsetRewindCount(),
            "potentially_lossy_leader_offset_rewind_count"));
    registerSensor(
        new DIVStatsGauge(this, () -> (double) getStats().getLeaderProducerFailure(), "leader_producer_failure_count"));
    registerSensor(
        new DIVStatsGauge(
            this,
            () -> (double) getStats().getBenignLeaderProducerFailure(),
            "benign_leader_producer_failure_count"));
  }

  protected void registerLatencySensor(
      String sensorBaseName,
      Function<DIVStats, WritePathLatencySensor> sensorFunction) {
    registerSensor(
        new DIVStatsGauge(this, () -> sensorFunction.apply(getStats()).getAvg(), sensorBaseName + "_latency_avg_ms"));
    registerSensor(
        new DIVStatsGauge(this, () -> sensorFunction.apply(getStats()).getMax(), sensorBaseName + "_latency_max_ms"));
  }

  protected static class DIVStatsGauge extends AsyncGauge {
    DIVStatsGauge(DIVStatsReporter reporter, DoubleSupplier supplier, String metricName) {
      super(
          (ignored, ignored2) -> (reporter.getStats() == null) ? NULL_DIV_STATS.code : supplier.getAsDouble(),
          metricName);
    }
  }

}
