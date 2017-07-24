package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import java.util.function.Supplier;


public class DIVStatsReporter extends AbstractVeniceStats {
  private DIVStats stats;

  public DIVStatsReporter(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);

    registerSensor("duplicate_msg",
        new DIVStatsCounter(this, () -> stats.getDuplicateMsg()));
    registerSensor("missing_msg",
        new DIVStatsCounter(this, () -> stats.getMissingMsg()));
    registerSensor("corrupted_msg",
        new DIVStatsCounter(this, () -> stats.getCorruptedMsg()));
    registerSensor("success_msg",
        new DIVStatsCounter(this, () -> stats.getSuccessMsg()));
    registerSensor("current_idle_time",
        new DIVStatsCounter(this, () -> stats.getCurrentIdleTime()));
    registerSensor("overall_idle_time",
        new DIVStatsCounter(this, () -> stats.getOverallIdleTime()));
  }

  public void setDIVStats(DIVStats stats) {
    this.stats = stats;
  }

  public DIVStats getDIVStats() {
    return stats;
  }

  private static class DIVStatsCounter extends LambdaStat {
    DIVStatsCounter(DIVStatsReporter divStatsReporter, Supplier<Long> supplier) {
      super(() -> divStatsReporter.getDIVStats() == null ? 0d : supplier.get());
    }
  }
}
