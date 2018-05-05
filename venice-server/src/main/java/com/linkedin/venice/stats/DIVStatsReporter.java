package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import java.util.function.Supplier;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;

public class DIVStatsReporter extends AbstractVeniceStatsReporter<DIVStats> {
  public DIVStatsReporter(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
  }

  @Override
  protected void registerStats() {
    registerSensor("duplicate_msg",
        new DIVStatsCounter(this, () -> (double) getStats().getDuplicateMsg()));
    registerSensor("missing_msg",
        new DIVStatsCounter(this, () -> (double) getStats().getMissingMsg()));
    registerSensor("corrupted_msg",
        new DIVStatsCounter(this, () -> (double) getStats().getCorruptedMsg()));
    registerSensor("success_msg",
        new DIVStatsCounter(this, () -> (double) getStats().getSuccessMsg()));
    registerSensor("current_idle_time",
        new DIVStatsCounter(this, () -> (double) getStats().getCurrentIdleTime()));
    registerSensor("overall_idle_time",
        new DIVStatsCounter(this, () -> (double) getStats().getOverallIdleTime()));
  }

  private static class DIVStatsCounter extends Gauge {
    DIVStatsCounter(AbstractVeniceStatsReporter reporter, Supplier<Double> supplier) {
      super(() -> reporter.getStats() == null ? NULL_DIV_STATS.code : supplier.get());
    }
  }

}
