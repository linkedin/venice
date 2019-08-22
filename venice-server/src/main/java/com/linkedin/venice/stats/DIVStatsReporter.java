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
    registerSensor("producer_to_broker_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerBrokerLatencyAvgMs()));
    registerSensor("producer_to_broker_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerBrokerLatencyMinMs()));
    registerSensor("producer_to_broker_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerBrokerLatencyMaxMs()));
    registerSensor("broker_to_consumer_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getBrokerConsumerLatencyAvgMs()));
    registerSensor("broker_to_consumer_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getBrokerConsumerLatencyMinMs()));
    registerSensor("broker_to_consumer_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getBrokerConsumerLatencyMaxMs()));
    registerSensor("producer_to_consumer_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerConsumerLatencyAvgMs()));
    registerSensor("producer_to_consumer_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerConsumerLatencyMinMs()));
    registerSensor("producer_to_consumer_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerConsumerLatencyMaxMs()));
    registerSensor("benign_leader_offset_rewind_count",
        new DIVStatsCounter(this, () -> (double) getStats().getBenignLeaderOffsetRewindCount()));
    registerSensor("potentially_lossy_leader_offset_rewind_count",
        new DIVStatsCounter(this, () -> (double) getStats().getPotentiallyLossyLeaderOffsetRewindCount()));
    registerSensor("leader_producer_failure_count",
        new DIVStatsCounter(this, () -> (double) getStats().getLeaderProducerFailure()));
  }

  private static class DIVStatsCounter extends Gauge {
    DIVStatsCounter(AbstractVeniceStatsReporter reporter, Supplier<Double> supplier) {
      super(() -> reporter.getStats() == null ? NULL_DIV_STATS.code : supplier.get());
    }
  }
}
