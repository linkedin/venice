package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;

import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import java.util.function.DoubleSupplier;


public class DIVStatsReporter extends AbstractVeniceStatsReporter<DIVStats> {
  public DIVStatsReporter(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
  }

  @Override
  protected void registerStats() {
    registerSensor("duplicate_msg", new DIVStatsCounter(this, () -> (double) getStats().getDuplicateMsg()));
    registerSensor("missing_msg", new DIVStatsCounter(this, () -> (double) getStats().getMissingMsg()));
    registerSensor("corrupted_msg", new DIVStatsCounter(this, () -> (double) getStats().getCorruptedMsg()));
    registerSensor("success_msg", new DIVStatsCounter(this, () -> (double) getStats().getSuccessMsg()));
    registerSensor("current_idle_time", new DIVStatsCounter(this, () -> (double) getStats().getCurrentIdleTime()));
    registerSensor("overall_idle_time", new DIVStatsCounter(this, () -> (double) getStats().getOverallIdleTime()));
    registerSensor(
        "producer_to_broker_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerBrokerLatencyAvgMs()));
    registerSensor(
        "producer_to_broker_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerBrokerLatencyMinMs()));
    registerSensor(
        "producer_to_broker_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerBrokerLatencyMaxMs()));
    registerSensor(
        "broker_to_consumer_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getBrokerConsumerLatencyAvgMs()));
    registerSensor(
        "broker_to_consumer_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getBrokerConsumerLatencyMinMs()));
    registerSensor(
        "broker_to_consumer_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getBrokerConsumerLatencyMaxMs()));
    registerSensor(
        "producer_to_consumer_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerConsumerLatencyAvgMs()));
    registerSensor(
        "producer_to_consumer_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerConsumerLatencyMinMs()));
    registerSensor(
        "producer_to_consumer_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerConsumerLatencyMaxMs()));
    registerSensor(
        "producer_to_source_broker_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerSourceBrokerLatencyAvgMs()));
    registerSensor(
        "producer_to_source_broker_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerSourceBrokerLatencyMinMs()));
    registerSensor(
        "producer_to_source_broker_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerSourceBrokerLatencyMaxMs()));
    registerSensor(
        "source_broker_to_leader_consumer_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getSourceBrokerLeaderConsumerLatencyAvgMs()));
    registerSensor(
        "source_broker_to_leader_consumer_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getSourceBrokerLeaderConsumerLatencyMinMs()));
    registerSensor(
        "source_broker_to_leader_consumer_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getSourceBrokerLeaderConsumerLatencyMaxMs()));
    registerSensor(
        "producer_to_leader_consumer_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerLeaderConsumerLatencyAvgMs()));
    registerSensor(
        "producer_to_leader_consumer_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerLeaderConsumerLatencyMinMs()));
    registerSensor(
        "producer_to_leader_consumer_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerLeaderConsumerLatencyMaxMs()));
    registerSensor(
        "producer_to_local_broker_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerLocalBrokerLatencyAvgMs()));
    registerSensor(
        "producer_to_local_broker_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerLocalBrokerLatencyMinMs()));
    registerSensor(
        "producer_to_local_broker_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerLocalBrokerLatencyMaxMs()));
    registerSensor(
        "local_broker_to_follower_consumer_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getLocalBrokerFollowerConsumerLatencyAvgMs()));
    registerSensor(
        "local_broker_to_follower_consumer_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getLocalBrokerFollowerConsumerLatencyMinMs()));
    registerSensor(
        "local_broker_to_follower_consumer_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getLocalBrokerFollowerConsumerLatencyMaxMs()));
    registerSensor(
        "producer_to_follower_consumer_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerFollowerConsumerLatencyAvgMs()));
    registerSensor(
        "producer_to_follower_consumer_latency_min_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerFollowerConsumerLatencyMinMs()));
    registerSensor(
        "producer_to_follower_consumer_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getProducerFollowerConsumerLatencyMaxMs()));
    registerSensor(
        "data_validation_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getDataValidationLatencyAvgMs()));
    registerSensor(
        "data_validation_max_ms",
        new DIVStatsCounter(this, () -> getStats().getDataValidationLatencyMaxMs()));
    registerSensor(
        "leader_producer_completion_latency_avg_ms",
        new DIVStatsCounter(this, () -> getStats().getLeaderProducerCompletionLatencyAvgMs()));
    registerSensor(
        "leader_producer_completion_latency_max_ms",
        new DIVStatsCounter(this, () -> getStats().getLeaderProducerCompletionLatencyMaxMs()));
    registerSensor(
        "benign_leader_offset_rewind_count",
        new DIVStatsCounter(this, () -> (double) getStats().getBenignLeaderOffsetRewindCount()));
    registerSensor(
        "potentially_lossy_leader_offset_rewind_count",
        new DIVStatsCounter(this, () -> (double) getStats().getPotentiallyLossyLeaderOffsetRewindCount()));
    registerSensor(
        "leader_producer_failure_count",
        new DIVStatsCounter(this, () -> (double) getStats().getLeaderProducerFailure()));
    registerSensor(
        "benign_leader_producer_failure_count",
        new DIVStatsCounter(this, () -> (double) getStats().getBenignLeaderProducerFailure()));

  }

  private static class DIVStatsCounter extends Gauge {
    DIVStatsCounter(AbstractVeniceStatsReporter reporter, DoubleSupplier supplier) {
      super(() -> reporter.getStats() == null ? NULL_DIV_STATS.code : supplier.getAsDouble());
    }
  }
}
