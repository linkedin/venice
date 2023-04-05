package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import java.util.function.DoubleSupplier;
import java.util.function.Function;


/**
 * This class is the reporting class for stats class {@link DIVStats}.
 * Metrics reporting logics are registered into {@link MetricsRepository} here and send out to external metrics
 * collection/visualization system.
 */
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

    // This prevents user store system store to register latency related DIV metric sensors.
    if (!VeniceSystemStoreUtils.isUserSystemStore(storeName)) {
      registerLatencySensor("producer_to_broker", DIVStats::getProducerBrokerLatencySensor);
      registerLatencySensor("broker_to_consumer", DIVStats::getBrokerConsumerLatencySensor);
      registerLatencySensor("producer_to_consumer", DIVStats::getProducerConsumerLatencySensor);
      registerLatencySensor("producer_to_source_broker", DIVStats::getProducerSourceBrokerLatencySensor);
      registerLatencySensor("source_broker_to_leader_consumer", DIVStats::getSourceBrokerLeaderConsumerLatencySensor);
      registerLatencySensor("producer_to_leader_consumer", DIVStats::getProducerLeaderConsumerLatencySensor);
      registerLatencySensor("producer_to_local_broker", DIVStats::getProducerLocalBrokerLatencySensor);
      registerLatencySensor("local_broker_to_follower_consumer", DIVStats::getLocalBrokerFollowerConsumerLatencySensor);
      registerLatencySensor("producer_to_follower_consumer", DIVStats::getProducerFollowerConsumerLatencySensor);
      registerLatencySensor("leader_producer_completion", DIVStats::getLeaderProducerCompletionLatencySensor);
    }
  }

  protected void registerLatencySensor(
      String sensorBaseName,
      Function<DIVStats, WritePathLatencySensor> sensorFunction) {
    registerSensor(
        sensorBaseName + "_latency_avg_ms",
        new DIVStatsCounter(this, () -> sensorFunction.apply(getStats()).getAvg()));
    registerSensor(
        sensorBaseName + "_latency_max_ms",
        new DIVStatsCounter(this, () -> sensorFunction.apply(getStats()).getMax()));
  }

  private static class DIVStatsCounter extends Gauge {
    DIVStatsCounter(DIVStatsReporter reporter, DoubleSupplier supplier) {
      super(() -> (reporter.getStats() == null) ? NULL_DIV_STATS.code : supplier.getAsDouble());
    }
  }

}
