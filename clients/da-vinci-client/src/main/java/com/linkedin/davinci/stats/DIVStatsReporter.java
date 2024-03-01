package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
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
  public DIVStatsReporter(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
  }

  @Override
  protected void registerStats() {
    registerSensor(new DIVStatsCounter("duplicate_msg", this, () -> (double) getStats().getDuplicateMsg()));
    registerSensor(new DIVStatsCounter("missing_msg", this, () -> (double) getStats().getMissingMsg()));
    registerSensor(new DIVStatsCounter("corrupted_msg", this, () -> (double) getStats().getCorruptedMsg()));
    registerSensor(new DIVStatsCounter("success_msg", this, () -> (double) getStats().getSuccessMsg()));
    registerSensor(
        new DIVStatsCounter(
            "benign_leader_offset_rewind_count",
            this,
            () -> (double) getStats().getBenignLeaderOffsetRewindCount()));
    registerSensor(
        new DIVStatsCounter(
            "potentially_lossy_leader_offset_rewind_count",
            this,
            () -> (double) getStats().getPotentiallyLossyLeaderOffsetRewindCount()));
    registerSensor(
        new DIVStatsCounter(
            "leader_producer_failure_count",
            this,
            () -> (double) getStats().getLeaderProducerFailure()));
    registerSensor(
        new DIVStatsCounter(
            "benign_leader_producer_failure_count",
            this,
            () -> (double) getStats().getBenignLeaderProducerFailure()));

    // This prevents user store system store to register latency related DIV metric sensors.
    if (!VeniceSystemStoreUtils.isUserSystemStore(storeName)) {
      registerLatencySensor("producer_to_source_broker", DIVStats::getProducerSourceBrokerLatencySensor);
      registerLatencySensor("source_broker_to_leader_consumer", DIVStats::getSourceBrokerLeaderConsumerLatencySensor);
      registerLatencySensor("producer_to_leader_consumer", DIVStats::getProducerLeaderConsumerLatencySensor);
      registerLatencySensor("producer_to_local_broker", DIVStats::getProducerLocalBrokerLatencySensor);
      registerLatencySensor("local_broker_to_follower_consumer", DIVStats::getLocalBrokerFollowerConsumerLatencySensor);
      registerLatencySensor("producer_to_follower_consumer", DIVStats::getProducerFollowerConsumerLatencySensor);
      registerLatencySensor("leader_producer_completion", DIVStats::getLeaderProducerCompletionLatencySensor);
      registerLatencySensor("leader_div_completion", DIVStats::getLeaderDIVCompletionLatencySensor);
    }
  }

  protected void registerLatencySensor(
      String sensorBaseName,
      Function<DIVStats, WritePathLatencySensor> sensorFunction) {
    registerSensor(
        new DIVStatsCounter(sensorBaseName + "_latency_avg_ms", this, () -> sensorFunction.apply(getStats()).getAvg()));
    registerSensor(
        new DIVStatsCounter(sensorBaseName + "_latency_max_ms", this, () -> sensorFunction.apply(getStats()).getMax()));
  }

  protected static class DIVStatsCounter extends AsyncGauge {
    DIVStatsCounter(String metricName, DIVStatsReporter reporter, DoubleSupplier supplier) {
      super(
          (ignored, ignored2) -> (reporter.getStats() == null) ? NULL_DIV_STATS.code : supplier.getAsDouble(),
          metricName);
    }
  }

}
