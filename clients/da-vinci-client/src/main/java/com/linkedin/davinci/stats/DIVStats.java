package com.linkedin.davinci.stats;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.atomic.LongAdder;


/**
 * This class contains stats for DIV. The stat class is used in {@link VeniceVersionedStats} to serve for
 * a single store version or total of all store versions.
 * This class does not contain reporting logic as reporting is done by the {@link DIVStatsReporter}.
 */
public class DIVStats {
  private final MetricConfig metricConfig = new MetricConfig();
  private final WritePathLatencySensor producerBrokerLatencySensor;
  private final WritePathLatencySensor brokerConsumerLatencySensor;
  private final WritePathLatencySensor producerConsumerLatencySensor;
  private final WritePathLatencySensor producerSourceBrokerLatencySensor;
  private final WritePathLatencySensor sourceBrokerLeaderConsumerLatencySensor;
  private final WritePathLatencySensor producerLeaderConsumerLatencySensor;
  private final WritePathLatencySensor producerLocalBrokerLatencySensor;
  private final WritePathLatencySensor localBrokerFollowerConsumerLatencySensor;
  private final WritePathLatencySensor producerFollowerConsumerLatencySensor;
  private final WritePathLatencySensor leaderProducerCompletionLatencySensor;
  private final LongAdder duplicateMsg = new LongAdder();
  private final LongAdder successMsg = new LongAdder();

  private long benignLeaderOffsetRewindCount = 0;
  private long potentiallyLossyLeaderOffsetRewindCount = 0;
  private long leaderProducerFailureCount = 0;
  private long benignLeaderProducerFailureCount = 0;
  private long missingMsg = 0;
  private long corruptedMsg = 0;

  public DIVStats() {
    /**
     * For currentTimeMs, latency sensors will still be created as store name is not passed down to this class, and we cannot
     * determine whether latency sensors are needed.
     * {@link DIVStatsReporter} already blocks latency metrics for system stores from being reported to external
     * metric collecting system.
     */
    /**
     * Creating this separate local metric repository only to utilize the sensor library and not for reporting.
     */
    MetricsRepository localRepository = new MetricsRepository(metricConfig);

    // TODO Remove the three metrics below when NR is fully rolled out, since these metrics only apply to non-NR.
    producerBrokerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "producer_to_broker_latency");
    brokerConsumerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "broker_to_consumer_latency");
    producerConsumerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "producer_to_consumer_latency");

    // NR metrics below:
    producerSourceBrokerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "producer_to_source_broker_latency");
    sourceBrokerLeaderConsumerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "source_broker_to_leader_consumer_latency");
    producerLeaderConsumerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "producer_to_leader_consumer_latency");
    producerLocalBrokerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "producer_to_local_broker_latency");
    localBrokerFollowerConsumerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "local_broker_to_follower_consumer_latency");
    producerFollowerConsumerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "producer_to_follower_consumer_latency");
    leaderProducerCompletionLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "leader_producer_completion_latency");
  }

  public long getDuplicateMsg() {
    return this.duplicateMsg.sum();
  }

  public void recordDuplicateMsg() {
    this.duplicateMsg.increment();
  }

  public void setDuplicateMsg(long count) {
    this.duplicateMsg.reset();
    this.duplicateMsg.add(count);
  }

  public synchronized long getMissingMsg() {
    return missingMsg;
  }

  public synchronized void recordMissingMsg() {
    missingMsg += 1;
  }

  public synchronized void setMissingMsg(long count) {
    this.missingMsg = count;
  }

  public synchronized long getCorruptedMsg() {
    return corruptedMsg;
  }

  public synchronized void recordCorruptedMsg() {
    corruptedMsg += 1;
  }

  public synchronized void setCorruptedMsg(long count) {
    this.corruptedMsg = count;
  }

  public synchronized long getSuccessMsg() {
    return successMsg.sum();
  }

  public void recordSuccessMsg() {
    this.successMsg.increment();
  }

  public void setSuccessMsg(long count) {
    this.successMsg.reset();
    this.successMsg.add(count);
  }

  public void recordProducerBrokerLatencyMs(double value, long currentTimeMs) {
    producerBrokerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getProducerBrokerLatencySensor() {
    return producerBrokerLatencySensor;
  }

  public WritePathLatencySensor getProducerSourceBrokerLatencySensor() {
    return producerSourceBrokerLatencySensor;
  }

  public void recordBrokerConsumerLatencyMs(double value, long currentTimeMs) {
    brokerConsumerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getBrokerConsumerLatencySensor() {
    return brokerConsumerLatencySensor;
  }

  public void recordProducerConsumerLatencyMs(double value, long currentTimeMs) {
    producerConsumerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getProducerConsumerLatencySensor() {
    return producerConsumerLatencySensor;
  }

  public void recordProducerSourceBrokerLatencyMs(double value, long currentTimeMs) {
    producerSourceBrokerLatencySensor.record(value, currentTimeMs);
  }

  public void recordSourceBrokerLeaderConsumerLatencyMs(double value, long currentTimeMs) {
    sourceBrokerLeaderConsumerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getSourceBrokerLeaderConsumerLatencySensor() {
    return sourceBrokerLeaderConsumerLatencySensor;
  }

  public void recordProducerLeaderConsumerLatencyMs(double value, long currentTimeMs) {
    producerLeaderConsumerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getProducerLeaderConsumerLatencySensor() {
    return producerLeaderConsumerLatencySensor;
  }

  public void recordProducerLocalBrokerLatencyMs(double value, long currentTimeMs) {
    producerLocalBrokerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getProducerLocalBrokerLatencySensor() {
    return producerLocalBrokerLatencySensor;
  }

  public void recordLocalBrokerFollowerConsumerLatencyMs(double value, long currentTimeMs) {
    localBrokerFollowerConsumerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getLocalBrokerFollowerConsumerLatencySensor() {
    return localBrokerFollowerConsumerLatencySensor;
  }

  public void recordProducerFollowerConsumerLatencyMs(double value, long currentTimeMs) {
    producerFollowerConsumerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getProducerFollowerConsumerLatencySensor() {
    return producerFollowerConsumerLatencySensor;
  }

  public void recordLeaderProducerCompletionLatencyMs(double value, long currentTimeMs) {
    leaderProducerCompletionLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getLeaderProducerCompletionLatencySensor() {
    return leaderProducerCompletionLatencySensor;
  }

  public synchronized void recordBenignLeaderOffsetRewind() {
    benignLeaderOffsetRewindCount += 1;
  }

  public synchronized long getBenignLeaderOffsetRewindCount() {
    return benignLeaderOffsetRewindCount;
  }

  public synchronized void setBenignLeaderOffsetRewindCount(long count) {
    this.benignLeaderOffsetRewindCount = count;
  }

  public synchronized void recordPotentiallyLossyLeaderOffsetRewind() {
    potentiallyLossyLeaderOffsetRewindCount += 1;
  }

  public synchronized long getPotentiallyLossyLeaderOffsetRewindCount() {
    return potentiallyLossyLeaderOffsetRewindCount;
  }

  public synchronized void setPotentiallyLossyLeaderOffsetRewindCount(long count) {
    this.potentiallyLossyLeaderOffsetRewindCount = count;
  }

  public synchronized void recordLeaderProducerFailure() {
    leaderProducerFailureCount += 1;
  }

  public synchronized long getLeaderProducerFailure() {
    return leaderProducerFailureCount;
  }

  public synchronized void setLeaderProducerFailure(long count) {
    this.leaderProducerFailureCount = count;
  }

  public synchronized void recordBenignLeaderProducerFailure() {
    benignLeaderProducerFailureCount += 1;
  }

  public synchronized long getBenignLeaderProducerFailure() {
    return benignLeaderProducerFailureCount;
  }

  public synchronized void setBenignLeaderProducerFailure(long count) {
    this.benignLeaderProducerFailureCount = count;
  }
}
