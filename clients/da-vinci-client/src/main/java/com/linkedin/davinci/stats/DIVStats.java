package com.linkedin.davinci.stats;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;


/**
 * This class contains stats for DIV. The stat class is used in {@link VeniceVersionedStats} to serve for
 * a single store version or total of all store versions.
 * This class does not contain reporting logic as reporting is done by the {@link DIVStatsReporter}.
 */
public class DIVStats {
  private final MetricConfig metricConfig = new MetricConfig();
  /**
   * Creating this separate local metric repository only to utilize the sensor library and not for reporting.
   */
  private final MetricsRepository localRepository = new MetricsRepository(metricConfig);
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

  private long benignLeaderOffsetRewindCount = 0;
  private long potentiallyLossyLeaderOffsetRewindCount = 0;
  private long leaderProducerFailureCount = 0;
  private long benignLeaderProducerFailureCount = 0;
  private long duplicateMsg = 0;
  private long missingMsg = 0;
  private long corruptedMsg = 0;
  private long successMsg = 0;
  private long currentIdleTime = 0;
  private long overallIdleTime = 0;

  public DIVStats() {
    /**
     * For now, latency sensors will still be created as store name is not passed down to this class, and we cannot
     * determine whether latency sensors are needed.
     * {@link DIVStatsReporter} already blocks latency metrics for system stores from being reported to external
     * metric collecting system.
     */
    producerBrokerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "producer_to_broker_latency");
    brokerConsumerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "broker_to_consumer_latency");
    producerConsumerLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "producer_to_consumer_latency");
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
    return duplicateMsg;
  }

  public void recordDuplicateMsg() {
    duplicateMsg += 1;
  }

  public void setDuplicateMsg(long count) {
    this.duplicateMsg = count;
  }

  public long getMissingMsg() {
    return missingMsg;
  }

  public void recordMissingMsg() {
    missingMsg += 1;
  }

  public void setMissingMsg(long count) {
    this.missingMsg = count;
  }

  public long getCorruptedMsg() {
    return corruptedMsg;
  }

  public void recordCorruptedMsg() {
    corruptedMsg += 1;
  }

  public void setCorruptedMsg(long count) {
    this.corruptedMsg = count;
  }

  public long getSuccessMsg() {
    return successMsg;
  }

  public void recordSuccessMsg() {
    successMsg += 1;
  }

  public long getCurrentIdleTime() {
    return currentIdleTime;
  }

  public void recordCurrentIdleTime() {
    currentIdleTime += 1;
  }

  public void resetCurrentIdleTime() {
    currentIdleTime = 0;
  }

  public long getOverallIdleTime() {
    return overallIdleTime;
  }

  public void recordOverallIdleTime() {
    overallIdleTime += 1;
  }

  public void recordProducerBrokerLatencyMs(double value) {
    producerBrokerLatencySensor.record(value);
  }

  public WritePathLatencySensor getProducerBrokerLatencySensor() {
    return producerBrokerLatencySensor;
  }

  public WritePathLatencySensor getProducerSourceBrokerLatencySensor() {
    return producerSourceBrokerLatencySensor;
  }

  public void recordBrokerConsumerLatencyMs(double value) {
    brokerConsumerLatencySensor.record(value);
  }

  public WritePathLatencySensor getBrokerConsumerLatencySensor() {
    return brokerConsumerLatencySensor;
  }

  public void recordProducerConsumerLatencyMs(double value) {
    producerConsumerLatencySensor.record(value);
  }

  public WritePathLatencySensor getProducerConsumerLatencySensor() {
    return producerConsumerLatencySensor;
  }

  public void recordProducerSourceBrokerLatencyMs(double value) {
    producerSourceBrokerLatencySensor.record(value);
  }

  public void recordSourceBrokerLeaderConsumerLatencyMs(double value) {
    sourceBrokerLeaderConsumerLatencySensor.record(value);
  }

  public WritePathLatencySensor getSourceBrokerLeaderConsumerLatencySensor() {
    return sourceBrokerLeaderConsumerLatencySensor;
  }

  public void recordProducerLeaderConsumerLatencyMs(double value) {
    producerLeaderConsumerLatencySensor.record(value);
  }

  public WritePathLatencySensor getProducerLeaderConsumerLatencySensor() {
    return producerLeaderConsumerLatencySensor;
  }

  public void recordProducerLocalBrokerLatencyMs(double value) {
    producerLocalBrokerLatencySensor.record(value);
  }

  public WritePathLatencySensor getProducerLocalBrokerLatencySensor() {
    return producerLocalBrokerLatencySensor;
  }

  public void recordLocalBrokerFollowerConsumerLatencyMs(double value) {
    localBrokerFollowerConsumerLatencySensor.record(value);
  }

  public WritePathLatencySensor getLocalBrokerFollowerConsumerLatencySensor() {
    return localBrokerFollowerConsumerLatencySensor;
  }

  public void recordProducerFollowerConsumerLatencyMs(double value) {
    producerFollowerConsumerLatencySensor.record(value);
  }

  public WritePathLatencySensor getProducerFollowerConsumerLatencySensor() {
    return producerFollowerConsumerLatencySensor;
  }

  public void recordLeaderProducerCompletionLatencyMs(double value) {
    leaderProducerCompletionLatencySensor.record(value);
  }

  public WritePathLatencySensor getLeaderProducerCompletionLatencySensor() {
    return leaderProducerCompletionLatencySensor;
  }

  public void recordBenignLeaderOffsetRewind() {
    benignLeaderOffsetRewindCount += 1;
  }

  public long getBenignLeaderOffsetRewindCount() {
    return benignLeaderOffsetRewindCount;
  }

  public void setBenignLeaderOffsetRewindCount(long count) {
    this.benignLeaderOffsetRewindCount = count;
  }

  public void recordPotentiallyLossyLeaderOffsetRewind() {
    potentiallyLossyLeaderOffsetRewindCount += 1;
  }

  public long getPotentiallyLossyLeaderOffsetRewindCount() {
    return potentiallyLossyLeaderOffsetRewindCount;
  }

  public void setPotentiallyLossyLeaderOffsetRewindCount(long count) {
    this.potentiallyLossyLeaderOffsetRewindCount = count;
  }

  public void recordLeaderProducerFailure() {
    leaderProducerFailureCount += 1;
  }

  public long getLeaderProducerFailure() {
    return leaderProducerFailureCount;
  }

  public void setLeaderProducerFailure(long count) {
    this.leaderProducerFailureCount = count;
  }

  public void recordBenignLeaderProducerFailure() {
    benignLeaderProducerFailureCount += 1;
  }

  public long getBenignLeaderProducerFailure() {
    return benignLeaderProducerFailureCount;
  }

  public void setBenignLeaderProducerFailure(long count) {
    this.benignLeaderProducerFailureCount = count;
  }
}
