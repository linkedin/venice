package com.linkedin.davinci.stats;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;


public class DIVStats {
  private long duplicateMsg = 0;
  private long missingMsg = 0;
  private long corruptedMsg = 0;
  private long successMsg = 0;
  private long currentIdleTime = 0;
  private long overallIdleTime = 0;

  private final MetricConfig metricConfig = new MetricConfig();
  /**
   * Creating this separate local MetricsRepository only to utilize the Sensor library and not for reporting.
   * Reporting is done at the higher level by the DIVStatsReporter.
   */
  private final MetricsRepository localRepository = new MetricsRepository(metricConfig);
  private final Sensor producerBrokerLatencySensor;
  private final Sensor brokerConsumerLatencySensor;
  private final Sensor producerConsumerLatencySensor;
  private final Sensor producerSourceBrokerLatencySensor;
  private final Sensor sourceBrokerLeaderConsumerLatencySensor;
  private final Sensor producerLeaderConsumerLatencySensor;
  private final Sensor producerLocalBrokerLatencySensor;
  private final Sensor localBrokerFollowerConsumerLatencySensor;
  private final Sensor producerFollowerConsumerLatencySensor;
  private final Sensor dataValidationLatencySensor;
  private final Sensor leaderProducerCompletionLatencySensor;

  private Avg producerBrokerLatencyAvgMs = new Avg();
  private Min producerBrokerLatencyMinMs = new Min();
  private Max producerBrokerLatencyMaxMs = new Max();
  private Avg brokerConsumerLatencyAvgMs = new Avg();
  private Min brokerConsumerLatencyMinMs = new Min();
  private Max brokerConsumerLatencyMaxMs = new Max();
  private Avg producerConsumerLatencyAvgMs = new Avg();
  private Min producerConsumerLatencyMinMs = new Min();
  private Max producerConsumerLatencyMaxMs = new Max();
  private Avg producerSourceBrokerLatencyAvgMs = new Avg();
  private Min producerSourceBrokerLatencyMinMs = new Min();
  private Max producerSourceBrokerLatencyMaxMs = new Max();
  private Avg sourceBrokerLeaderConsumerLatencyAvgMs = new Avg();
  private Min sourceBrokerLeaderConsumerLatencyMinMs = new Min();
  private Max sourceBrokerLeaderConsumerLatencyMaxMs = new Max();
  private Avg producerLeaderConsumerLatencyAvgMs = new Avg();
  private Min producerLeaderConsumerLatencyMinMs = new Min();
  private Max producerLeaderConsumerLatencyMaxMs = new Max();
  private Avg producerLocalBrokerLatencyAvgMs = new Avg();
  private Min producerLocalBrokerLatencyMinMs = new Min();
  private Max producerLocalBrokerLatencyMaxMs = new Max();
  private Avg localBrokerFollowerConsumerLatencyAvgMs = new Avg();
  private Min localBrokerFollowerConsumerLatencyMinMs = new Min();
  private Max localBrokerFollowerConsumerLatencyMaxMs = new Max();
  private Avg producerFollowerConsumerLatencyAvgMs = new Avg();
  private Min producerFollowerConsumerLatencyMinMs = new Min();
  private Max producerFollowerConsumerLatencyMaxMs = new Max();
  private Avg dataValidationLatencyAvgMs = new Avg();
  private Max dataValidationLatencyMaxMs = new Max();

  private Avg leaderProducerCompletionLatencyAvgMs = new Avg();
  private Max leaderProducerCompletionLatencyMaxMs = new Max();

  private long benignLeaderOffsetRewindCount = 0;
  private long potentiallyLossyLeaderOffsetRewindCount = 0;
  private long leaderProducerFailureCount = 0;
  private long benignLeaderProducerFailureCount = 0;

  public DIVStats() {
    String sensorName = "producer_to_broker_latency";
    producerBrokerLatencySensor = localRepository.sensor(sensorName);
    producerBrokerLatencySensor
        .add(sensorName + producerBrokerLatencyAvgMs.getClass().getSimpleName(), producerBrokerLatencyAvgMs);
    producerBrokerLatencySensor
        .add(sensorName + producerBrokerLatencyMaxMs.getClass().getSimpleName(), producerBrokerLatencyMaxMs);
    producerBrokerLatencySensor
        .add(sensorName + producerBrokerLatencyMinMs.getClass().getSimpleName(), producerBrokerLatencyMinMs);

    sensorName = "broker_to_consumer_latency";
    brokerConsumerLatencySensor = localRepository.sensor(sensorName);
    brokerConsumerLatencySensor
        .add(sensorName + brokerConsumerLatencyAvgMs.getClass().getSimpleName(), brokerConsumerLatencyAvgMs);
    brokerConsumerLatencySensor
        .add(sensorName + brokerConsumerLatencyMaxMs.getClass().getSimpleName(), brokerConsumerLatencyMaxMs);
    brokerConsumerLatencySensor
        .add(sensorName + brokerConsumerLatencyMinMs.getClass().getSimpleName(), brokerConsumerLatencyMinMs);

    sensorName = "producer_to_consumer_latency";
    producerConsumerLatencySensor = localRepository.sensor(sensorName);
    producerConsumerLatencySensor
        .add(sensorName + producerConsumerLatencyAvgMs.getClass().getSimpleName(), producerConsumerLatencyAvgMs);
    producerConsumerLatencySensor
        .add(sensorName + producerConsumerLatencyMaxMs.getClass().getSimpleName(), producerConsumerLatencyMaxMs);
    producerConsumerLatencySensor
        .add(sensorName + producerConsumerLatencyMinMs.getClass().getSimpleName(), producerConsumerLatencyMinMs);

    sensorName = "producer_to_source_broker_latency";
    producerSourceBrokerLatencySensor = localRepository.sensor(sensorName);
    producerSourceBrokerLatencySensor.add(
        sensorName + producerSourceBrokerLatencyAvgMs.getClass().getSimpleName(),
        producerSourceBrokerLatencyAvgMs);
    producerSourceBrokerLatencySensor.add(
        sensorName + producerSourceBrokerLatencyMaxMs.getClass().getSimpleName(),
        producerSourceBrokerLatencyMaxMs);
    producerSourceBrokerLatencySensor.add(
        sensorName + producerSourceBrokerLatencyMinMs.getClass().getSimpleName(),
        producerSourceBrokerLatencyMinMs);

    sensorName = "source_broker_to_leader_consumer_latency";
    sourceBrokerLeaderConsumerLatencySensor = localRepository.sensor(sensorName);
    sourceBrokerLeaderConsumerLatencySensor.add(
        sensorName + sourceBrokerLeaderConsumerLatencyAvgMs.getClass().getSimpleName(),
        sourceBrokerLeaderConsumerLatencyAvgMs);
    sourceBrokerLeaderConsumerLatencySensor.add(
        sensorName + sourceBrokerLeaderConsumerLatencyMaxMs.getClass().getSimpleName(),
        sourceBrokerLeaderConsumerLatencyMaxMs);
    sourceBrokerLeaderConsumerLatencySensor.add(
        sensorName + sourceBrokerLeaderConsumerLatencyMinMs.getClass().getSimpleName(),
        sourceBrokerLeaderConsumerLatencyMinMs);

    sensorName = "producer_to_leader_consumer_latency";
    producerLeaderConsumerLatencySensor = localRepository.sensor(sensorName);
    producerLeaderConsumerLatencySensor.add(
        sensorName + producerLeaderConsumerLatencyAvgMs.getClass().getSimpleName(),
        producerLeaderConsumerLatencyAvgMs);
    producerLeaderConsumerLatencySensor.add(
        sensorName + producerLeaderConsumerLatencyMaxMs.getClass().getSimpleName(),
        producerLeaderConsumerLatencyMaxMs);
    producerLeaderConsumerLatencySensor.add(
        sensorName + producerLeaderConsumerLatencyMinMs.getClass().getSimpleName(),
        producerLeaderConsumerLatencyMinMs);

    sensorName = "producer_to_local_broker_latency";
    producerLocalBrokerLatencySensor = localRepository.sensor(sensorName);
    producerLocalBrokerLatencySensor
        .add(sensorName + producerLocalBrokerLatencyAvgMs.getClass().getSimpleName(), producerLocalBrokerLatencyAvgMs);
    producerLocalBrokerLatencySensor
        .add(sensorName + producerLocalBrokerLatencyMaxMs.getClass().getSimpleName(), producerLocalBrokerLatencyMaxMs);
    producerLocalBrokerLatencySensor
        .add(sensorName + producerLocalBrokerLatencyMinMs.getClass().getSimpleName(), producerLocalBrokerLatencyMinMs);

    sensorName = "local_broker_to_follower_consumer_latency";
    localBrokerFollowerConsumerLatencySensor = localRepository.sensor(sensorName);
    localBrokerFollowerConsumerLatencySensor.add(
        sensorName + localBrokerFollowerConsumerLatencyAvgMs.getClass().getSimpleName(),
        localBrokerFollowerConsumerLatencyAvgMs);
    localBrokerFollowerConsumerLatencySensor.add(
        sensorName + localBrokerFollowerConsumerLatencyMaxMs.getClass().getSimpleName(),
        localBrokerFollowerConsumerLatencyMaxMs);
    localBrokerFollowerConsumerLatencySensor.add(
        sensorName + localBrokerFollowerConsumerLatencyMinMs.getClass().getSimpleName(),
        localBrokerFollowerConsumerLatencyMinMs);

    sensorName = "producer_to_follower_consumer_latency";
    producerFollowerConsumerLatencySensor = localRepository.sensor(sensorName);
    producerFollowerConsumerLatencySensor.add(
        sensorName + producerFollowerConsumerLatencyAvgMs.getClass().getSimpleName(),
        producerFollowerConsumerLatencyAvgMs);
    producerFollowerConsumerLatencySensor.add(
        sensorName + producerFollowerConsumerLatencyMaxMs.getClass().getSimpleName(),
        producerFollowerConsumerLatencyMaxMs);
    producerFollowerConsumerLatencySensor.add(
        sensorName + producerFollowerConsumerLatencyMinMs.getClass().getSimpleName(),
        producerFollowerConsumerLatencyMinMs);

    sensorName = "data_validation_latency";
    dataValidationLatencySensor = localRepository.sensor(sensorName);
    dataValidationLatencySensor
        .add(sensorName + dataValidationLatencyAvgMs.getClass().getSimpleName(), dataValidationLatencyAvgMs);
    dataValidationLatencySensor
        .add(sensorName + dataValidationLatencyMaxMs.getClass().getSimpleName(), dataValidationLatencyMaxMs);

    sensorName = "leader_producer_completion_latency";
    leaderProducerCompletionLatencySensor = localRepository.sensor(sensorName);
    leaderProducerCompletionLatencySensor.add(
        sensorName + leaderProducerCompletionLatencyAvgMs.getClass().getSimpleName(),
        leaderProducerCompletionLatencyAvgMs);
    leaderProducerCompletionLatencySensor.add(
        sensorName + leaderProducerCompletionLatencyMaxMs.getClass().getSimpleName(),
        producerSourceBrokerLatencyMaxMs);
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

  public double getProducerBrokerLatencyAvgMs() {
    return producerBrokerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerBrokerLatencyMinMs() {
    return producerBrokerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerBrokerLatencyMaxMs() {
    return producerBrokerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordProducerBrokerLatencyMs(double value) {
    producerBrokerLatencySensor.record(value);
  }

  public double getBrokerConsumerLatencyAvgMs() {
    return brokerConsumerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getBrokerConsumerLatencyMinMs() {
    return brokerConsumerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getBrokerConsumerLatencyMaxMs() {
    return brokerConsumerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordBrokerConsumerLatencyMs(double value) {
    brokerConsumerLatencySensor.record(value);
  }

  public double getProducerConsumerLatencyAvgMs() {
    return producerConsumerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerConsumerLatencyMinMs() {
    return producerConsumerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerConsumerLatencyMaxMs() {
    return producerConsumerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordProducerConsumerLatencyMs(double value) {
    producerConsumerLatencySensor.record(value);
  }

  public double getProducerSourceBrokerLatencyAvgMs() {
    return producerSourceBrokerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerSourceBrokerLatencyMinMs() {
    return producerSourceBrokerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerSourceBrokerLatencyMaxMs() {
    return producerSourceBrokerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordProducerSourceBrokerLatencyMs(double value) {
    producerSourceBrokerLatencySensor.record(value);
  }

  public double getSourceBrokerLeaderConsumerLatencyAvgMs() {
    return sourceBrokerLeaderConsumerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getSourceBrokerLeaderConsumerLatencyMinMs() {
    return sourceBrokerLeaderConsumerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getSourceBrokerLeaderConsumerLatencyMaxMs() {
    return sourceBrokerLeaderConsumerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordSourceBrokerLeaderConsumerLatencyMs(double value) {
    sourceBrokerLeaderConsumerLatencySensor.record(value);
  }

  public double getProducerLeaderConsumerLatencyAvgMs() {
    return producerLeaderConsumerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerLeaderConsumerLatencyMinMs() {
    return producerLeaderConsumerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerLeaderConsumerLatencyMaxMs() {
    return producerLeaderConsumerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordProducerLeaderConsumerLatencyMs(double value) {
    producerLeaderConsumerLatencySensor.record(value);
  }

  public double getProducerLocalBrokerLatencyAvgMs() {
    return producerLocalBrokerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerLocalBrokerLatencyMinMs() {
    return producerLocalBrokerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerLocalBrokerLatencyMaxMs() {
    return producerLocalBrokerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordProducerLocalBrokerLatencyMs(double value) {
    producerLocalBrokerLatencySensor.record(value);
  }

  public double getLocalBrokerFollowerConsumerLatencyAvgMs() {
    return localBrokerFollowerConsumerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getLocalBrokerFollowerConsumerLatencyMinMs() {
    return localBrokerFollowerConsumerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getLocalBrokerFollowerConsumerLatencyMaxMs() {
    return localBrokerFollowerConsumerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordLocalBrokerFollowerConsumerLatencyMs(double value) {
    localBrokerFollowerConsumerLatencySensor.record(value);
  }

  public double getProducerFollowerConsumerLatencyAvgMs() {
    return producerFollowerConsumerLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerFollowerConsumerLatencyMinMs() {
    return producerFollowerConsumerLatencyMinMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getProducerFollowerConsumerLatencyMaxMs() {
    return producerFollowerConsumerLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordProducerFollowerConsumerLatencyMs(double value) {
    producerFollowerConsumerLatencySensor.record(value);
  }

  public double getDataValidationLatencyAvgMs() {
    return dataValidationLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getDataValidationLatencyMaxMs() {
    return dataValidationLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getLeaderProducerCompletionLatencyAvgMs() {
    return leaderProducerCompletionLatencyAvgMs.measure(metricConfig, System.currentTimeMillis());
  }

  public double getLeaderProducerCompletionLatencyMaxMs() {
    return leaderProducerCompletionLatencyMaxMs.measure(metricConfig, System.currentTimeMillis());
  }

  public void recordLeaderProducerCompletionLatencyMs(double value) {
    leaderProducerCompletionLatencySensor.record(value);
  }

  public void recordDataValidationLatencyMs(double value) {
    dataValidationLatencySensor.record(value);
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
