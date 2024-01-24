package com.linkedin.davinci.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.AbstractVeniceAggStoreStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.function.LongSupplier;


/**
 * This class is an aggregate place that keeps stats objects for multiple stores and total stats for each region for
 * AggKafkaConsumerService.
 */
public class AggKafkaConsumerServiceStats extends AbstractVeniceAggStoreStats<KafkaConsumerServiceStats> {
  public AggKafkaConsumerServiceStats(
      String regionName,
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      LongSupplier getMaxElapsedTimeSinceLastPollInConsumerPool,
      boolean isUnregisterMetricForDeletedStoreEnabled) {
    super(
        regionName,
        metricsRepository,
        (
            repo,
            storeName) -> new KafkaConsumerServiceStats(repo, storeName, getMaxElapsedTimeSinceLastPollInConsumerPool),
        metadataRepository,
        isUnregisterMetricForDeletedStoreEnabled);
  }

  public void recordTotalBytesPerPoll(double count) {
    totalStats.recordByteSizePerPoll(count);
  }

  public void recordTotalConsumerIdleTime(double idleTime) {
    totalStats.recordConsumerIdleTime(idleTime);
  }

  public void recordTotalPollRequestLatency(double latency) {
    totalStats.recordPollRequestLatency(latency);
  }

  public void recordTotalPollResultNum(int count) {
    totalStats.recordPollResultNum(count);
  }

  public void recordTotalNonZeroPollResultNum(int count) {
    totalStats.recordNonZeroPollResultNum(count);
  }

  public void recordTotalConsumerRecordsProducingToWriterBufferLatency(double latency) {
    totalStats.recordConsumerRecordsProducingToWriterBufferLatency(latency);
  }

  public void recordTotalPollError() {
    totalStats.recordPollError();
  }

  public void recordTotalDetectedDeletedTopicNum(int count) {
    totalStats.recordDetectedDeletedTopicNum(count);
  }

  public void recordTotalDetectedNoRunningIngestionTopicPartitionNum(int count) {
    totalStats.recordDetectedNoRunningIngestionTopicPartitionNum(count);
  }

  public void recordTotalDelegateSubscribeLatency(double value) {
    totalStats.recordDelegateSubscribeLatency(value);
  }

  public void recordTotalUpdateCurrentAssignmentLatency(double value) {
    totalStats.recordUpdateCurrentAssignmentLatency(value);
  }

  public void recordTotalMinPartitionsPerConsumer(int count) {
    totalStats.recordMinPartitionsPerConsumer(count);
  }

  public void recordTotalMaxPartitionsPerConsumer(int count) {
    totalStats.recordMaxPartitionsPerConsumer(count);
  }

  public void recordTotalAvgPartitionsPerConsumer(int count) {
    totalStats.recordAvgPartitionsPerConsumer(count);
  }

  public void recordTotalOffsetLagIsAbsent() {
    totalStats.recordOffsetLagIsAbsent();
  }

  public void recordTotalOffsetLagIsPresent() {
    totalStats.recordOffsetLagIsPresent();
  }

  public void recordTotalLatestOffsetIsAbsent() {
    totalStats.recordLatestOffsetIsAbsent();
  }

  public void recordTotalLatestOffsetIsPresent() {
    totalStats.recordLatestOffsetIsPresent();
  }
}
