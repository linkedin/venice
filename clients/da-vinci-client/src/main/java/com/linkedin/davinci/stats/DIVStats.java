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

  private final WritePathLatencySensor leaderProcessToDIVLatencySensor;
  private final WritePathLatencySensor drainerProcessToDIVLatencySensor;
  private final LongAdder duplicateMsg = new LongAdder();
  private final LongAdder successMsg = new LongAdder();
  private long benignLeaderOffsetRewindCount = 0;
  private long potentiallyLossyLeaderOffsetRewindCount = 0;
  private long missingMsg = 0;
  private long corruptedMsg = 0;
  private long leaderProducerFailureCount = 0;
  private long benignLeaderProducerFailureCount = 0;

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

    // this sensor measure the latency from the start of processing a record until the DIV validation completes in
    // pre-producing stage, that's on consumer thread
    leaderProcessToDIVLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "leader_process_to_div_latency");
    // this sensor measure the latency from the start of processing a record until the DIV validation completes in
    // internal processing stage, that's on drainer thread
    drainerProcessToDIVLatencySensor =
        new WritePathLatencySensor(localRepository, metricConfig, "drainer_process_to_div_latency");
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

  public WritePathLatencySensor getLeaderProcessToDIVLatencySensor() {
    return leaderProcessToDIVLatencySensor;
  }

  public void recordLeaderProcessToDIVLatencyMs(double value, long currentTimeMs) {
    leaderProcessToDIVLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getDrainerProcessToDIVLatencySensor() {
    return drainerProcessToDIVLatencySensor;
  }

  public void recordDrainerProcessToDIVLatencyMs(double value, long currentTimeMs) {
    drainerProcessToDIVLatencySensor.record(value, currentTimeMs);
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
