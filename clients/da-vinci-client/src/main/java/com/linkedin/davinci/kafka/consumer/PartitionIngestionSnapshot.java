package com.linkedin.davinci.kafka.consumer;

/**
 * Immutable POJO holding computed rates and averages from a single snapshot interval.
 * Returned by {@link PartitionIngestionMonitor#snapshotAndReset(long)}.
 */
public class PartitionIngestionSnapshot {
  // Consumer rates
  private final double recordsConsumedPerSec;
  private final double bytesConsumedPerSec;

  // Leader produced rates
  private final double leaderRecordsProducedPerSec;
  private final double leaderBytesProducedPerSec;

  // Average latencies (ms)
  private final double e2eProcessingLatencyAvgMs;
  private final double leaderPreprocessingLatencyAvgMs;
  private final double leaderProduceLatencyAvgMs;
  private final double leaderCompletionLatencyAvgMs;
  private final double leaderCallbackLatencyAvgMs;
  private final double storagePutLatencyAvgMs;
  private final double valueLookupLatencyAvgMs;
  private final double rmdLookupLatencyAvgMs;

  public PartitionIngestionSnapshot(
      double recordsConsumedPerSec,
      double bytesConsumedPerSec,
      double leaderRecordsProducedPerSec,
      double leaderBytesProducedPerSec,
      double e2eProcessingLatencyAvgMs,
      double leaderPreprocessingLatencyAvgMs,
      double leaderProduceLatencyAvgMs,
      double leaderCompletionLatencyAvgMs,
      double leaderCallbackLatencyAvgMs,
      double storagePutLatencyAvgMs,
      double valueLookupLatencyAvgMs,
      double rmdLookupLatencyAvgMs) {
    this.recordsConsumedPerSec = recordsConsumedPerSec;
    this.bytesConsumedPerSec = bytesConsumedPerSec;
    this.leaderRecordsProducedPerSec = leaderRecordsProducedPerSec;
    this.leaderBytesProducedPerSec = leaderBytesProducedPerSec;
    this.e2eProcessingLatencyAvgMs = e2eProcessingLatencyAvgMs;
    this.leaderPreprocessingLatencyAvgMs = leaderPreprocessingLatencyAvgMs;
    this.leaderProduceLatencyAvgMs = leaderProduceLatencyAvgMs;
    this.leaderCompletionLatencyAvgMs = leaderCompletionLatencyAvgMs;
    this.leaderCallbackLatencyAvgMs = leaderCallbackLatencyAvgMs;
    this.storagePutLatencyAvgMs = storagePutLatencyAvgMs;
    this.valueLookupLatencyAvgMs = valueLookupLatencyAvgMs;
    this.rmdLookupLatencyAvgMs = rmdLookupLatencyAvgMs;
  }

  public double getRecordsConsumedPerSec() {
    return recordsConsumedPerSec;
  }

  public double getBytesConsumedPerSec() {
    return bytesConsumedPerSec;
  }

  public double getLeaderRecordsProducedPerSec() {
    return leaderRecordsProducedPerSec;
  }

  public double getLeaderBytesProducedPerSec() {
    return leaderBytesProducedPerSec;
  }

  public double getE2eProcessingLatencyAvgMs() {
    return e2eProcessingLatencyAvgMs;
  }

  public double getLeaderPreprocessingLatencyAvgMs() {
    return leaderPreprocessingLatencyAvgMs;
  }

  public double getLeaderProduceLatencyAvgMs() {
    return leaderProduceLatencyAvgMs;
  }

  public double getLeaderCompletionLatencyAvgMs() {
    return leaderCompletionLatencyAvgMs;
  }

  public double getLeaderCallbackLatencyAvgMs() {
    return leaderCallbackLatencyAvgMs;
  }

  public double getStoragePutLatencyAvgMs() {
    return storagePutLatencyAvgMs;
  }

  public double getValueLookupLatencyAvgMs() {
    return valueLookupLatencyAvgMs;
  }

  public double getRmdLookupLatencyAvgMs() {
    return rmdLookupLatencyAvgMs;
  }
}
