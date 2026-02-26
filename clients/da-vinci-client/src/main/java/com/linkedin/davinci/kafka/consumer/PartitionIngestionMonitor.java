package com.linkedin.davinci.kafka.consumer;

import java.util.concurrent.atomic.LongAdder;


/**
 * Lightweight per-partition metric accumulator using {@link LongAdder} for lock-free concurrent recording.
 * Only instantiated when a monitoring session is active for a partition.
 *
 * <p>All counters use {@link LongAdder} where increment/add operations are ~10-20ns with no contention.
 * Latency tracking uses sum+count pairs (not histograms) to minimize overhead.
 * {@link #snapshotAndReset(long)} atomically reads and resets all counters via {@code sumThenReset()},
 * computes rates and averages, and returns a {@link PartitionIngestionSnapshot} POJO.
 *
 * <p>Minor temporal smearing at reset boundaries is acceptable for a monitoring tool.
 */
public class PartitionIngestionMonitor {
  // Consumed records/bytes
  private final LongAdder recordsConsumed = new LongAdder();
  private final LongAdder bytesConsumed = new LongAdder();

  // Leader produced records/bytes
  private final LongAdder leaderRecordsProduced = new LongAdder();
  private final LongAdder leaderBytesProduced = new LongAdder();

  // E2E processing latency (ns sum + count)
  private final LongAdder e2eProcessingLatencyNsSum = new LongAdder();
  private final LongAdder e2eProcessingLatencyCount = new LongAdder();

  // Leader preprocessing latency (ns sum + count)
  private final LongAdder leaderPreprocessingLatencyNsSum = new LongAdder();
  private final LongAdder leaderPreprocessingLatencyCount = new LongAdder();

  // Leader produce latency (ns sum + count)
  private final LongAdder leaderProduceLatencyNsSum = new LongAdder();
  private final LongAdder leaderProduceLatencyCount = new LongAdder();

  // Leader producer completion latency (ns sum + count)
  private final LongAdder leaderCompletionLatencyNsSum = new LongAdder();
  private final LongAdder leaderCompletionLatencyCount = new LongAdder();

  // Leader producer callback latency (ns sum + count)
  private final LongAdder leaderCallbackLatencyNsSum = new LongAdder();
  private final LongAdder leaderCallbackLatencyCount = new LongAdder();

  // Storage engine put latency (ns sum + count)
  private final LongAdder storagePutLatencyNsSum = new LongAdder();
  private final LongAdder storagePutLatencyCount = new LongAdder();

  // Value bytes lookup latency (ns sum + count)
  private final LongAdder valueLookupLatencyNsSum = new LongAdder();
  private final LongAdder valueLookupLatencyCount = new LongAdder();

  // RMD lookup latency (ns sum + count)
  private final LongAdder rmdLookupLatencyNsSum = new LongAdder();
  private final LongAdder rmdLookupLatencyCount = new LongAdder();

  /**
   * Record a consumed record with its size in bytes.
   */
  public void recordConsumed(int bytes) {
    recordsConsumed.increment();
    bytesConsumed.add(bytes);
  }

  /**
   * Record a leader-produced record with its size in bytes.
   */
  public void recordLeaderProduced(long bytes) {
    leaderRecordsProduced.increment();
    leaderBytesProduced.add(bytes);
  }

  public void recordE2EProcessingLatencyNs(long nanos) {
    e2eProcessingLatencyNsSum.add(nanos);
    e2eProcessingLatencyCount.increment();
  }

  public void recordLeaderPreprocessingLatencyNs(long nanos) {
    leaderPreprocessingLatencyNsSum.add(nanos);
    leaderPreprocessingLatencyCount.increment();
  }

  public void recordLeaderProduceLatencyNs(long nanos) {
    leaderProduceLatencyNsSum.add(nanos);
    leaderProduceLatencyCount.increment();
  }

  public void recordLeaderCompletionLatencyNs(long nanos) {
    leaderCompletionLatencyNsSum.add(nanos);
    leaderCompletionLatencyCount.increment();
  }

  public void recordLeaderCallbackLatencyNs(long nanos) {
    leaderCallbackLatencyNsSum.add(nanos);
    leaderCallbackLatencyCount.increment();
  }

  public void recordStoragePutLatencyNs(long nanos) {
    storagePutLatencyNsSum.add(nanos);
    storagePutLatencyCount.increment();
  }

  public void recordValueLookupLatencyNs(long nanos) {
    valueLookupLatencyNsSum.add(nanos);
    valueLookupLatencyCount.increment();
  }

  public void recordRmdLookupLatencyNs(long nanos) {
    rmdLookupLatencyNsSum.add(nanos);
    rmdLookupLatencyCount.increment();
  }

  /**
   * Atomically reads and resets all counters, computes rates and averages, and returns
   * a {@link PartitionIngestionSnapshot}.
   *
   * @param elapsedMs the elapsed time in milliseconds since the last snapshot
   * @return snapshot with computed rates and averages
   */
  public PartitionIngestionSnapshot snapshotAndReset(long elapsedMs) {
    double elapsedSec = elapsedMs / 1000.0;

    long records = recordsConsumed.sumThenReset();
    long bytes = bytesConsumed.sumThenReset();
    long leaderRecords = leaderRecordsProduced.sumThenReset();
    long leaderBytes = leaderBytesProduced.sumThenReset();

    return new PartitionIngestionSnapshot(
        elapsedSec > 0 ? records / elapsedSec : 0,
        elapsedSec > 0 ? bytes / elapsedSec : 0,
        elapsedSec > 0 ? leaderRecords / elapsedSec : 0,
        elapsedSec > 0 ? leaderBytes / elapsedSec : 0,
        computeAvgLatencyMs(e2eProcessingLatencyNsSum, e2eProcessingLatencyCount),
        computeAvgLatencyMs(leaderPreprocessingLatencyNsSum, leaderPreprocessingLatencyCount),
        computeAvgLatencyMs(leaderProduceLatencyNsSum, leaderProduceLatencyCount),
        computeAvgLatencyMs(leaderCompletionLatencyNsSum, leaderCompletionLatencyCount),
        computeAvgLatencyMs(leaderCallbackLatencyNsSum, leaderCallbackLatencyCount),
        computeAvgLatencyMs(storagePutLatencyNsSum, storagePutLatencyCount),
        computeAvgLatencyMs(valueLookupLatencyNsSum, valueLookupLatencyCount),
        computeAvgLatencyMs(rmdLookupLatencyNsSum, rmdLookupLatencyCount));
  }

  private static double computeAvgLatencyMs(LongAdder sumAdder, LongAdder countAdder) {
    long sum = sumAdder.sumThenReset();
    long count = countAdder.sumThenReset();
    if (count == 0) {
      return 0;
    }
    // Convert from nanoseconds to milliseconds
    return (sum / (double) count) / 1_000_000.0;
  }
}
