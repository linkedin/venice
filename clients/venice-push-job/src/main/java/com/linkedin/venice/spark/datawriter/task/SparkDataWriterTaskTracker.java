package com.linkedin.venice.spark.datawriter.task;

import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * This class is used to track the metrics for the Spark Data Writer task.
 */
public class SparkDataWriterTaskTracker implements DataWriterTaskTracker {
  private final DataWriterAccumulators accumulators;
  private Map<Integer, Long> perPartitionRecordCounts = Collections.emptyMap();
  private final Set<String> failedExternalStorageRegions = new HashSet<>();
  /**
   * Deliberately plain fields rather than {@code LongAccumulator}s: with speculative execution enabled a
   * partition can be attempted twice and both attempts' accumulator updates reach the driver, inflating a
   * summed duration. On an executor these hold this task attempt's own accrued time and are shipped to the
   * driver as task-output row columns; on the driver they hold the sum over the one surviving row per
   * partition, set via {@link #setExternalStorageWriteTimeMs}/{@link #setVeniceWriteTimeMs}.
   */
  private long externalStorageWriteTimeMs;
  private long veniceWriteTimeMs;

  public SparkDataWriterTaskTracker(DataWriterAccumulators accumulators) {
    this.accumulators = accumulators;
  }

  @Override
  public void trackSprayAllPartitions() {
    accumulators.sprayAllPartitionsTriggeredCount.add(1);
  }

  @Override
  public void trackEmptyRecord() {
    accumulators.emptyRecordCounter.add(1);
  }

  @Override
  public void trackKeySize(int size) {
    accumulators.totalKeySizeCounter.add(size);
  }

  @Override
  public void trackUncompressedValueSize(int size) {
    accumulators.uncompressedValueSizeCounter.add(size);
  }

  @Override
  public void trackLargestUncompressedValueSize(int size) {
    accumulators.largestUncompressedValueSize.add(size);
  }

  @Override
  public void trackCompressedValueSize(int size) {
    accumulators.compressedValueSizeCounter.add(size);
  }

  @Override
  public void trackGzipCompressedValueSize(int size) {
    accumulators.gzipCompressedValueSizeCounter.add(size);
  }

  @Override
  public void trackZstdCompressedValueSize(int size) {
    accumulators.zstdCompressedValueSizeCounter.add(size);
  }

  @Override
  public void trackWriteAclAuthorizationFailure() {
    accumulators.writeAclAuthorizationFailureCounter.add(1);
  }

  @Override
  public void trackRecordTooLargeFailure() {
    accumulators.recordTooLargeFailureCounter.add(1);
  }

  @Override
  public void trackUncompressedRecordTooLargeFailure() {
    accumulators.uncompressedRecordTooLargeFailureCounter.add(1);
  }

  @Override
  public void trackRecordSentToPubSub() {
    accumulators.outputRecordCounter.add(1);
  }

  @Override
  public void trackDuplicateKeyWithDistinctValue(int count) {
    accumulators.duplicateKeyWithDistinctValueCounter.add(count);
  }

  @Override
  public void trackDuplicateKeyWithIdenticalValue(int count) {
    accumulators.duplicateKeyWithIdenticalValueCounter.add(count);
  }

  @Override
  public void trackRepushTtlFilteredRecord() {
    accumulators.repushTtlFilteredRecordCounter.add(1);
  }

  @Override
  public void trackIncrementalPushThrottledTime(long timeMs) {
    accumulators.incrementalPushThrottleTimeCounter.add(timeMs);
  }

  @Override
  public void trackFailedExternalStorageRegion(String regionName) {
    if (regionName == null || regionName.isEmpty()) {
      return;
    }
    failedExternalStorageRegions.add(regionName);
  }

  @Override
  public void trackExternalStorageWriteTime(long timeMs) {
    if (timeMs <= 0) {
      return;
    }
    externalStorageWriteTimeMs += timeMs;
  }

  @Override
  public void trackVeniceWriteTime(long timeMs) {
    if (timeMs <= 0) {
      return;
    }
    veniceWriteTimeMs += timeMs;
  }

  @Override
  public void trackPartitionWriterClose() {
    accumulators.partitionWriterCloseCounter.add(1);
  }

  @Override
  public long getSprayAllPartitionsCount() {
    return accumulators.sprayAllPartitionsTriggeredCount.value();
  }

  @Override
  public long getTotalKeySize() {
    return accumulators.totalKeySizeCounter.value();
  }

  @Override
  public long getTotalValueSize() {
    return accumulators.compressedValueSizeCounter.value();
  }

  @Override
  public long getTotalUncompressedValueSize() {
    return accumulators.uncompressedValueSizeCounter.value();
  }

  @Override
  public int getLargestUncompressedValueSize() {
    return accumulators.largestUncompressedValueSize.value();
  }

  @Override
  public long getTotalGzipCompressedValueSize() {
    return accumulators.gzipCompressedValueSizeCounter.value();
  }

  @Override
  public long getTotalZstdCompressedValueSize() {
    return accumulators.zstdCompressedValueSizeCounter.value();
  }

  @Override
  public long getRecordTooLargeFailureCount() {
    return accumulators.recordTooLargeFailureCounter.value();
  }

  @Override
  public long getUncompressedRecordTooLargeFailureCount() {
    return accumulators.uncompressedRecordTooLargeFailureCounter.value();
  }

  @Override
  public long getWriteAclAuthorizationFailureCount() {
    return accumulators.writeAclAuthorizationFailureCounter.value();
  }

  @Override
  public long getDuplicateKeyWithDistinctValueCount() {
    return accumulators.duplicateKeyWithDistinctValueCounter.value();
  }

  @Override
  public long getOutputRecordsCount() {
    return accumulators.outputRecordCounter.value();
  }

  @Override
  public long getPartitionWriterCloseCount() {
    return accumulators.partitionWriterCloseCounter.value();
  }

  @Override
  public long getRepushTtlFilterCount() {
    return accumulators.repushTtlFilteredRecordCounter.value();
  }

  @Override
  public long getIncrementalPushThrottledTimeMs() {
    return accumulators.incrementalPushThrottleTimeCounter.value();
  }

  /**
   * Sets the per-partition record counts collected from the Spark DAG output via {@code collect()}.
   */
  public void setPerPartitionRecordCounts(Map<Integer, Long> counts) {
    this.perPartitionRecordCounts = (counts == null || counts.isEmpty())
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(new HashMap<>(counts));
  }

  /**
   * Sets the deduplicated failed external-storage regions collected from successful Spark task output.
   */
  public void setFailedExternalStorageRegions(Set<String> failedRegions) {
    failedExternalStorageRegions.clear();
    if (failedRegions != null) {
      failedExternalStorageRegions.addAll(failedRegions);
    }
  }

  /**
   * Sets the total external-storage write time summed on the driver from the successful task-output rows.
   */
  public void setExternalStorageWriteTimeMs(long timeMs) {
    this.externalStorageWriteTimeMs = timeMs;
  }

  /**
   * Sets the total Venice write time summed on the driver from the successful task-output rows.
   */
  public void setVeniceWriteTimeMs(long timeMs) {
    this.veniceWriteTimeMs = timeMs;
  }

  @Override
  public long getExternalStorageWriteTimeMs() {
    return externalStorageWriteTimeMs;
  }

  @Override
  public long getVeniceWriteTimeMs() {
    return veniceWriteTimeMs;
  }

  @Override
  public Map<Integer, Long> getPerPartitionRecordCounts() {
    return perPartitionRecordCounts;
  }

  @Override
  public Set<String> getFailedExternalStorageRegions() {
    return failedExternalStorageRegions.isEmpty()
        ? Collections.emptySet()
        : Collections.unmodifiableSet(new HashSet<>(failedExternalStorageRegions));
  }
}
