package com.linkedin.venice.spark.datawriter.task;

import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;


public class SparkDataWriterTaskTracker implements DataWriterTaskTracker {
  private final DataWriterAccumulators accumulators;

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
}
