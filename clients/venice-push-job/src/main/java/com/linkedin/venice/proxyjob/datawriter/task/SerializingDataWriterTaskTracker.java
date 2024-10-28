package com.linkedin.venice.proxyjob.datawriter.task;

import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import java.io.Serializable;


public class SerializingDataWriterTaskTracker implements DataWriterTaskTracker, Serializable {
  private long sprayAllPartitionsCount = 0;
  private long emptyRecordCount = 0;
  private long totalKeySize = 0;
  private long totalValueSize = 0;
  private long totalUncompressedValueSize = 0;
  private long totalGzipCompressedValueSize = 0;
  private long totalZstdCompressedValueSize = 0;
  private long recordTooLargeFailureCount = 0;
  private long writeAclAuthorizationFailureCount = 0;
  private long duplicateKeyWithIdenticalValueCount = 0;
  private long duplicateKeyWithDistinctValueCount = 0;
  private long outputRecordsCount = 0;
  private long partitionWriterCloseCount = 0;
  private long repushTtlFilterCount = 0;
  private long totalPutOrDeleteRecordsCount = 0;

  public static SerializingDataWriterTaskTracker fromDataWriterTaskTracker(DataWriterTaskTracker delegate) {
    SerializingDataWriterTaskTracker taskTracker = new SerializingDataWriterTaskTracker();
    taskTracker.setSprayAllPartitionsCount(delegate.getSprayAllPartitionsCount());
    taskTracker.setEmptyRecordCount(delegate.getEmptyRecordCount());
    taskTracker.setTotalKeySize(delegate.getTotalKeySize());
    taskTracker.setTotalValueSize(delegate.getTotalValueSize());
    taskTracker.setTotalUncompressedValueSize(delegate.getTotalUncompressedValueSize());
    taskTracker.setTotalGzipCompressedValueSize(delegate.getTotalGzipCompressedValueSize());
    taskTracker.setTotalZstdCompressedValueSize(delegate.getTotalZstdCompressedValueSize());
    taskTracker.setRecordTooLargeFailureCount(delegate.getRecordTooLargeFailureCount());
    taskTracker.setWriteAclAuthorizationFailureCount(delegate.getWriteAclAuthorizationFailureCount());
    taskTracker.setDuplicateKeyWithIdenticalValueCount(delegate.getDuplicateKeyWithIdenticalValueCount());
    taskTracker.setDuplicateKeyWithDistinctValueCount(delegate.getDuplicateKeyWithDistinctValueCount());
    taskTracker.setOutputRecordsCount(delegate.getOutputRecordsCount());
    taskTracker.setPartitionWriterCloseCount(delegate.getPartitionWriterCloseCount());
    taskTracker.setRepushTtlFilterCount(delegate.getRepushTtlFilterCount());
    taskTracker.setTotalPutOrDeleteRecordsCount(delegate.getTotalPutOrDeleteRecordsCount());
    return taskTracker;
  }

  @Override
  public long getSprayAllPartitionsCount() {
    return sprayAllPartitionsCount;
  }

  public void setSprayAllPartitionsCount(long sprayAllPartitionsCount) {
    this.sprayAllPartitionsCount = sprayAllPartitionsCount;
  }

  @Override
  public long getEmptyRecordCount() {
    return emptyRecordCount;
  }

  public void setEmptyRecordCount(long emptyRecordCount) {
    this.emptyRecordCount = emptyRecordCount;
  }

  @Override
  public long getTotalKeySize() {
    return totalKeySize;
  }

  public void setTotalKeySize(long totalKeySize) {
    this.totalKeySize = totalKeySize;
  }

  @Override
  public long getTotalValueSize() {
    return totalValueSize;
  }

  public void setTotalValueSize(long totalValueSize) {
    this.totalValueSize = totalValueSize;
  }

  @Override
  public long getTotalUncompressedValueSize() {
    return totalUncompressedValueSize;
  }

  public void setTotalUncompressedValueSize(long totalUncompressedValueSize) {
    this.totalUncompressedValueSize = totalUncompressedValueSize;
  }

  @Override
  public long getTotalGzipCompressedValueSize() {
    return totalGzipCompressedValueSize;
  }

  public void setTotalGzipCompressedValueSize(long totalGzipCompressedValueSize) {
    this.totalGzipCompressedValueSize = totalGzipCompressedValueSize;
  }

  @Override
  public long getTotalZstdCompressedValueSize() {
    return totalZstdCompressedValueSize;
  }

  public void setTotalZstdCompressedValueSize(long totalZstdCompressedValueSize) {
    this.totalZstdCompressedValueSize = totalZstdCompressedValueSize;
  }

  @Override
  public long getRecordTooLargeFailureCount() {
    return recordTooLargeFailureCount;
  }

  public void setRecordTooLargeFailureCount(long recordTooLargeFailureCount) {
    this.recordTooLargeFailureCount = recordTooLargeFailureCount;
  }

  @Override
  public long getWriteAclAuthorizationFailureCount() {
    return writeAclAuthorizationFailureCount;
  }

  public void setWriteAclAuthorizationFailureCount(long writeAclAuthorizationFailureCount) {
    this.writeAclAuthorizationFailureCount = writeAclAuthorizationFailureCount;
  }

  @Override
  public long getDuplicateKeyWithIdenticalValueCount() {
    return duplicateKeyWithIdenticalValueCount;
  }

  public void setDuplicateKeyWithIdenticalValueCount(long duplicateKeyWithIdenticalValueCount) {
    this.duplicateKeyWithIdenticalValueCount = duplicateKeyWithIdenticalValueCount;
  }

  @Override
  public long getDuplicateKeyWithDistinctValueCount() {
    return duplicateKeyWithDistinctValueCount;
  }

  public void setDuplicateKeyWithDistinctValueCount(long duplicateKeyWithDistinctValueCount) {
    this.duplicateKeyWithDistinctValueCount = duplicateKeyWithDistinctValueCount;
  }

  @Override
  public long getOutputRecordsCount() {
    return outputRecordsCount;
  }

  public void setOutputRecordsCount(long outputRecordsCount) {
    this.outputRecordsCount = outputRecordsCount;
  }

  @Override
  public long getPartitionWriterCloseCount() {
    return partitionWriterCloseCount;
  }

  public void setPartitionWriterCloseCount(long partitionWriterCloseCount) {
    this.partitionWriterCloseCount = partitionWriterCloseCount;
  }

  @Override
  public long getRepushTtlFilterCount() {
    return repushTtlFilterCount;
  }

  public void setRepushTtlFilterCount(long repushTtlFilterCount) {
    this.repushTtlFilterCount = repushTtlFilterCount;
  }

  @Override
  public long getTotalPutOrDeleteRecordsCount() {
    return totalPutOrDeleteRecordsCount;
  }

  public void setTotalPutOrDeleteRecordsCount(long totalPutOrDeleteRecordsCount) {
    this.totalPutOrDeleteRecordsCount = totalPutOrDeleteRecordsCount;
  }
}
