package com.linkedin.venice.proxyjob.datawriter.task;

import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;


public class DelegatingReadOnlyDataWriterTaskTracker implements DataWriterTaskTracker {
  private DataWriterTaskTracker delegate;

  public DelegatingReadOnlyDataWriterTaskTracker() {
  }

  public void setDelegate(DataWriterTaskTracker delegate) {
    this.delegate = delegate;
  }

  @Override
  public long getSprayAllPartitionsCount() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getSprayAllPartitionsCount();
    }
    return delegate.getSprayAllPartitionsCount();
  }

  @Override
  public long getEmptyRecordCount() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getEmptyRecordCount();
    }
    return delegate.getEmptyRecordCount();
  }

  @Override
  public long getTotalKeySize() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getTotalKeySize();
    }
    return delegate.getTotalKeySize();
  }

  @Override
  public long getTotalValueSize() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getTotalValueSize();
    }
    return delegate.getTotalValueSize();
  }

  @Override
  public long getTotalUncompressedValueSize() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getTotalUncompressedValueSize();
    }
    return delegate.getTotalUncompressedValueSize();
  }

  @Override
  public long getTotalGzipCompressedValueSize() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getTotalGzipCompressedValueSize();
    }
    return delegate.getTotalGzipCompressedValueSize();
  }

  @Override
  public long getTotalZstdCompressedValueSize() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getTotalZstdCompressedValueSize();
    }
    return delegate.getTotalZstdCompressedValueSize();
  }

  @Override
  public long getRecordTooLargeFailureCount() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getRecordTooLargeFailureCount();
    }
    return delegate.getRecordTooLargeFailureCount();
  }

  @Override
  public long getWriteAclAuthorizationFailureCount() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getWriteAclAuthorizationFailureCount();
    }
    return delegate.getWriteAclAuthorizationFailureCount();
  }

  @Override
  public long getDuplicateKeyWithIdenticalValueCount() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getDuplicateKeyWithIdenticalValueCount();
    }
    return delegate.getDuplicateKeyWithIdenticalValueCount();
  }

  @Override
  public long getDuplicateKeyWithDistinctValueCount() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getDuplicateKeyWithDistinctValueCount();
    }
    return delegate.getDuplicateKeyWithDistinctValueCount();
  }

  @Override
  public long getOutputRecordsCount() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getOutputRecordsCount();
    }
    return delegate.getOutputRecordsCount();
  }

  @Override
  public long getPartitionWriterCloseCount() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getPartitionWriterCloseCount();
    }
    return delegate.getPartitionWriterCloseCount();
  }

  @Override
  public long getRepushTtlFilterCount() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getRepushTtlFilterCount();
    }
    return delegate.getRepushTtlFilterCount();
  }

  @Override
  public long getTotalPutOrDeleteRecordsCount() {
    if (delegate == null) {
      return DataWriterTaskTracker.super.getTotalPutOrDeleteRecordsCount();
    }
    return delegate.getTotalPutOrDeleteRecordsCount();
  }
}
