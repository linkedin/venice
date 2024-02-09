package com.linkedin.venice.hadoop.mapreduce.datawriter.task;

import com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import org.apache.hadoop.mapred.Counters;


/**
 * An interface to retrieve metrics related to data writer tasks after a MapReduce job has completed.
 */
public class CounterBackedMapReduceDataWriterTaskTracker implements DataWriterTaskTracker {
  private final Counters counters;

  public CounterBackedMapReduceDataWriterTaskTracker(Counters counters) {
    this.counters = counters;
  }

  @Override
  public long getSprayAllPartitionsCount() {
    return MRJobCounterHelper.getMapperSprayAllPartitionsTriggeredCount(counters);
  }

  @Override
  public long getTotalKeySize() {
    return MRJobCounterHelper.getTotalKeySize(counters);
  }

  @Override
  public long getTotalValueSize() {
    return MRJobCounterHelper.getTotalValueSize(counters);
  }

  @Override
  public long getTotalUncompressedValueSize() {
    return MRJobCounterHelper.getTotalUncompressedValueSize(counters);
  }

  @Override
  public long getTotalGzipCompressedValueSize() {
    return MRJobCounterHelper.getTotalGzipCompressedValueSize(counters);
  }

  @Override
  public long getTotalZstdCompressedValueSize() {
    return MRJobCounterHelper.getTotalZstdWithDictCompressedValueSize(counters);
  }

  @Override
  public long getRecordTooLargeFailureCount() {
    return MRJobCounterHelper.getRecordTooLargeFailureCount(counters);
  }

  @Override
  public long getWriteAclAuthorizationFailureCount() {
    return MRJobCounterHelper.getWriteAclAuthorizationFailureCount(counters);
  }

  @Override
  public long getDuplicateKeyWithDistinctValueCount() {
    return MRJobCounterHelper.getDuplicateKeyWithDistinctCount(counters);
  }

  @Override
  public long getOutputRecordsCount() {
    return MRJobCounterHelper.getOutputRecordsCount(counters);
  }

  @Override
  public long getPartitionWriterCloseCount() {
    return MRJobCounterHelper.getReducerClosedCount(counters);
  }

  @Override
  public long getRepushTtlFilterCount() {
    return MRJobCounterHelper.getRepushTtlFilterCount(counters);
  }

  @Override
  public long getTotalPutOrDeleteRecordsCount() {
    return MRJobCounterHelper.getTotalPutOrDeleteRecordsCount(counters);
  }
}
