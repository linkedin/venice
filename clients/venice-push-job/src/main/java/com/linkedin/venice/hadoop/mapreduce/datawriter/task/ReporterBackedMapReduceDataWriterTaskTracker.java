package com.linkedin.venice.hadoop.mapreduce.datawriter.task;

import com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import org.apache.hadoop.mapred.Reporter;


/**
 * An interface to report and retrieve metrics related to data writer tasks during the execution of a MapReduce job.
 */
public class ReporterBackedMapReduceDataWriterTaskTracker implements DataWriterTaskTracker {
  private final Reporter reporter;

  public ReporterBackedMapReduceDataWriterTaskTracker(Reporter reporter) {
    if (reporter != null) {
      this.reporter = reporter;
    } else {
      this.reporter = Reporter.NULL;
    }
  }

  public Reporter getReporter() {
    return reporter;
  }

  @Override
  public void heartbeat() {
    reporter.progress();
  }

  @Override
  public float getProgress() {
    return reporter.getProgress();
  }

  @Override
  public void trackSprayAllPartitions() {
    MRJobCounterHelper.incrMapperSprayAllPartitionsTriggeredCount(reporter, 1);
  }

  @Override
  public void trackEmptyRecord() {
    MRJobCounterHelper.incrEmptyRecordCount(reporter, 1);
  }

  @Override
  public void trackKeySize(int size) {
    MRJobCounterHelper.incrTotalKeySize(reporter, size);
  }

  @Override
  public void trackUncompressedValueSize(int size) {
    MRJobCounterHelper.incrTotalUncompressedValueSize(reporter, size);
  }

  @Override
  public void trackCompressedValueSize(int size) {
    MRJobCounterHelper.incrTotalValueSize(reporter, size);
  }

  @Override
  public void trackGzipCompressedValueSize(int size) {
    MRJobCounterHelper.incrTotalGzipCompressedValueSize(reporter, size);
  }

  @Override
  public void trackZstdCompressedValueSize(int size) {
    MRJobCounterHelper.incrTotalZstdCompressedValueSize(reporter, size);
  }

  @Override
  public void trackWriteAclAuthorizationFailure() {
    MRJobCounterHelper.incrWriteAclAuthorizationFailureCount(reporter, 1);
  }

  @Override
  public void trackRecordTooLargeFailure() {
    MRJobCounterHelper.incrRecordTooLargeFailureCount(reporter, 1);
  }

  @Override
  public void trackUncompressedRecordTooLargeFailure() {
    MRJobCounterHelper.incrUncompressedRecordTooLargeFailureCount(reporter, 1);
  }

  @Override
  public void trackRecordSentToPubSub() {
    MRJobCounterHelper.incrOutputRecordCount(reporter, 1);
  }

  @Override
  public void trackDuplicateKeyWithDistinctValue(int count) {
    MRJobCounterHelper.incrDuplicateKeyWithDistinctValue(reporter, count);
  }

  @Override
  public void trackDuplicateKeyWithIdenticalValue(int count) {
    MRJobCounterHelper.incrDuplicateKeyWithIdenticalValue(reporter, count);
  }

  @Override
  public void trackRepushTtlFilteredRecord() {
    MRJobCounterHelper.incrRepushTtlFilterCount(reporter, 1L);
  }

  @Override
  public void trackPartitionWriterClose() {
    MRJobCounterHelper.incrReducerClosedCount(reporter, 1);
  }

  @Override
  public void trackPutOrDeleteRecord() {
    MRJobCounterHelper.incrTotalPutOrDeleteRecordCount(reporter, 1);
  }

  @Override
  public void trackIncrementalPushThrottledTime(long timeMs) {
    MRJobCounterHelper.incrIncrementalPushThrottleTime(reporter, timeMs);
  }

  @Override
  public long getIncrementalPushThrottledTimeMs() {
    return MRJobCounterHelper.getIncrementalPushThrottleTimeMs(reporter);
  }

  @Override
  public long getTotalKeySize() {
    return MRJobCounterHelper.getTotalKeySize(reporter);
  }

  @Override
  public long getTotalValueSize() {
    return MRJobCounterHelper.getTotalValueSize(reporter);
  }

  @Override
  public long getRecordTooLargeFailureCount() {
    return MRJobCounterHelper.getRecordTooLargeFailureCount(reporter);
  }

  @Override
  public long getUncompressedRecordTooLargeFailureCount() {
    return MRJobCounterHelper.getUncompressedRecordTooLargeFailureCount(reporter);
  }

  @Override
  public long getWriteAclAuthorizationFailureCount() {
    return MRJobCounterHelper.getWriteAclAuthorizationFailureCount(reporter);
  }

  @Override
  public long getDuplicateKeyWithDistinctValueCount() {
    return MRJobCounterHelper.getDuplicateKeyWithDistinctCount(reporter);
  }

  @Override
  public long getTotalPutOrDeleteRecordsCount() {
    return MRJobCounterHelper.getTotalPutOrDeleteRecordsCount(reporter);
  }
}
