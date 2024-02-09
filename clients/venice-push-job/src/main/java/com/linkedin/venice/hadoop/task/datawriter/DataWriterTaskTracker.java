package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.hadoop.task.TaskTracker;


/**
 * An interface to report and retrieve metrics related to data writer tasks.
 */
public interface DataWriterTaskTracker extends TaskTracker {
  default void trackSprayAllPartitions() {
  }

  default void trackEmptyRecord() {
  }

  default void trackKeySize(int size) {
  }

  default void trackUncompressedValueSize(int size) {
  }

  default void trackCompressedValueSize(int size) {
  }

  default void trackGzipCompressedValueSize(int size) {
  }

  default void trackZstdCompressedValueSize(int size) {
  }

  default void trackWriteAclAuthorizationFailure() {
  }

  default void trackRecordTooLargeFailure() {
  }

  default void trackRecordSentToPubSub() {
  }

  default void trackDuplicateKeyWithDistinctValue(int count) {
  }

  default void trackDuplicateKeyWithIdenticalValue(int count) {
  }

  default void trackRepushTtlFilteredRecord() {
  }

  default void trackPutOrDeleteRecord() {
  }

  default void trackPartitionWriterClose() {
  }

  default long getSprayAllPartitionsCount() {
    return 0;
  }

  default long getTotalKeySize() {
    return 0;
  }

  default long getTotalValueSize() {
    return 0;
  }

  default long getTotalUncompressedValueSize() {
    return 0;
  }

  default long getTotalGzipCompressedValueSize() {
    return 0;
  }

  default long getTotalZstdCompressedValueSize() {
    return 0;
  }

  default long getRecordTooLargeFailureCount() {
    return 0;
  }

  default long getWriteAclAuthorizationFailureCount() {
    return 0;
  }

  default long getDuplicateKeyWithDistinctValueCount() {
    return 0;
  }

  default long getOutputRecordsCount() {
    return 0;
  }

  default long getPartitionWriterCloseCount() {
    return 0;
  }

  default long getRepushTtlFilterCount() {
    return 0;
  }

  default long getTotalPutOrDeleteRecordsCount() {
    return 0;
  }
}
