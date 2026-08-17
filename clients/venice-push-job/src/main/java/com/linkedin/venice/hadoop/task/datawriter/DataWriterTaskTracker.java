package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.hadoop.task.TaskTracker;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


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

  /**
   * This accumulator performs a 'max' operation, which is not natively supported
   * by either Spark or Hadoop. It is implemented using a custom accumulator in Spark.
   */
  default void trackLargestUncompressedValueSize(int size) {
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

  default void trackUncompressedRecordTooLargeFailure() {
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

  default void trackIncrementalPushThrottledTime(long timeMs) {
  }

  /**
   * Report elapsed wall-clock time this task spent inside the external-storage write path.
   * This covers the complete external leg: the per-region write throttling wait, the
   * {@code ExternalStorageWriter.batchPut} calls including their retries and retry backoff sleeps, the
   * external {@code flush}, and the external {@code close}. It excludes anything spent producing to
   * Venice/Kafka.
   *
   * <p>Callers report monotonic deltas as they accrue, so the aggregate is the <em>sum of per-task
   * durations</em>, not the push's wall-clock duration: with N concurrent data-writer tasks the total can be
   * up to N times the push's elapsed time.
   */
  default void trackExternalStorageWriteTime(long timeMs) {
  }

  /**
   * Report elapsed wall-clock time this task spent invoking the Venice/Kafka write path: the
   * {@code VeniceWriter.put} invocations plus flushing and closing the Venice writer. It excludes anything
   * spent in the external-storage path.
   *
   * <p>Same aggregation semantics as {@link #trackExternalStorageWriteTime(long)}: summed task durations,
   * not push wall-clock time.
   */
  default void trackVeniceWriteTime(long timeMs) {
  }

  /**
   * Report that the external writer for {@code regionName} exhausted its retry budget and was disabled for
   * the remainder of the task. Callers should report each region at most once per task.
   */
  default void trackFailedExternalStorageRegion(String regionName) {
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

  /**
   * This accumulator performs a 'max' operation, which is not natively supported
   * by either Spark or Hadoop. It is implemented using a custom accumulator in Spark.
   */
  default int getLargestUncompressedValueSize() {
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

  default long getUncompressedRecordTooLargeFailureCount() {
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

  default long getIncrementalPushThrottledTimeMs() {
    return 0;
  }

  /**
   * @return the summed per-task duration (ms) spent in the external-storage write path across all successful
   *         task outputs of this push. See {@link #trackExternalStorageWriteTime(long)} for exactly what is
   *         included; this is <em>not</em> the push's wall-clock time.
   */
  default long getExternalStorageWriteTimeMs() {
    return 0;
  }

  /**
   * @return the summed per-task duration (ms) spent in the Venice/Kafka write path across all successful task
   *         outputs of this push. See {@link #trackVeniceWriteTime(long)}; this is <em>not</em> the push's
   *         wall-clock time.
   */
  default long getVeniceWriteTimeMs() {
    return 0;
  }

  /**
   * Returns per-partition record counts collected during the data writer job.
   * For the Spark path, these are collected via {@code collect()} on the DAG output
   *
   * @return Map of partition ID to record count, or empty map if not available.
   */
  default Map<Integer, Long> getPerPartitionRecordCounts() {
    return Collections.emptyMap();
  }

  /**
   * Returns the set of regions whose external writers exhausted retries and were reported by one or more
   * data-writer tasks. Implementations should return an immutable or defensive-copy snapshot.
   */
  default Set<String> getFailedExternalStorageRegions() {
    return Collections.emptySet();
  }
}
