package com.linkedin.davinci.notifier;

import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import java.io.Closeable;
import java.util.List;


/**
 * Interface for listening to Notifications for Store consumption.
 */
public interface VeniceNotifier extends Closeable {
  /**
   * Consumption is started for a store and partition
   */
  default void started(String kafkaTopic, int partitionId) {
    started(kafkaTopic, partitionId, "");
  }

  default void started(String kafkaTopic, int partitionId, String message) {
  }

  /**
   * Consumption is restarted from given offset for a store and partition
   */
  default void restarted(String kafkaTopic, int partitionId, long offset) {
    restarted(kafkaTopic, partitionId, offset, "");
  }

  default void restarted(String kafkaTopic, int partitionId, long offset, String message) {
  }

  /**
   * Periodic progress report of consumption for a store and partition.
   */
  default void progress(String kafkaTopic, int partitionId, long offset) {
    progress(kafkaTopic, partitionId, offset, "");
  }

  default void progress(String kafkaTopic, int partitionId, long offset, String message) {
  }

  /**
   * The {@link ControlMessageType#END_OF_PUSH} control message was consumed.
   * <p>
   * This is only emitted for Hybrid Stores, since Batch-Only Stores report
   * {@link #completed(String, int, long)} right away when getting the EOP.
   */
  default void endOfPushReceived(String kafkaTopic, int partitionId, long offset) {
    endOfPushReceived(kafkaTopic, partitionId, offset, "");
  }

  default void endOfPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
  }

  /**
   * The {@link ControlMessageType#TOPIC_SWITCH} control message was consumed.
   * <p>
   * This is only emitted for Hybrid Stores using Leader/Follower model, after the report of
   * {@link #endOfPushReceived(String, int, long)} and before {@link #completed(String, int, long)}.
   */
  default void topicSwitchReceived(String kafkaTopic, int partitionId, long offset) {
    topicSwitchReceived(kafkaTopic, partitionId, offset, "");
  }

  default void topicSwitchReceived(String kafkaTopic, int partitionId, long offset, String message) {
  }

  default void dataRecoveryCompleted(String kafkaTopic, int partitionId, long offset, String message) {
  }

  /**
   * Consumption is started for an incremental push
   */
  default void startOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset) {
    startOfIncrementalPushReceived(kafkaTopic, partitionId, offset, "");
  }

  default void startOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
  }

  /**
   * Consumption is completed for an incremental push
   */
  default void endOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset) {
    endOfIncrementalPushReceived(kafkaTopic, partitionId, offset, "");
  }

  default void endOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
  }

  default void batchEndOfIncrementalPushReceived(
      String kafkaTopic,
      int partitionId,
      long offset,
      List<String> historicalIncPushes) {
  }

  default void catchUpVersionTopicOffsetLag(String kafkaTopic, int partitionId) {
  }

  /**
   * Consumption is completed for a store and partition.
   */
  default void completed(String kafkaTopic, int partitionId, long offset) {
    completed(kafkaTopic, partitionId, offset, "");
  }

  default void completed(String kafkaTopic, int partitionId, long offset, String message) {
  }

  /**
   * Quota is violated for a store.
   */
  default void quotaViolated(String kafkaTopic, int partitionId, long offset) {
    quotaViolated(kafkaTopic, partitionId, offset, "");
  }

  default void quotaViolated(String kafkaTopic, int partitionId, long offset, String message) {
  }

  /**
   * Quota is not violated for a store.
   */
  default void quotaNotViolated(String kafkaTopic, int partitionId, long offset) {
    quotaNotViolated(kafkaTopic, partitionId, offset, "");
  }

  default void quotaNotViolated(String kafkaTopic, int partitionId, long offset, String message) {
  }

  /**
   * The Process is shutting down and clean up the resources associated with the Notifier.
   * N.B. When implementing the method, make it idempotent.
   */
  default void close() {
  }

  /**
   * Report an error, during the consumption for a Partitions and store. The error may or may not be fatal.
   */
  default void error(String kafkaTopic, int partitionId, String message, Exception e) {
  }

  default void stopped(String kafkaTopic, int partitionId, long offset) {
    // no-op
  }
}
