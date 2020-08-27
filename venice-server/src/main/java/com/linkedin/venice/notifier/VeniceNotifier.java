package com.linkedin.venice.notifier;

import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;


/**
 * Interface for listening to Notifications for Store consumption.
 */
public interface VeniceNotifier {
  /**
   * Consumption is started for a store and partition
   */
  default void started(String kafkaTopic, int partitionId) {
    started(kafkaTopic, partitionId, "");
  }

  default void started(String kafkaTopic, int partitionId, String message) {}

  /**
   * Consumption is restarted from given offset for a store and partition
   */
  default void restarted(String kafkaTopic, int partitionId, long offset) {
    restarted(kafkaTopic, partitionId, offset, "");
  }

  default void restarted(String kafkaTopic, int partitionId, long offset, String message) {}

  /**
   * Periodic progress report of consumption for a store and partition.
   */
  default void progress(String kafkaTopic, int partitionId, long offset) {
    progress(kafkaTopic, partitionId, offset, "");
  }

  default void progress(String kafkaTopic, int partitionId, long offset, String message) {}

  /**
   * The {@link ControlMessageType#END_OF_PUSH} control message was consumed.
   * <p>
   * This is only emitted for Hybrid Stores, since Batch-Only Stores report
   * {@link #completed(String, int, long)} right away when getting the EOP.
   */
  default void endOfPushReceived(String kafkaTopic, int partitionId, long offset) {
    endOfPushReceived(kafkaTopic, partitionId, offset, "");
  }

  default void endOfPushReceived(String kafkaTopic, int partitionId, long offset, String message) {}

  /**
   * The {@link ControlMessageType#START_OF_BUFFER_REPLAY} control message was consumed.
   * <p>
   * This is only emitted for Hybrid Stores using Online/Offline model, after the report of
   * {@link #endOfPushReceived(String, int, long)} and before {@link #completed(String, int, long)}.
   */
  default void startOfBufferReplayReceived(String kafkaTopic, int partitionId, long offset) {
    startOfBufferReplayReceived(kafkaTopic, partitionId, offset, "");
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

  default void startOfBufferReplayReceived(String kafkaTopic, int partitionId, long offset, String message) {}

  default void topicSwitchReceived(String kafkaTopic, int partitionId, long offset, String message) {}

  /**
   * Consumption is started for an incremental push
   */
  default void startOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset) {
    startOfIncrementalPushReceived(kafkaTopic, partitionId, offset, "");
  }

  default void startOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String message) {}

  /**
   * Consumption is completed for an incremental push
   */
  default void endOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset) {
    endOfIncrementalPushReceived(kafkaTopic, partitionId, offset, "");
  }

  default void endOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String message) {}

  default void catchUpBaseTopicOffsetLag(String kafkaTopic, int partitionId) {}

  /**
   * Consumption is completed for a store and partition.
   */
  default void completed(String kafkaTopic, int partitionId, long offset) {
    completed(kafkaTopic, partitionId, offset, "");
  }

  default void completed(String kafkaTopic, int partitionId, long offset, String message) {}

  /**
   * The Process is shutting down and clean up the resources associated with the Notifier.
   * N.B. When implementing the method, make it idempotent.
   */
  default void close() {}

  /**
   * Report an error, during the consumption for a Partitions and store. The error may or may not be fatal.
   */
  default void error(String kafkaTopic, int partitionId, String message, Exception e) {}
}
