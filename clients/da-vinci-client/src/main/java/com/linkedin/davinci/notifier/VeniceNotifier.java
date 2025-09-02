package com.linkedin.davinci.notifier;

import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.pubsub.api.PubSubPosition;
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
   * Consumption is restarted from given position for a store and partition
   */
  default void restarted(String kafkaTopic, int partitionId, PubSubPosition position) {
    restarted(kafkaTopic, partitionId, position, "");
  }

  default void restarted(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
  }

  /**
   * Periodic progress report of consumption for a store and partition.
   */
  default void progress(String kafkaTopic, int partitionId, PubSubPosition position) {
    progress(kafkaTopic, partitionId, position, "");
  }

  default void progress(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
  }

  /**
   * The {@link ControlMessageType#END_OF_PUSH} control message was consumed.
   * <p>
   * This is only emitted for Hybrid Stores, since Batch-Only Stores report
   * {@link #completed(String, int, PubSubPosition)} right away when getting the EOP.
   */
  default void endOfPushReceived(String kafkaTopic, int partitionId, PubSubPosition position) {
    endOfPushReceived(kafkaTopic, partitionId, position, "");
  }

  default void endOfPushReceived(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
  }

  /**
   * The {@link ControlMessageType#TOPIC_SWITCH} control message was consumed.
   * <p>
   * This is only emitted for Hybrid Stores using Leader/Follower model, after the report of
   * {@link #endOfPushReceived(String, int, PubSubPosition)} and before {@link #completed(String, int, PubSubPosition)}.
   */
  default void topicSwitchReceived(String kafkaTopic, int partitionId, PubSubPosition position) {
    topicSwitchReceived(kafkaTopic, partitionId, position, "");
  }

  default void topicSwitchReceived(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
  }

  default void dataRecoveryCompleted(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
  }

  /**
   * Consumption is started for an incremental push
   */
  default void startOfIncrementalPushReceived(String kafkaTopic, int partitionId, PubSubPosition position) {
    startOfIncrementalPushReceived(kafkaTopic, partitionId, position, "");
  }

  default void startOfIncrementalPushReceived(
      String kafkaTopic,
      int partitionId,
      PubSubPosition position,
      String message) {
  }

  /**
   * Consumption is completed for an incremental push
   */
  default void endOfIncrementalPushReceived(String kafkaTopic, int partitionId, PubSubPosition position) {
    endOfIncrementalPushReceived(kafkaTopic, partitionId, position, "");
  }

  default void endOfIncrementalPushReceived(
      String kafkaTopic,
      int partitionId,
      PubSubPosition position,
      String message) {
  }

  default void batchEndOfIncrementalPushReceived(
      String kafkaTopic,
      int partitionId,
      PubSubPosition position,
      List<String> historicalIncPushes) {
  }

  default void catchUpVersionTopicOffsetLag(String kafkaTopic, int partitionId) {
  }

  /**
   * Consumption is completed for a store and partition.
   */
  default void completed(String kafkaTopic, int partitionId, PubSubPosition position) {
    completed(kafkaTopic, partitionId, position, "");
  }

  default void completed(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
  }

  /**
   * Quota is violated for a store.
   */
  default void quotaViolated(String kafkaTopic, int partitionId, PubSubPosition position) {
    quotaViolated(kafkaTopic, partitionId, position, "");
  }

  default void quotaViolated(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
  }

  /**
   * Quota is not violated for a store.
   */
  default void quotaNotViolated(String kafkaTopic, int partitionId, PubSubPosition position) {
    quotaNotViolated(kafkaTopic, partitionId, position, "");
  }

  default void quotaNotViolated(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
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

  default void stopped(String kafkaTopic, int partitionId, PubSubPosition position) {
    // no-op
  }
}
