package com.linkedin.venice.notifier;

import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
/**
 * Interface for listening to Notifications for Store consumption.
 */
public interface VeniceNotifier {

    /**
     * Consumption is started for a store and partition
     */
    default void started(String storeName, int partitionId) {
        started(storeName, partitionId, "");
    }

    void started(String storeName, int partitionId, String message);

    /**
     * Consumption is restarted from given offset for a store and partition
     */
    default void restarted(String storeName, int partitionId, long offset) {
        restarted(storeName, partitionId, offset, "");
    }

    void restarted(String storeName, int partitionId, long offset, String message);

    /**
     * Periodic progress report of consumption for a store and partition.
     */
    default void progress(String storeName, int partitionId, long offset) {
        progress(storeName, partitionId, offset, "");
    }

    void progress(String storeName, int partitionId, long offset, String message);

    /**
     * The {@link ControlMessageType#END_OF_PUSH} control message was consumed.
     *
     * This is only emitted for Hybrid Stores, since Batch-Only Stores report
     * {@link #completed(String, int, long)} right away when getting the EOP.
     */
    default void endOfPushReceived(String storeName, int partitionId, long offset) {
        endOfPushReceived(storeName, partitionId, offset, "");
    }

    void endOfPushReceived(String storeName, int partitionId, long offset, String message);

    /**
     * The {@link ControlMessageType#START_OF_BUFFER_REPLAY} control message was consumed.
     *
     * This is only emitted for Hybrid Stores using Online/Offline model, after the report of
     * {@link #endOfPushReceived(String, int, long)} and before {@link #completed(String, int, long)}.
     */
    default void startOfBufferReplayReceived(String storeName, int partitionId, long offset) {
        startOfBufferReplayReceived(storeName, partitionId, offset, "");
    }

    /**
     * The {@link ControlMessageType#TOPIC_SWITCH} control message was consumed.
     *
     * This is only emitted for Hybrid Stores using Leader/Follower model, after the report of
     * {@link #endOfPushReceived(String, int, long)} and before {@link #completed(String, int, long)}.
     */
    default void topicSwitchReceived(String storeName, int partitionId, long offset) {
        topicSwitchReceived(storeName, partitionId, offset, "");
    }

    void startOfBufferReplayReceived(String storeName, int partitionId, long offset, String message);

    void topicSwitchReceived(String storeName, int partitionId, long offset, String message);

    /**
     * Consumption is started for an incremental push
     */
    default void startOfIncrementalPushReceived(String storeName, int partitionId, long offset) {
        startOfIncrementalPushReceived(storeName, partitionId, offset, "");
    }

    void startOfIncrementalPushReceived(String storeName, int partitionId, long offset, String message);

    /**
     * Consumption is completed for an incremental push
     */
    default void endOfIncrementalPushReceived(String storeName, int partitionId, long offset) {
        endOfIncrementalPushReceived(storeName, partitionId, offset, "");
    }

    void endOfIncrementalPushReceived(String storeName, int partitionId, long offset, String message);

    /**
     * Consumption is completed for a store and partition.
     */
    default void completed(String storeName, int partitionId, long offset) {
        completed(storeName, partitionId, offset, "");
    }

    void completed(String storeName, int partitionId, long offset, String message);

    /**
     * The Process is shutting down and clean up the resources associated with the Notifier.
     */
    void close();

    /**
     * Report an error, during the consumption for a Partitions and store.
     * The error may or may not be fatal.
     *  @param storeName storeName
     * @param partitionId partitionId
     * @param message debug error message
     * @param ex exception encountered.
     */
    void error(String storeName, int partitionId, String message, Exception ex);
}
