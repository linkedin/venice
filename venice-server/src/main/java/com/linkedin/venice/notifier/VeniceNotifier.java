package com.linkedin.venice.notifier;

import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
/**
 * Interface for listening to Notifications for Store consumption.
 */
public interface VeniceNotifier {

    /**
     * Consumption is started for a store and partition
     */

    void started(String storeName, int partitionId);

    /**
     * Consumption is restarted from given offset for a store and partition
     */
    void restarted(String storeName, int partitionId, long offset);

    /**
     * Periodic progress report of consumption for a store and partition.
     */
    void progress(String storeName, int partitionId, long offset);
  
    /**
     * The {@link ControlMessageType#END_OF_PUSH} control message was consumed.
     * 
     * This is only emitted for Hybrid Stores, since Batch-Only Stores report 
     * {@link #completed(String, int, long)} right away when getting the EOP.
     */
    void endOfPushReceived(String storeName, int partitionId, long offset);
  
    /**
     * The {@link ControlMessageType#START_OF_BUFFER_REPLAY} control message was consumed.
     *
     * This is only emitted for Hybrid Stores, after the report of
     * {@link #endOfPushReceived(String, int, long)} and before {@link #completed(String, int, long)}.
     */
    void startOfBufferReplayReceived(String storeName, int partitionId, long offset);

    /**
     * Consumption is completed for a store and partition.
     */
    void completed(String storeName, int partitionId, long offset);
//
//    /**
//     * This flag controls whether completion should be based only on the reception of the EOP
//     * message (false) or also on the replication lag reaching a certain acceptable threshold
//     * (true). The default is false.
//     *
//     * @return true if completion should be blocked by excessive replication lag, false otherwise.
//     */
//    default boolean replicationLagShouldBlockCompletion() {
//      return false;
//    }

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
