package com.linkedin.venice.notifier;


/**
 * Interface for listening to Notifications for Store consumption.
 */
public interface VeniceNotifier {

    /**
     * Consumption is started for a store and partition
     * @param storeName storeName
     * @param partitionId  partitionId
     */

    void started(String storeName, int partitionId);

    /**
     * Consumption is restarted from given offset for a store and partition
     *
     * @param storeName
     * @param partitionId
     * @param offset
     */
    void restarted(String storeName, int partitionId, long offset);

    /**
     * Consumption is completed for a store and partition.
     * @param storeName storeName
     * @param partitionId partitionId
     */
    void completed(String storeName, int partitionId, long offset);

    /**
     * Periodic progress report of consumption for a store and partition.
     * @param storeName storeName
     * @param partitionId partitionId
     */
    void progress(String storeName, int partitionId, long offset);

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
