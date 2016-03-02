package com.linkedin.venice.kafka.consumer;

import java.util.Collection;


/**
 * Interface for listening to Notifications for Store consumption.
 */
public interface VeniceNotifier {

    /**
     * Consumption is started for a store and partition
     *
     * @param jobId jobId for the consumption
     * @param storeName storeName
     * @param partitionId  partitionId
     */

    void started(long jobId, String storeName, int partitionId);

    /**
     * Consumption is completed for a store and partition.
     *
     * @param jobId jobId for the consumption
     * @param storeName storeName
     * @param partitionId partitionId
     * @param counter number of messages consumed. This may not reflect the total messages consumed,
     *                if the process is restarted.
     */
    void completed(long jobId, String storeName, int partitionId, long counter);

    /**
     * Periodic progress report of consumption for a store and partition.
     *
     * @param jobId JobId for the consumption
     * @param storeName storeName
     * @param partitionId partitionId
     * @param counter numbers of messages consumed so far, it will never decrease unless the process
     *                is restarted.
     */
    void progress(long jobId, String storeName, int partitionId, long counter);

    /**
     * The Process is shutting down and clean up the resources associated with the Notifier.
     */
    void close();

    /**
     * Report an error, during the consumption for a Partitions and store.
     * The error may or may not be fatal.
     *
     * @param jobId JobId of the consumption.
     * @param storeName storeName
     * @param partitions collection of partitions where error is encountered.
     * @param message debug error message
     * @param ex exception encountered.
     */
    void error(long jobId, String storeName, Collection<Integer> partitions , String message, Exception ex);
}
