package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.notifier.VeniceNotifier;

/**
 * An interface for Store Ingestion Service for Venice.
 */
public interface StoreIngestionService {

  /**
   * Starts consuming messages from Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  void startConsumption(VeniceStoreConfig veniceStore, int partitionId);

  /**
   * Stops consuming messages from Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  void stopConsumption(VeniceStoreConfig veniceStore, int partitionId);

  /**
   * Resets Offset to beginning for Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  void resetConsumptionOffset(VeniceStoreConfig veniceStore, int partitionId);

  /**
   * Kill all of running consumptions of given store.
   *
   * @param veniceStore Venice Store that consumer task need to be killed belong to.
   */
  void killConsumptionTask(VeniceStoreConfig veniceStore);

  /**
   * Adds Notifier to get Notifications for get various status of the consumption
   * tasks like start, completed, progress and error states.
   *
   * Multiple Notifiers can be added for the same consumption tasks and all of them will
   * be notified in order.
   *
   * @param notifier
   */
  void addNotifier(VeniceNotifier notifier);

  /**
   * Judge whether there is a running consumption task for given store.
   */
  boolean containsRunningConsumption(VeniceStoreConfig veniceStore);

  /**
   * Check whether the specified partition is still being consumed
   */
  boolean isPartitionConsuming(VeniceStoreConfig veniceStore, int partitionId);
}
