package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * An interface for Store Ingestion Service for Venice.
 */
public interface StoreIngestionService {
  /**
   * Starts consuming messages from Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  void startConsumption(VeniceStoreVersionConfig veniceStore, int partitionId);

  /**
   * Stops consuming messages from Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  CompletableFuture<Void> stopConsumption(VeniceStoreVersionConfig veniceStore, int partitionId);

  /**
   * Stops consuming messages from Kafka Partition corresponding to Venice Partition and wait up to
   * (sleepSeconds * numRetires) to make sure partition consumption is stopped.
   */
  void stopConsumptionAndWait(
      VeniceStoreVersionConfig veniceStore,
      int partitionId,
      int sleepSeconds,
      int numRetries,
      boolean whetherToResetOffset);

  /**
   * Kill all of running consumptions of given store.
   *
   * @param topicName Venice topic (store and version number) for the corresponding consumer task that needs to be killed.
   */
  boolean killConsumptionTask(String topicName);

  void promoteToLeader(
      VeniceStoreVersionConfig veniceStoreVersionConfig,
      int partitionId,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker);

  void demoteToStandby(
      VeniceStoreVersionConfig veniceStoreVersionConfig,
      int partitionId,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker);

  /**
   * Adds Notifier to get Notifications for get various status of the consumption
   * tasks like start, completed, progress and error states.
   *
   * Multiple Notifiers can be added for the same consumption tasks and all of them will
   * be notified in order.
   *
   * @param notifier
   */
  void addIngestionNotifier(VeniceNotifier notifier);

  void replaceAndAddTestNotifier(VeniceNotifier notifier);

  /**
   * Check whether there is a running consumption task for given store.
   */
  boolean containsRunningConsumption(VeniceStoreVersionConfig veniceStore);

  /**
   * Check whether there is a running consumption task for given store version topic.
   */
  boolean containsRunningConsumption(String topic);

  /**
   * Check whether the specified partition is still being consumed
   */
  boolean isPartitionConsuming(String topic, int partitionId);

  /**
   * Get topic names that are currently maintained by the ingestion service with corresponding version status not in an
   * online state. Topics with invalid store or version number are also included in the returned list.
   * @return a {@link Set} of topic names.
   */
  Set<String> getIngestingTopicsWithVersionStatusNotOnline();

  void recordIngestionFailure(String storeName);

  /**
   * Get AggVersionedStorageIngestionStats
   * @return an instance of {@link AggVersionedIngestionStats}
   */
  AggVersionedIngestionStats getAggVersionedIngestionStats();

  StoreIngestionTask getStoreIngestionTask(String topic);

  VeniceConfigLoader getVeniceConfigLoader();

  VeniceWriterFactory getVeniceWriterFactory();
}
