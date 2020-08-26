package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.helix.LeaderFollowerParticipantModel;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedStorageIngestionStats;
import java.util.Set;


/**
 * An interface for Store Ingestion Service for Venice.
 */
public interface StoreIngestionService {

  /**
   * Starts consuming messages from Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  void startConsumption(VeniceStoreConfig veniceStore, int partitionId, boolean isLeaderFollowerModel);

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
   * @param topicName Venice topic (store and version number) for the corresponding consumer task that needs to be killed.
   */
  boolean killConsumptionTask(String topicName);

//  void promoteToStandby(VeniceStoreConfig veniceStoreConfig, int partitionId, long sessionId);

  void promoteToLeader(VeniceStoreConfig veniceStoreConfig, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker);

  void demoteToStandby(VeniceStoreConfig veniceStoreConfig, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker);

  /**
   * Adds Notifier to get Notifications for get various status of the consumption
   * tasks like start, completed, progress and error states.
   *
   * Multiple Notifiers can be added for the same consumption tasks and all of them will
   * be notified in order.
   *
   * The notifier added here is used in both Online/Offline and Leader/Follower
   * consumption tasks.
   *
   * @param notifier
   */
  void addCommonNotifier(VeniceNotifier notifier);

  /**
   * The notifier added here is only used in Online/Offline consumption task.
   *
   * @param notifier
   */
  void addOnlineOfflineModelNotifier(VeniceNotifier notifier);

  /**
   * The notifier added here is only used in Leader/Follower consumption task.
   *
   * @param notifier
   */
  void addLeaderFollowerModelNotifier(VeniceNotifier notifier);

  /**
   * Check whether there is a running consumption task for given store.
   */
  boolean containsRunningConsumption(VeniceStoreConfig veniceStore);

  /**
   * Check whether there is a running consumption task for given store version topic.
   */
  boolean containsRunningConsumption(String topic);

  /**
   * Check whether the specified partition is still being consumed
   */
  boolean isPartitionConsuming(VeniceStoreConfig veniceStore, int partitionId);

  /**
   * Get topic names that are currently maintained by the ingestion service with corresponding version status not in an
   * online state. Topics with invalid store or version number are also included in the returned list.
   * @return a {@link Set} of topic names.
   */
  Set<String> getIngestingTopicsWithVersionStatusNotOnline();

  /**
   * Get AggStoreIngestionStats
   * @return an instance of {@link AggStoreIngestionStats}
   */
  AggStoreIngestionStats getAggStoreIngestionStats();

  /**
   * Get AggVersionedStorageIngestionStats
   * @return an instance of {@link AggVersionedStorageIngestionStats}
   */
  AggVersionedStorageIngestionStats getAggVersionedStorageIngestionStats();

  StoreIngestionTask getStoreIngestionTask(String topic);
}
