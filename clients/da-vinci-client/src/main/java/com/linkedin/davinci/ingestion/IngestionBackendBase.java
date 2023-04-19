package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import java.io.Closeable;
import java.util.Optional;


public interface IngestionBackendBase extends Closeable {
  default void startConsumption(VeniceStoreVersionConfig storeConfig, int partition) {
    startConsumption(storeConfig, partition, Optional.empty());
  }

  void startConsumption(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      Optional<LeaderFollowerStateType> leaderState);

  void stopConsumption(VeniceStoreVersionConfig storeConfig, int partition);

  void killConsumptionTask(String topicName);

  void shutdownIngestionTask(String topicName);

  void addIngestionNotifier(VeniceNotifier ingestionListener);

  /**
   * This method stops to subscribe the specified topic partition and delete partition data from storage and it will
   * always drop empty storage engine.
   * @param storeConfig Store version config
   * @param partition Partition number to be dropped in the store version.
   * @param timeoutInSeconds Number of seconds to wait before timeout.
   */
  default void dropStoragePartitionGracefully(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      int timeoutInSeconds) {
    dropStoragePartitionGracefully(storeConfig, partition, timeoutInSeconds, true);
  }

  /**
   * This method stops to subscribe the specified topic partition and delete partition data from storage.
   * @param storeConfig Store version config
   * @param partition Partition number to be dropped in the store version.
   * @param timeoutInSeconds Number of seconds to wait before timeout.
   * @param removeEmptyStorageEngine Whether to drop storage engine when dropping the last partition.
   */
  void dropStoragePartitionGracefully(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      int timeoutInSeconds,
      boolean removeEmptyStorageEngine);

  StorageMetadataService getStorageMetadataService();

  KafkaStoreIngestionService getStoreIngestionService();

  StorageService getStorageService();
}
