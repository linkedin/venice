package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import java.io.Closeable;
import java.util.Optional;


public interface IngestionBackendBase extends Closeable {

  default void startConsumption(VeniceStoreConfig storeConfig, int partition) {
    startConsumption(storeConfig, partition, Optional.empty());
  }

  void startConsumption(VeniceStoreConfig storeConfig, int partition, Optional<LeaderFollowerStateType> leaderState);

  void stopConsumption(VeniceStoreConfig storeConfig, int partition);

  void killConsumptionTask(String topicName);

  // addIngestionNotifier adds ingestion listener to KafkaStoreIngestionService
  void addIngestionNotifier(VeniceNotifier ingestionListener);

  // dropStoragePartitionGracefully will stop subscribe topic's partition and delete partition data from storage.
  void dropStoragePartitionGracefully(VeniceStoreConfig storeConfig, int partition, int timeoutInSeconds);

  StorageMetadataService getStorageMetadataService();

  KafkaStoreIngestionService getStoreIngestionService();

  StorageService getStorageService();
}
