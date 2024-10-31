package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.store.AbstractStorageEngine;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;


public interface IngestionBackend extends Closeable {
  void startConsumption(VeniceStoreVersionConfig storeConfig, int partition);

  CompletableFuture<Void> stopConsumption(VeniceStoreVersionConfig storeConfig, int partition);

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

  KafkaStoreIngestionService getStoreIngestionService();

  // removeStorageEngine removes the whole storage engine and delete all the data from disk.
  void removeStorageEngine(String topicName);

  // setStorageEngineReference is used by Da Vinci exclusively to speed up storage engine retrieval for read path.
  void setStorageEngineReference(String topicName, AtomicReference<AbstractStorageEngine> storageEngineReference);

  /**
   * Check whether there are any current version bootstrapping or not.
   */
  boolean hasCurrentVersionBootstrapping();
}
