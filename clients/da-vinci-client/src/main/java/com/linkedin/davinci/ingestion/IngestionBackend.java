package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;


public interface IngestionBackend extends Closeable {
  void startConsumption(VeniceStoreVersionConfig storeConfig, int partition, Optional<PubSubPosition> pubSubPosition);

  CompletableFuture<Void> stopConsumption(VeniceStoreVersionConfig storeConfig, int partition);

  /**
   * Cancel any ongoing blob transfer for the given partition.
   * This method blocks until cancellation completes and cleans up the cancellation flag.
   *
   * @param storeConfig Store version config
   * @param partition Partition number to cancel blob transfer for
   * @param timeoutInSeconds Number of seconds to wait before timeout
   */
  void cancelBlobTransferIfInProgress(VeniceStoreVersionConfig storeConfig, int partition, int timeoutInSeconds);

  void killConsumptionTask(String topicName);

  void shutdownIngestionTask(String topicName);

  void addIngestionNotifier(VeniceNotifier ingestionListener);

  /**
   * This method stops to subscribe the specified topic partition and delete partition data from storage and it will
   * always drop empty storage engine.
   * @param storeConfig Store version config
   * @param partition Partition number to be dropped in the store version.
   * @param timeoutInSeconds Number of seconds to wait before timeout.
   * @return a future for the drop partition action.
   */
  default CompletableFuture<Void> dropStoragePartitionGracefully(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      int timeoutInSeconds) {
    return dropStoragePartitionGracefully(storeConfig, partition, timeoutInSeconds, true);
  }

  /**
   * This method stops to subscribe the specified topic partition and delete partition data from storage.
   * @param storeConfig Store version config
   * @param partition Partition number to be dropped in the store version.
   * @param timeoutInSeconds Number of seconds to wait before timeout.
   * @param removeEmptyStorageEngine Whether to drop storage engine when dropping the last partition.
   * @return a future for the drop partition action.
   */
  CompletableFuture<Void> dropStoragePartitionGracefully(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      int timeoutInSeconds,
      boolean removeEmptyStorageEngine);

  KafkaStoreIngestionService getStoreIngestionService();

  // removeStorageEngine removes the whole storage engine and delete all the data from disk.
  void removeStorageEngine(String topicName);

  // setStorageEngineReference is used by Da Vinci exclusively to speed up storage engine retrieval for read path.
  void setStorageEngineReference(String topicName, AtomicReference<StorageEngine> storageEngineReference);

  /**
   * Check whether there are any current version bootstrapping or not.
   */
  boolean hasCurrentVersionBootstrapping();
}
