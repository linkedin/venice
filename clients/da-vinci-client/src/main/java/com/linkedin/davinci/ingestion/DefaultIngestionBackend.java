package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.blobtransfer.BlobTransferManager;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The default ingestion backend implementation. Ingestion will be done in the same JVM as the application.
 */
public class DefaultIngestionBackend implements IngestionBackend {
  private static final Logger LOGGER = LogManager.getLogger(DefaultIngestionBackend.class);
  private final StorageMetadataService storageMetadataService;
  private final StorageService storageService;
  private final KafkaStoreIngestionService storeIngestionService;
  private final Map<String, AtomicReference<AbstractStorageEngine>> topicStorageEngineReferenceMap =
      new VeniceConcurrentHashMap<>();
  private static final ScheduledExecutorService blobTransferScheduler = Executors.newScheduledThreadPool(1);
  private final BlobTransferManager blobTransferManager;

  public DefaultIngestionBackend(
      StorageMetadataService storageMetadataService,
      KafkaStoreIngestionService storeIngestionService,
      StorageService storageService,
      BlobTransferManager blobTransferManager) {
    this.storageMetadataService = storageMetadataService;
    this.storeIngestionService = storeIngestionService;
    this.storageService = storageService;
    this.blobTransferManager = blobTransferManager;
  }

  @Override
  public void startConsumption(VeniceStoreVersionConfig storeConfig, int partition) {
    String storeVersion = storeConfig.getStoreVersionName();
    LOGGER.info("Retrieving storage engine for store {} partition {}", storeVersion, partition);
    Pair<Store, Version> storeAndVersion =
        Utils.waitStoreVersionOrThrow(storeVersion, getStoreIngestionService().getMetadataRepo());
    Supplier<StoreVersionState> svsSupplier = () -> storageMetadataService.getStoreVersionState(storeVersion);
    AbstractStorageEngine storageEngine = storageService.openStoreForNewPartition(storeConfig, partition, svsSupplier);
    topicStorageEngineReferenceMap.compute(storeVersion, (key, storageEngineAtomicReference) -> {
      if (storageEngineAtomicReference != null) {
        storageEngineAtomicReference.set(storageEngine);
      }
      return storageEngineAtomicReference;
    });

    CompletionStage<Void> bootstrapFuture =
        bootstrapFromBlobs(storeAndVersion.getFirst(), storeAndVersion.getSecond().getNumber(), partition);

    bootstrapFuture.whenComplete((result, throwable) -> {
      LOGGER.info(
          "Retrieved storage engine for store {} partition {}. Starting consumption in ingestion service",
          storeVersion,
          partition);
      getStoreIngestionService().startConsumption(storeConfig, partition);
      LOGGER.info(
          "Completed starting consumption in ingestion service for store {} partition {}",
          storeVersion,
          partition);
    });
  }

  /**
   * Bootstrap from the blobs from another source (like another peer). If it fails (due to the 30-minute timeout or
   * any exceptions), it deletes the partially downloaded blobs, and eventually falls back to bootstrapping from Kafka.
   * Blob transfer should be enabled to boostrap from blobs, and it currently only supports batch-stores.
   */
  private CompletionStage<Void> bootstrapFromBlobs(Store store, int versionNumber, int partitionId) {
    if (!store.isBlobTransferEnabled() || store.isHybrid()) {
      return CompletableFuture.completedFuture(null);
    }

    String storeName = store.getName();
    CompletionStage<InputStream> p2pTransfer = blobTransferManager.get(storeName, versionNumber, partitionId);
    CompletableFuture<Void> result = new CompletableFuture<>();

    blobTransferScheduler.schedule(() -> {
      if (!result.isDone()) {
        handleDeleteAndComplete(
            storeName,
            versionNumber,
            partitionId,
            new TimeoutException("Blob Transfer timed out after 30 minutes"),
            result);
      }
    }, 30, TimeUnit.MINUTES);

    p2pTransfer.thenAccept(inputStream -> {
      if (!result.isDone()) {
        result.complete(null);
      }
    }).exceptionally(throwable -> {
      if (!result.isDone()) {
        handleDeleteAndComplete(storeName, versionNumber, partitionId, throwable, result);
      }
      return null;
    });

    return result;
  }

  private void handleDeleteAndComplete(
      String storeName,
      int versionNumber,
      int partitionId,
      Throwable throwable,
      CompletableFuture<Void> result) {
    blobTransferManager.delete(storeName, versionNumber, partitionId)
        .thenAccept(deleteResult -> result.completeExceptionally(throwable))
        .exceptionally(deleteThrowable -> {
          if (deleteThrowable != null) {
            result.completeExceptionally(
                new RuntimeException("Blob transfer failed and deletion failed: " + deleteThrowable, throwable));
          } else {
            result.completeExceptionally(throwable);
          }
          return null;
        });
  }

  @Override
  public CompletableFuture<Void> stopConsumption(VeniceStoreVersionConfig storeConfig, int partition) {
    return getStoreIngestionService().stopConsumption(storeConfig, partition);
  }

  @Override
  public void killConsumptionTask(String topicName) {
    getStoreIngestionService().killConsumptionTask(topicName);
  }

  @Override
  public void shutdownIngestionTask(String topicName) {
    getStoreIngestionService().shutdownStoreIngestionTask(topicName);
  }

  @Override
  public void removeStorageEngine(String topicName) {
    this.storageService.removeStorageEngine(topicName);
  }

  @Override
  public void dropStoragePartitionGracefully(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      int timeoutInSeconds,
      boolean removeEmptyStorageEngine) {
    // Stop consumption of the partition.
    final int waitIntervalInSecond = 1;
    final int maxRetry = timeoutInSeconds / waitIntervalInSecond;
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, waitIntervalInSecond, maxRetry, true);
    // Drops corresponding data partition from storage.
    this.storageService.dropStorePartition(storeConfig, partition, removeEmptyStorageEngine);
  }

  @Override
  public void addIngestionNotifier(VeniceNotifier ingestionListener) {
    getStoreIngestionService().addIngestionNotifier(ingestionListener);
  }

  @Override
  public void setStorageEngineReference(
      String topicName,
      AtomicReference<AbstractStorageEngine> storageEngineReference) {
    if (storageEngineReference == null) {
      topicStorageEngineReferenceMap.remove(topicName);
    } else {
      topicStorageEngineReferenceMap.put(topicName, storageEngineReference);
    }
  }

  @Override
  public KafkaStoreIngestionService getStoreIngestionService() {
    return storeIngestionService;
  }

  @Override
  public void close() {
    // Do nothing here, since this is only a wrapper class.
  }
}
