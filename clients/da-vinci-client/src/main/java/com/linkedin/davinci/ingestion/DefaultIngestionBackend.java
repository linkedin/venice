package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceConfigLoader;
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
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
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
  private final VeniceConfigLoader configLoader;
  private final Map<String, AtomicReference<AbstractStorageEngine>> topicStorageEngineReferenceMap =
      new VeniceConcurrentHashMap<>();
  private final BlobTransferManager blobTransferManager;

  public DefaultIngestionBackend(
      StorageMetadataService storageMetadataService,
      KafkaStoreIngestionService storeIngestionService,
      StorageService storageService,
      BlobTransferManager blobTransferManager,
      VeniceConfigLoader configLoader) {
    this.storageMetadataService = storageMetadataService;
    this.storeIngestionService = storeIngestionService;
    this.storageService = storageService;
    this.blobTransferManager = blobTransferManager;
    this.configLoader = configLoader;
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
    if (!store.isBlobTransferEnabled() || store.isHybrid() || blobTransferManager == null) {
      return CompletableFuture.completedFuture(null);
    }

    String storeName = store.getName();
    CompletionStage<InputStream> p2pTransfer = blobTransferManager.get(storeName, versionNumber, partitionId);

    LOGGER
        .info("Bootstrapping from blobs for store {}, version {}, partition {}", storeName, versionNumber, partitionId);

    CompletableFuture<Void> result = new CompletableFuture<>();

    try {
      p2pTransfer.toCompletableFuture().get(30, TimeUnit.MINUTES);
      result.complete(null);
    } catch (Exception e) {
      LOGGER.error(
          "Failed bootstrapping from blobs for store {}, version {}, partition {}",
          storeName,
          versionNumber,
          partitionId,
          e);
      deleteBlobFiles(storeName, versionNumber, partitionId).thenAccept(deleteResult -> result.completeExceptionally(e))
          .exceptionally(throwable -> {
            result.completeExceptionally(new RuntimeException("Blob deletion failed: " + throwable, e));
            return null;
          });
    }

    return result;
  }

  /**
   * Deletes the files associated with the specified store, version, and partition when a blob transfer fails.
   *
   * @param storeName the name of the store
   * @param version the version number of the store
   * @param partition the partition ID
   * @return a CompletableFuture that completes when the deletion process is done
   */
  private CompletableFuture<Void> deleteBlobFiles(String storeName, int version, int partition) {
    return CompletableFuture.runAsync(() -> {
      Path path = null;
      String baseDir = configLoader.getVeniceServerConfig().getRocksDBPath();
      try {
        path = Paths.get(baseDir, storeName, String.valueOf(version), String.valueOf(partition));
        if (Files.exists(path)) {
          Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
          LOGGER.info("Deleted blobs at path: {}", path);
        } else {
          LOGGER.warn("Blob path does not exist: {}", path);
        }
      } catch (Exception e) {
        LOGGER.error("Error occurred while deleting blobs at path: {} - {}", path, e.getMessage());
      }
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
