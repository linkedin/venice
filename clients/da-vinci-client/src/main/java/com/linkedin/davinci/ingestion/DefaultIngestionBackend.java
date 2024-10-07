package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
  private final VeniceServerConfig serverConfig;
  private final Map<String, AtomicReference<AbstractStorageEngine>> topicStorageEngineReferenceMap =
      new VeniceConcurrentHashMap<>();
  private final BlobTransferManager blobTransferManager;

  public DefaultIngestionBackend(
      StorageMetadataService storageMetadataService,
      KafkaStoreIngestionService storeIngestionService,
      StorageService storageService,
      BlobTransferManager blobTransferManager,
      VeniceServerConfig serverConfig) {
    this.storageMetadataService = storageMetadataService;
    this.storeIngestionService = storeIngestionService;
    this.storageService = storageService;
    this.blobTransferManager = blobTransferManager;
    this.serverConfig = serverConfig;
  }

  @Override
  public void startConsumption(VeniceStoreVersionConfig storeConfig, int partition) {
    String storeVersion = storeConfig.getStoreVersionName();
    LOGGER.info("Retrieving storage engine for store {} partition {}", storeVersion, partition);
    Pair<Store, Version> storeAndVersion =
        Utils.waitStoreVersionOrThrow(storeVersion, getStoreIngestionService().getMetadataRepo());
    Runnable runnable = () -> {
      Supplier<StoreVersionState> svsSupplier = () -> storageMetadataService.getStoreVersionState(storeVersion);
      syncStoreVersionConfig(storeAndVersion.getFirst(), storeConfig);
      AbstractStorageEngine storageEngine =
          storageService.openStoreForNewPartition(storeConfig, partition, svsSupplier);
      topicStorageEngineReferenceMap.compute(storeVersion, (key, storageEngineAtomicReference) -> {
        if (storageEngineAtomicReference != null) {
          storageEngineAtomicReference.set(storageEngine);
        }
        return storageEngineAtomicReference;
      });
      LOGGER.info(
          "Retrieved storage engine for store {} partition {}. Starting consumption in ingestion service",
          storeVersion,
          partition);
      getStoreIngestionService().startConsumption(storeConfig, partition);
      LOGGER.info(
          "Completed starting consumption in ingestion service for store {} partition {}",
          storeVersion,
          partition);
    };
    // TODO: remove hybrid check after blob transfer in hybrid mode is fully supported
    if (!storeAndVersion.getFirst().isBlobTransferEnabled() || storeAndVersion.getFirst().isHybrid()
        || blobTransferManager == null) {
      runnable.run();
    } else {
      CompletionStage<Void> bootstrapFuture =
          bootstrapFromBlobs(storeAndVersion.getFirst(), storeAndVersion.getSecond().getNumber(), partition);

      bootstrapFuture.whenComplete((result, throwable) -> {
        runnable.run();
      });
    }
  }

  /**
   * Bootstrap from the blobs from another source (like another peer). If it fails (due to the 30-minute timeout or
   * any exceptions), it deletes the partially downloaded blobs, and eventually falls back to bootstrapping from Kafka.
   * Blob transfer should be enabled to boostrap from blobs, and it currently only supports batch-stores.
   */
  CompletionStage<Void> bootstrapFromBlobs(Store store, int versionNumber, int partitionId) {
    // TODO: need to differentiate that's DVC or server. Right now, it doesn't tell so both components can create,
    // though
    // Only DVC would create blobTransferManager.
    if (!store.isBlobTransferEnabled() || store.isHybrid() || blobTransferManager == null) {
      return CompletableFuture.completedFuture(null);
    }

    String storeName = store.getName();
    String baseDir = serverConfig.getRocksDBPath();
    try {
      blobTransferManager.get(storeName, versionNumber, partitionId).toCompletableFuture();
    } catch (VenicePeersNotFoundException e) {
      LOGGER.warn(
          "No valid peers founds for store {}, version {}, partition {}, giving up the blob transfer bootstrap",
          storeName,
          versionNumber,
          partitionId);
    } catch (Exception e) {
      LOGGER.error(
          "Failed bootstrapping from blobs for store {}, version {}, partition {} with exception, "
              + "giving up the blob transfer bootstrap.",
          storeName,
          versionNumber,
          partitionId,
          e);
      RocksDBUtils.deletePartitionDir(baseDir, storeName, versionNumber, partitionId);
    }
    return CompletableFuture.completedFuture(null);
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
  public boolean hasCurrentVersionBootstrapping() {
    return getStoreIngestionService().hasCurrentVersionBootstrapping();
  }

  @Override
  public KafkaStoreIngestionService getStoreIngestionService() {
    return storeIngestionService;
  }

  @Override
  public void close() {
    // Do nothing here, since this is only a wrapper class.
  }

  /**
   * This method is used to sync the store version config with on the store metadata obtained from ZK.
   * VeniceStoreVersionConfig was introduced to allow store-version level configs be configurable via a config file.
   * However, that's no longer a standard practice today since every metadata is stored in ZK. For backward compatibility,
   * there are some configs may need to be copied over from ZK to VeniceStoreVersionConfig.
   * @param store, the store metadata obtained from ZK.
   * @param storeConfig, a POJO class to hold some store-version level configs.
   */
  private void syncStoreVersionConfig(Store store, VeniceStoreVersionConfig storeConfig) {
    if (store.isBlobTransferEnabled()) {
      storeConfig.setBlobTransferEnabled(true);
    }
  }
}
