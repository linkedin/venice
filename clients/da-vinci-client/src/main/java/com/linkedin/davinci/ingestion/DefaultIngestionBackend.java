package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
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
    Supplier<StoreVersionState> svsSupplier = () -> storageMetadataService.getStoreVersionState(storeVersion);
    syncStoreVersionConfig(storeAndVersion.getFirst(), storeConfig);

    Runnable runnable = () -> {
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
    if (!storeAndVersion.getFirst().isBlobTransferEnabled() || blobTransferManager == null) {
      runnable.run();
    } else {
      storageService.openStore(storeConfig, svsSupplier);
      CompletionStage<Void> bootstrapFuture = bootstrapFromBlobs(
          storeAndVersion.getFirst(),
          storeAndVersion.getSecond().getNumber(),
          partition,
          serverConfig.getBlobTransferDisabledOffsetLagThreshold());

      bootstrapFuture.whenComplete((result, throwable) -> {
        runnable.run();
      });
    }
  }

  /**
   * Bootstrap from the blobs from another source (like another peer). If it fails (due to the 30-minute timeout or
   * any exceptions), it deletes the partially downloaded blobs, and eventually falls back to bootstrapping from Kafka.
   * Blob transfer should be enabled to boostrap from blobs.
   */
  CompletionStage<Void> bootstrapFromBlobs(
      Store store,
      int versionNumber,
      int partitionId,
      long blobTransferDisabledOffsetLagThreshold) {
    if (!store.isBlobTransferEnabled() || blobTransferManager == null) {
      return CompletableFuture.completedFuture(null);
    }

    // If the offset lag is below the blobTransferDisabledOffsetLagThreshold, it indicates there is not lagging and
    // can bootstrap from Kafka.
    if (!isOffsetLagged(store.getName(), versionNumber, partitionId, blobTransferDisabledOffsetLagThreshold)) {
      return CompletableFuture.completedFuture(null);
    }

    String storeName = store.getName();
    return blobTransferManager.get(storeName, versionNumber, partitionId).handle((inputStream, throwable) -> {
      updateBlobTransferResponseStats(throwable == null, storeName, versionNumber);
      if (throwable != null) {
        LOGGER.error(
            "Failed to bootstrap partition {} from blobs transfer for store {} with exception {}",
            partitionId,
            storeName,
            throwable);
      } else {
        LOGGER.info("Successfully bootstrapped partition {} from blobs transfer for store {}", partitionId, storeName);
      }
      return null;
    });
  }

  /**
   * A helper method to check if the offset lag is within the allowed threshold.
   * If the offset lag is smaller than the `blobTransferDisabledOffsetLagThreshold`,
   * bootstrapping from Kafka firstly, even if blob transfer is enabled.
   *
   * @param store the store name
   * @param versionNumber the version number
   * @param partition the partition number
   * @param blobTransferDisabledOffsetLagThreshold the maximum allowed offset lag threshold.
   *        If the offset lag is within this threshold, bootstrapping from Kafka is allowed, even if blob transfer is enabled.
   *        If the lag exceeds this threshold, bootstrapping should happen from blobs transfer firstly.
   *
   * @return true if the offset lag exceeds the threshold or if the lag is 0, indicating bootstrapping should happen from blobs transfer.
   *         false otherwise
   */
  public boolean isOffsetLagged(
      String store,
      int versionNumber,
      int partition,
      long blobTransferDisabledOffsetLagThreshold) {
    String topicName = Version.composeKafkaTopic(store, versionNumber);
    OffsetRecord offsetRecord = storageMetadataService.getLastOffset(topicName, partition);

    if (offsetRecord == null || (offsetRecord.getOffsetLag() == 0 && offsetRecord.getLocalVersionTopicOffset() == -1)) {
      LOGGER.info(
          "Offset record is null or offset lag is 0 and topic offset is -1 for store {} partition {}.",
          store,
          partition);
      return true;
    }

    if (offsetRecord.getOffsetLag() < blobTransferDisabledOffsetLagThreshold) {
      LOGGER.info(
          "Offset lag {} for store {} partition {} is within the allowed lag threshold {}. Bootstrapping from Kafka.",
          offsetRecord.getOffsetLag(),
          store,
          partition,
          blobTransferDisabledOffsetLagThreshold);
      return false;
    }

    LOGGER.info(
        "Store {} partition {} topic offset is {}, offset lag is {}",
        store,
        partition,
        offsetRecord.getLocalVersionTopicOffset(),
        offsetRecord.getOffsetLag());
    return true;
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
    getStoreIngestionService().dropStoragePartitionGracefully(storeConfig, partition);
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

  /**
   * Update the blob transfer response stats based on the blob transfer success.
   * @param isBlobTransferSuccess true if the blob transfer is successful, false otherwise.
   */
  private void updateBlobTransferResponseStats(boolean isBlobTransferSuccess, String storeName, int version) {
    if (blobTransferManager.getAggVersionedBlobTransferStats() == null) {
      LOGGER.error(
          "Blob transfer stats is not initialized. Skip updating blob transfer response stats for store {} version {}",
          storeName,
          version);
      return;
    }

    try {
      // Record the blob transfer request count.
      blobTransferManager.getAggVersionedBlobTransferStats().recordBlobTransferResponsesCount(storeName, version);
      // Record the blob transfer response based on the blob transfer status.
      blobTransferManager.getAggVersionedBlobTransferStats()
          .recordBlobTransferResponsesBasedOnBoostrapStatus(storeName, version, isBlobTransferSuccess);
    } catch (Exception e) {
      LOGGER.error("Failed to update blob transfer response stats for store {} version {}", storeName, version, e);
    }
  }
}
