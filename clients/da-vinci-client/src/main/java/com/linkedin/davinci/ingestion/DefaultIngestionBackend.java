package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.File;
import java.util.Arrays;
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
  private final Map<String, AtomicReference<StorageEngine>> topicStorageEngineReferenceMap =
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
    StoreVersionInfo storeAndVersion =
        Utils.waitStoreVersionOrThrow(storeVersion, getStoreIngestionService().getMetadataRepo());
    Supplier<StoreVersionState> svsSupplier = () -> storageMetadataService.getStoreVersionState(storeVersion);
    syncStoreVersionConfig(storeAndVersion.getStore(), storeConfig);

    Runnable runnable = () -> {
      StorageEngine storageEngine = storageService.openStoreForNewPartition(storeConfig, partition, svsSupplier);
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
    if (!storeAndVersion.getStore().isBlobTransferEnabled() || blobTransferManager == null) {
      runnable.run();
    } else {
      BlobTransferTableFormat requestTableFormat =
          serverConfig.getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled()
              ? BlobTransferTableFormat.PLAIN_TABLE
              : BlobTransferTableFormat.BLOCK_BASED_TABLE;

      CompletionStage<Void> bootstrapFuture = bootstrapFromBlobs(
          storeAndVersion.getStore(),
          storeAndVersion.getVersion().getNumber(),
          partition,
          requestTableFormat,
          serverConfig.getBlobTransferDisabledOffsetLagThreshold(),
          storeConfig,
          svsSupplier);

      bootstrapFuture.whenComplete((result, throwable) -> {
        runnable.run();
      });
    }
  }

  /**
   * Bootstrap from the blobs from another source (like another peer). If it fails (due to the timeout or
   * any exceptions), it deletes the partially downloaded blobs, and eventually falls back to bootstrapping from Kafka.
   * Blob transfer should be enabled to boostrap from blobs.
   *
   * If the blob transfer fails during transferring:
   * The folder will be cleaned up in NettyP2PBlobTransferManager#handlePeerFetchException.
   * When falling back to Kafka ingestion, it will bootstrap from the beginning.
   *
   * If the blob transfer succeeds:
   * When falling back to Kafka ingestion, it will resume from the last offset instead of starting from the beginning.
   *
   * Regardless of whether the blob transfer succeeds or fails,
   * this method always returns a completed future, All exceptions are handled either by cleaning up the folder or dropping the partition.
   */
  CompletionStage<Void> bootstrapFromBlobs(
      Store store,
      int versionNumber,
      int partitionId,
      BlobTransferTableFormat tableFormat,
      long blobTransferDisabledOffsetLagThreshold,
      VeniceStoreVersionConfig storeConfig,
      Supplier<StoreVersionState> svsSupplier) {
    String storeName = store.getName();
    String kafkaTopic = Version.composeKafkaTopic(storeName, versionNumber);

    if (!store.isBlobTransferEnabled() || blobTransferManager == null) {
      return CompletableFuture.completedFuture(null);
    }

    // Open store for lag check and later metadata update for offset/StoreVersionState
    // If the offset lag is below the blobTransferDisabledOffsetLagThreshold, it indicates there is not lagging and
    // can bootstrap from Kafka.
    storageService.openStore(storeConfig, svsSupplier);
    if (!isOffsetLagged(
        store.getName(),
        versionNumber,
        partitionId,
        blobTransferDisabledOffsetLagThreshold,
        store.isHybrid())) {
      return CompletableFuture.completedFuture(null);
    }

    // After decide to bootstrap from blobs transfer, close the partition, clean up the offset and partition folder,
    // but the metadata partition is not removed.
    StorageEngine storageEngine = storageService.getStorageEngine(kafkaTopic);
    if (storageEngine != null && storageEngine.containsPartition(partitionId)) {
      storageEngine.dropPartition(partitionId, false);
      LOGGER.info(
          "Due to storage engine contains this partition, clean up the offset and delete partition folder for topic {} partition {} before bootstrap from blob transfer",
          kafkaTopic,
          partitionId);
    }

    // double check that the partition folder is not existed before bootstrapping from blobs transfer,
    // as the partition should not exist due to previous dropPartition call.
    String partitionFolder = RocksDBUtils.composePartitionDbDir(serverConfig.getRocksDBPath(), kafkaTopic, partitionId);
    File partitionFolderDir = new File(partitionFolder);
    if (partitionFolderDir.exists()) {
      LOGGER.error(
          "Partition folder {} for {} is existed with files {}.",
          partitionFolder,
          Utils.getReplicaId(kafkaTopic, partitionId),
          Arrays.toString(partitionFolderDir.list()));
    }

    addStoragePartitionWhenBlobTransferStart(partitionId, storeConfig, svsSupplier);

    return blobTransferManager.get(storeName, versionNumber, partitionId, tableFormat)
        .handle((inputStream, throwable) -> {
          updateBlobTransferResponseStats(throwable == null, storeName, versionNumber);
          if (throwable != null) {
            LOGGER.error(
                "Failed to bootstrap partition {} from blobs transfer for store {} with exception {}, falling back to kafka ingestion.",
                partitionId,
                storeName,
                throwable);
          } else {
            LOGGER.info(
                "Successfully bootstrapped partition {} from blobs transfer for store {}",
                partitionId,
                storeName);

            if (partitionFolderDir.exists()) {
              LOGGER.info(
                  "Successfully bootstrapped from blob transfer {} with files: {}",
                  Utils.getReplicaId(kafkaTopic, partitionId),
                  Arrays.toString(partitionFolderDir.list()));
            }
          }
          adjustStoragePartitionWhenBlobTransferComplete(storageService.getStorageEngine(kafkaTopic), partitionId);
          return null;
        });
  }

  /**
   * Add a partition to the storage engine when blob transfer starts
   * with disabled read, disabled write, and rocksDB not open, but have blob transfer in-progress flag.
   *
   */
  private void addStoragePartitionWhenBlobTransferStart(
      int partitionId,
      VeniceStoreVersionConfig storeConfig,
      Supplier<StoreVersionState> svsSupplier) {
    // Prepare configs with blob transfer in-progress flag.
    StoragePartitionConfig storagePartitionConfig =
        new StoragePartitionConfig(storeConfig.getStoreVersionName(), partitionId, true);
    storagePartitionConfig.setReadOnly(true);

    // due to we use storage service to open the store and partition here,
    // it can prevent the race condition between dropping entire store (SE) and receiving transfer file for new
    // partition.
    storageService.openStoreForNewPartition(storeConfig, partitionId, svsSupplier, storagePartitionConfig);
    LOGGER.info(
        "Storage partition is added with {} for replica {} for blob transfer with config {}",
        StoragePartitionAdjustmentTrigger.BEGIN_BLOB_TRANSFER,
        Utils.getReplicaId(storeConfig.getStoreVersionName(), partitionId),
        storagePartitionConfig);
  }

  /**
   * Adjust the storage partition when blob transfer is complete
   * Adjust storage partition will drop the old partition without rocksDB and create a new one with default options and running rocksDB.
   */
  private void adjustStoragePartitionWhenBlobTransferComplete(StorageEngine storageEngine, int partitionId) {
    try {
      // Prepare storage partition with default options, and remove blob transfer in-progress flag.
      StoragePartitionConfig defaultStoragePartitionConfig =
          new StoragePartitionConfig(storageEngine.getStoreVersionName(), partitionId);

      // Adjust the storage partition will create a new partition with rocksDB
      storageEngine.adjustStoragePartition(
          partitionId,
          StoragePartitionAdjustmentTrigger.END_BLOB_TRANSFER,
          defaultStoragePartitionConfig);
    } catch (Exception e) {
      LOGGER.error(
          "Failed to adjust storage partition for replica {} after blob transfer completed: {}, dropping the partition.",
          Utils.getReplicaId(storageEngine.getStoreVersionName(), partitionId),
          e.getMessage(),
          e);
      storageEngine.dropPartition(partitionId, false);
    }
  }

  /**
   * A helper method to help decide if skip blob transfer and use kafka ingestion directly when there are some files already restore.
   *
   * 1. If the store is a batch store, check if the end of push is received
   * 2. If the store is a hybrid store, check the offset lag within the allowed threshold.
   *
   * Note: If `blobTransferDisabledOffsetLagThreshold` is negative, the offset lag check is skipped, and blob transfer always runs.
   * This is because retained data may not be cleaned up unless a new host is added, making it difficult to validate this feature.
   * This 'blobTransferDisabledOffsetLagThreshold' config ensures blob transfer always runs in such cases.
   *
   * @param store the store name
   * @param versionNumber the version number
   * @param partition the partition number
   * @param blobTransferDisabledOffsetLagThreshold the maximum allowed offset lag threshold.
   *        This value is controlled by config BLOB_TRANSFER_DISABLED_OFFSET_LAG_THRESHOLD, and default is 100000L.
   *        If the offset lag is within this threshold, bootstrapping from Kafka is allowed, even if blob transfer is enabled.
   *        If the lag exceeds this threshold, bootstrapping should happen from blobs transfer firstly.
   * @param hybridStore whether the store is a hybrid store or not.
   *                    If it is a hybrid store, then check via the offset.
   *                    If it is a batch store, check if the batch push is done or not.
   * @return true if the store is lagged and needs to bootstrap from blob transfer, else false then bootstrap from Kafka.
   */
  public boolean isOffsetLagged(
      String store,
      int versionNumber,
      int partition,
      long blobTransferDisabledOffsetLagThreshold,
      boolean hybridStore) {
    String topicName = Version.composeKafkaTopic(store, versionNumber);
    OffsetRecord offsetRecord = storageMetadataService.getLastOffset(topicName, partition);

    if (offsetRecord == null) {
      return true;
    }

    if (blobTransferDisabledOffsetLagThreshold < 0) {
      return true;
    }

    if (!hybridStore) {
      if (offsetRecord.isEndOfPushReceived()) {
        LOGGER.info(
            "End of push received for batch store replica {}, might due to restore. Bootstrapping from Kafka.",
            Utils.getReplicaId(topicName, partition));
        return false;
      }
    } else {
      if (offsetRecord.getOffsetLag() == 0
          && PubSubSymbolicPosition.EARLIEST.equals(offsetRecord.getCheckpointedLocalVtPosition())) {
        LOGGER.info(
            "Offset lag is 0 and topic offset is EARLIEST for replica {}.",
            Utils.getReplicaId(topicName, partition));
        return true;
      }

      if (offsetRecord.getOffsetLag() < blobTransferDisabledOffsetLagThreshold) {
        LOGGER.info(
            "Offset lag {} for hybrid store replica {} is within the allowed lag threshold {}. Bootstrapping from Kafka.",
            offsetRecord.getOffsetLag(),
            Utils.getReplicaId(topicName, partition),
            blobTransferDisabledOffsetLagThreshold);
        return false;
      }
    }

    LOGGER.info(
        "Lag check before blob transfer: Replica {} offset is {}. Bootstrapping from blob transfer.",
        Utils.getReplicaId(topicName, partition),
        offsetRecord);
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
  public CompletableFuture<Void> dropStoragePartitionGracefully(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      int timeoutInSeconds,
      boolean removeEmptyStorageEngine) {
    // Stop consumption of the partition.
    final int waitIntervalInSecond = 1;
    final int maxRetry = timeoutInSeconds / waitIntervalInSecond;
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, waitIntervalInSecond, maxRetry, true);
    return getStoreIngestionService().dropStoragePartitionGracefully(storeConfig, partition);
  }

  @Override
  public void addIngestionNotifier(VeniceNotifier ingestionListener) {
    getStoreIngestionService().addIngestionNotifier(ingestionListener);
  }

  @Override
  public void setStorageEngineReference(String topicName, AtomicReference<StorageEngine> storageEngineReference) {
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
