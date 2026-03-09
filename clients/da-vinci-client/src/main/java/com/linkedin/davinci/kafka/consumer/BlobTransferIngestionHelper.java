package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.blobtransfer.BlobTransferStatusTrackingManager;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.LatencyUtils;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class encapsulating blob transfer logic for use within {@link StoreIngestionTask}.
 * Methods are moved from {@link com.linkedin.davinci.ingestion.DefaultIngestionBackend}.
 */
public class BlobTransferIngestionHelper {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferIngestionHelper.class);

  private final BlobTransferManager blobTransferManager;
  private final StorageService storageService;
  private final StorageMetadataService storageMetadataService;
  private final VeniceServerConfig serverConfig;

  public BlobTransferIngestionHelper(
      BlobTransferManager blobTransferManager,
      StorageService storageService,
      StorageMetadataService storageMetadataService,
      VeniceServerConfig serverConfig) {
    this.blobTransferManager = blobTransferManager;
    this.storageService = storageService;
    this.storageMetadataService = storageMetadataService;
    this.serverConfig = serverConfig;
  }

  /**
   * Checks if a replica is lagged enough to benefit from blob transfer rather than PubSub ingestion.
   */
  public boolean isReplicaLaggedAndNeedBlobTransfer(
      String store,
      int versionNumber,
      int partition,
      long blobTransferDisabledOffsetLagThreshold,
      int blobTransferDisabledTimeLagThresholdInMinutes,
      boolean hybridStore,
      String replicaId,
      PubSubContext pubSubContext) {
    String topicName = Version.composeKafkaTopic(store, versionNumber);
    OffsetRecord offsetRecord = storageMetadataService.getLastOffset(topicName, partition, pubSubContext);
    if (offsetRecord == null) {
      LOGGER.warn("Offset record not found for: {}", replicaId);
      return true;
    }

    if (blobTransferDisabledOffsetLagThreshold < 0) {
      return true;
    }
    if (!hybridStore) {
      if (offsetRecord.isEndOfPushReceived()) {
        LOGGER.info(
            "End of push received for batch store replica {}, might due to restore. Bootstrapping from Kafka.",
            replicaId);
        return false;
      }
      return true;
    }

    if (blobTransferDisabledTimeLagThresholdInMinutes > 0) {
      LOGGER.info(
          "Checking time lag for hybrid store replica {} with heartbeat timestamp lag {} ms and offset HB timestamp {}.",
          replicaId,
          LatencyUtils.getElapsedTimeFromMsToMs(offsetRecord.getHeartbeatTimestamp()),
          offsetRecord.getHeartbeatTimestamp());
      return LatencyUtils.getElapsedTimeFromMsToMs(offsetRecord.getHeartbeatTimestamp()) > TimeUnit.MINUTES
          .toMillis(blobTransferDisabledTimeLagThresholdInMinutes);
    }

    if (offsetRecord.getOffsetLag() == 0
        && PubSubSymbolicPosition.EARLIEST.equals(offsetRecord.getCheckpointedLocalVtPosition())) {
      LOGGER.info("Offset lag is 0 and topic offset is EARLIEST for replica {}.", replicaId);
      return true;
    }

    if (offsetRecord.getOffsetLag() < blobTransferDisabledOffsetLagThreshold) {
      LOGGER.info(
          "Offset lag {} for hybrid store replica {} is within the allowed lag threshold {}. Bootstrapping from Kafka.",
          offsetRecord.getOffsetLag(),
          replicaId,
          blobTransferDisabledOffsetLagThreshold);
      return false;
    }

    LOGGER.info(
        "Lag check before blob transfer: Replica {} offset is {}. Bootstrapping from blob transfer.",
        replicaId,
        offsetRecord);
    return true;
  }

  /**
   * Orchestrates the blob transfer: validates directories, sets up storage partition, fetches blobs,
   * and adjusts storage partition on completion.
   *
   * @return a CompletionStage that completes when blob transfer is done (successfully or with error)
   */
  public CompletionStage<Void> bootstrapFromBlobs(
      String storeName,
      int versionNumber,
      int partitionId,
      BlobTransferTableFormat tableFormat,
      boolean isHybrid,
      VeniceStoreVersionConfig storeConfig,
      Supplier<StoreVersionState> svsSupplier,
      String replicaId,
      PubSubContext pubSubContext) {
    String kafkaTopic = Version.composeKafkaTopic(storeName, versionNumber);

    // Open store for metadata update
    storageService.openStore(storeConfig, svsSupplier);

    long blobTransferDisabledOffsetLagThreshold = serverConfig.getBlobTransferDisabledOffsetLagThreshold();
    int blobTransferDisabledTimeLagThresholdInMinutes = serverConfig.getBlobTransferDisabledTimeLagThresholdInMinutes();

    if (!isReplicaLaggedAndNeedBlobTransfer(
        storeName,
        versionNumber,
        partitionId,
        blobTransferDisabledOffsetLagThreshold,
        blobTransferDisabledTimeLagThresholdInMinutes,
        isHybrid,
        replicaId,
        pubSubContext)) {
      LOGGER.info("Replica: {} is not lagged, will consume from PubSub directly", replicaId);
      return CompletableFuture.completedFuture(null);
    }

    LOGGER.info("Replica: {} is lagged, will try to bootstrap via blob transfer", replicaId);

    // After deciding to bootstrap from blob transfer, clean up existing partition data
    StorageEngine storageEngine = storageService.getStorageEngine(kafkaTopic);
    if (storageEngine != null && storageEngine.containsPartition(partitionId)) {
      storageEngine.dropPartition(partitionId, false);
      LOGGER.info(
          "Clean up offset and delete partition folder for topic {} partition {} before blob transfer",
          kafkaTopic,
          partitionId);
    }

    validateDirectoriesBeforeBlobTransfer(storeName, versionNumber, partitionId);
    addStoragePartitionWhenBlobTransferStart(partitionId, storeConfig, svsSupplier, replicaId);

    // Initialize transfer tracking
    blobTransferManager.getTransferStatusTrackingManager().initialTransfer(replicaId);

    return blobTransferManager.get(storeName, versionNumber, partitionId, tableFormat)
        .handle((inputStream, throwable) -> {
          updateBlobTransferResponseStats(throwable, storeName, versionNumber);
          if (throwable != null) {
            LOGGER.warn(
                "Failed to bootstrap replica {} via blob transfer due to exception {}; will fall back to Kafka ingestion.",
                replicaId,
                throwable);
          } else {
            LOGGER.info("Successfully bootstrapped replica {} from blobs transfer.", replicaId);
          }

          validateDirectoriesAfterBlobTransfer(storeName, versionNumber, partitionId, throwable == null, replicaId);
          adjustStoragePartitionWhenBlobTransferComplete(
              storageService.getStorageEngine(kafkaTopic),
              partitionId,
              replicaId);

          blobTransferManager.getTransferStatusTrackingManager().markTransferCompleted(replicaId);
          return null;
        });
  }

  /**
   * Add a partition to the storage engine when blob transfer starts
   * with disabled read, disabled write, and rocksDB not open, but have blob transfer in-progress flag.
   */
  void addStoragePartitionWhenBlobTransferStart(
      int partitionId,
      VeniceStoreVersionConfig storeConfig,
      Supplier<StoreVersionState> svsSupplier,
      String replicaId) {
    StoragePartitionConfig storagePartitionConfig =
        new StoragePartitionConfig(storeConfig.getStoreVersionName(), partitionId, true);
    storagePartitionConfig.setReadOnly(true);

    storageService.openStoreForNewPartition(storeConfig, partitionId, svsSupplier, storagePartitionConfig);
    LOGGER.info(
        "Storage partition is added with {} for replica {} for blob transfer with config {}",
        StoragePartitionAdjustmentTrigger.BEGIN_BLOB_TRANSFER,
        replicaId,
        storagePartitionConfig);
  }

  void validateDirectoriesBeforeBlobTransfer(String storeName, int versionNumber, int partitionId) {
    RocksDBUtils.cleanupBothPartitionDirAndTempTransferredDir(
        storeName,
        versionNumber,
        partitionId,
        serverConfig.getRocksDBPath());
  }

  void validateDirectoriesAfterBlobTransfer(
      String storeName,
      int versionNumber,
      int partitionId,
      boolean transferSuccessful,
      String replicaId) {
    String rocksDBPath = serverConfig.getRocksDBPath();
    String kafkaTopic = Version.composeKafkaTopic(storeName, versionNumber);

    String tempPartitionDir = RocksDBUtils.composeTempPartitionDir(rocksDBPath, kafkaTopic, partitionId);
    String partitionDir = RocksDBUtils.composePartitionDbDir(rocksDBPath, kafkaTopic, partitionId);

    File tempPartitionDirFile = new File(tempPartitionDir);
    File partitionDirFile = new File(partitionDir);

    if (transferSuccessful) {
      if (tempPartitionDirFile.exists()) {
        LOGGER
            .error("Temp directory {} still exists after successful blob transfer for {}", tempPartitionDir, replicaId);
        try {
          RocksDBUtils.deleteDirectory(tempPartitionDir);
        } catch (Exception e) {
          LOGGER.error("Failed to clean up remaining temp directory: {}", tempPartitionDir, e);
        }
      }

      if (!partitionDirFile.exists()) {
        LOGGER.error(
            "Partition directory {} does not exist after successful blob transfer for {}",
            partitionDir,
            replicaId);
      } else {
        LOGGER.info(
            "Successfully bootstrapped from blob transfer {} with files: {}",
            replicaId,
            Arrays.toString(partitionDirFile.list()));
      }
    } else {
      RocksDBUtils.cleanupBothPartitionDirAndTempTransferredDir(storeName, versionNumber, partitionId, rocksDBPath);
    }
  }

  /**
   * Adjust the storage partition when blob transfer is complete.
   * Drops the old partition without rocksDB and creates a new one with default options and running rocksDB.
   */
  void adjustStoragePartitionWhenBlobTransferComplete(StorageEngine storageEngine, int partitionId, String replicaId) {
    try {
      StoragePartitionConfig defaultStoragePartitionConfig =
          new StoragePartitionConfig(storageEngine.getStoreVersionName(), partitionId);
      storageEngine.adjustStoragePartition(
          partitionId,
          StoragePartitionAdjustmentTrigger.END_BLOB_TRANSFER,
          defaultStoragePartitionConfig);
    } catch (Exception e) {
      LOGGER.error(
          "Failed to adjust storage partition for replica {} after blob transfer completed: {}, dropping the partition.",
          replicaId,
          e.getMessage(),
          e);
      storageEngine.dropPartition(partitionId, false);
    }
  }

  /**
   * Cancel an in-progress blob transfer for the given replica.
   */
  public void cancelTransfer(String replicaId) {
    if (blobTransferManager != null) {
      BlobTransferStatusTrackingManager trackingManager = blobTransferManager.getTransferStatusTrackingManager();
      trackingManager.cancelTransfer(replicaId);
    }
  }

  public BlobTransferManager getBlobTransferManager() {
    return blobTransferManager;
  }

  public BlobTransferTableFormat getRequestTableFormat() {
    return serverConfig.getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled()
        ? BlobTransferTableFormat.PLAIN_TABLE
        : BlobTransferTableFormat.BLOCK_BASED_TABLE;
  }

  private void updateBlobTransferResponseStats(Object throwable, String storeName, int version) {
    if (blobTransferManager.getAggVersionedBlobTransferStats() == null) {
      LOGGER.error(
          "Blob transfer stats is not initialized. Skip updating blob transfer response stats for store {} version {}",
          storeName,
          version);
      return;
    }

    if (throwable != null && throwable instanceof VenicePeersNotFoundException) {
      return;
    }

    try {
      blobTransferManager.getAggVersionedBlobTransferStats().recordBlobTransferResponsesCount(storeName, version);
      blobTransferManager.getAggVersionedBlobTransferStats()
          .recordBlobTransferResponsesBasedOnBoostrapStatus(storeName, version, throwable == null);
    } catch (Exception e) {
      LOGGER.error("Failed to update blob transfer response stats for store {} version {}", storeName, version, e);
    }
  }
}
