package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.blobtransfer.BlobTransferStatusTrackingManager;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferStatus;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.ConfigCommonUtils.ActivationState;
import com.linkedin.venice.utils.LatencyUtils;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class encapsulating blob transfer logic for use within {@link StoreIngestionTask}.
 * Methods are extracted from {@link StoreIngestionTask} and
 * {@link com.linkedin.davinci.ingestion.DefaultIngestionBackend} for better readability.
 */
public class BlobTransferIngestionHelper {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferIngestionHelper.class);

  private final BlobTransferManager blobTransferManager;
  private final StorageService storageService;
  private final StorageMetadataService storageMetadataService;
  private final VeniceServerConfig serverConfig;
  private final Set<String> blobTransferDisabledStores;

  public BlobTransferIngestionHelper(
      BlobTransferManager blobTransferManager,
      StorageService storageService,
      StorageMetadataService storageMetadataService,
      VeniceServerConfig serverConfig,
      Set<String> blobTransferDisabledStores) {
    this.blobTransferManager = blobTransferManager;
    this.storageService = storageService;
    this.storageMetadataService = storageMetadataService;
    this.serverConfig = serverConfig;
    this.blobTransferDisabledStores =
        blobTransferDisabledStores != null ? blobTransferDisabledStores : Collections.emptySet();
  }

  /**
   * Determines whether blob transfer should be started for a given partition.
   * Checks blob transfer manager availability, consumer action type, store config, and replica lag.
   */
  public boolean shouldStartBlobTransfer(
      ConsumerAction consumerAction,
      Store store,
      String storeName,
      int versionNumber,
      int partition,
      String replicaId,
      boolean isDaVinciClient,
      boolean isHybrid,
      String kafkaVersionTopic,
      PubSubContext pubSubContext) {
    if (blobTransferManager == null) {
      return false;
    }
    // Skip blob transfer for user-driven seek/subscribe with explicit PubSubPosition (seekToCheckpoint).
    // The user explicitly requested a specific Kafka offset; blob transfer would ignore this position.
    if (consumerAction != null && consumerAction.getPubSubPosition() != null) {
      return false;
    }
    if (!shouldEnableBlobTransfer(store, isDaVinciClient)) {
      return false;
    }
    return isReplicaLaggedAndNeedBlobTransfer(
        storeName,
        versionNumber,
        partition,
        replicaId,
        isHybrid,
        kafkaVersionTopic,
        pubSubContext);
  }

  /**
   * Checks store/server config to determine if blob transfer is enabled.
   */
  public boolean shouldEnableBlobTransfer(Store store, boolean isDaVinciClient) {
    if (isDaVinciClient) {
      if (blobTransferDisabledStores.contains(store.getName())) {
        LOGGER.debug("Blob transfer disabled for store {} (version-specific or stateless client)", store.getName());
        return false;
      }
      return store.isBlobTransferEnabled();
    }
    // For server mode, check combined store-level and server-level config
    String storeLevelPolicy = store.getBlobTransferInServerEnabled();
    ActivationState serverLevelPolicy = serverConfig.getBlobTransferReceiverServerPolicy();
    if (ActivationState.DISABLED.name().equals(storeLevelPolicy)
        || ActivationState.DISABLED.equals(serverLevelPolicy)) {
      return false;
    }
    if (ActivationState.ENABLED.name().equals(storeLevelPolicy) || ActivationState.ENABLED.equals(serverLevelPolicy)) {
      return true;
    }
    return false;
  }

  /**
   * Checks if the replica is lagged enough to warrant blob transfer instead of Kafka bootstrap.
   */
  public boolean isReplicaLaggedAndNeedBlobTransfer(
      String storeName,
      int versionNumber,
      int partition,
      String replicaId,
      boolean isHybrid,
      String kafkaVersionTopic,
      PubSubContext pubSubContext) {
    OffsetRecord offsetRecord = storageMetadataService.getLastOffset(kafkaVersionTopic, partition, pubSubContext);
    if (offsetRecord == null) {
      LOGGER.warn("Offset record not found for: {}", replicaId);
      return true;
    }
    // TODO: Remove offset lag threshold entirely in the future — no offset lag should be allowed.
    long blobTransferDisabledOffsetLagThreshold = serverConfig.getBlobTransferDisabledOffsetLagThreshold();
    if (blobTransferDisabledOffsetLagThreshold < 0) {
      return true;
    }
    if (!isHybrid) {
      if (offsetRecord.isEndOfPushReceived()) {
        LOGGER.info("End of push received for batch store replica {}, bootstrapping from Kafka.", replicaId);
        return false;
      }
      return true;
    }
    int timeLagThresholdMinutes = serverConfig.getBlobTransferDisabledTimeLagThresholdInMinutes();
    if (timeLagThresholdMinutes > 0) {
      long timeLagMs = LatencyUtils.getElapsedTimeFromMsToMs(offsetRecord.getHeartbeatTimestamp());
      long thresholdMs = TimeUnit.MINUTES.toMillis(timeLagThresholdMinutes);
      boolean isLagged = timeLagMs > thresholdMs;
      LOGGER.info(
          "Time lag check for replica {}: timeLagMs={}, thresholdMs={} ({}min), isLagged={}",
          replicaId,
          timeLagMs,
          thresholdMs,
          timeLagThresholdMinutes,
          isLagged);
      return isLagged;
    }
    if (offsetRecord.getOffsetLag() == 0
        && PubSubSymbolicPosition.EARLIEST.equals(offsetRecord.getCheckpointedLocalVtPosition())) {
      return true;
    }
    if (offsetRecord.getOffsetLag() < blobTransferDisabledOffsetLagThreshold) {
      LOGGER.info(
          "Offset lag {} for replica {} is within threshold {}. Bootstrapping from Kafka.",
          offsetRecord.getOffsetLag(),
          replicaId,
          blobTransferDisabledOffsetLagThreshold);
      return false;
    }
    return true;
  }

  /**
   * Starts an async blob transfer for the given partition.
   * Prepares storage, cleans up directories, opens a blob-transfer-in-progress partition,
   * and kicks off the async transfer. The returned future is stored on the PCS.
   */
  public void startBlobTransferAsyncForPartition(
      int partition,
      PartitionConsumptionState pcs,
      StorageEngine storageEngine,
      String storeName,
      int versionNumber,
      VeniceStoreVersionConfig storeVersionConfig,
      String kafkaVersionTopic) {
    String replicaId = pcs.getReplicaId();
    LOGGER.info("Starting async blob transfer for replica: {}", replicaId);

    // Prepare storage for blob transfer: drop existing partition data and clean up directories
    if (storageEngine.containsPartition(partition)) {
      storageEngine.dropPartition(partition, false);
      LOGGER.info("Dropped existing partition {} before blob transfer for replica {}", partition, replicaId);
    }

    // Pre-transfer validation and cleanup
    Supplier<StoreVersionState> svsSupplier = () -> storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    validateDirectoriesBeforeBlobTransfer(storeName, versionNumber, partition);
    addStoragePartitionWhenBlobTransferStart(partition, storeVersionConfig, svsSupplier, replicaId);

    // Determine table format
    BlobTransferTableFormat tableFormat = getRequestTableFormat();

    // Initialize transfer status tracking: null -> TRANSFER_NOT_STARTED
    BlobTransferStatusTrackingManager trackingManager = blobTransferManager.getTransferStatusTrackingManager();
    if (trackingManager != null) {
      trackingManager.initialTransfer(replicaId);
    }

    // Start async blob transfer and save the future on PCS.
    // Post-transfer work (completeBlobTransferAndSubscribe) happens on the SIT thread
    // in checkLongRunningTaskState when the future completes.
    CompletableFuture<Void> blobTransferFuture =
        blobTransferManager.get(storeName, versionNumber, partition, tableFormat)
            .thenApply(inputStream -> (Void) null)
            .toCompletableFuture();

    // Set the future on PCS before adding callbacks to avoid race condition where
    // the transfer completes before setPendingBlobTransfer is called.
    pcs.setPendingBlobTransfer(blobTransferFuture);

    // Track status transitions: TRANSFER_STARTED -> TRANSFER_COMPLETED or TRANSFER_CANCELLED
    blobTransferFuture.whenComplete((result, throwable) -> {
      if (trackingManager != null) {
        if (trackingManager.isBlobTransferCancelRequested(replicaId)) {
          trackingManager.markTransferCancelled(replicaId);
        } else {
          trackingManager.markTransferCompleted(replicaId);
        }
      }
    });
  }

  /**
   * Before bootstrapping from blob transfer, validate the partition directory and temp partition directory.
   * If either of them exists, delete them to ensure a clean state for blob transfer.
   */
  private void validateDirectoriesBeforeBlobTransfer(String storeName, int versionNumber, int partitionId) {
    RocksDBUtils.cleanupBothPartitionDirAndTempTransferredDir(
        storeName,
        versionNumber,
        partitionId,
        serverConfig.getRocksDBPath());
  }

  /**
   * Add a partition to the storage engine when blob transfer starts
   * with disabled read, disabled write, and rocksDB not open, but have blob transfer in-progress flag.
   */
  private void addStoragePartitionWhenBlobTransferStart(
      int partitionId,
      VeniceStoreVersionConfig storeConfig,
      Supplier<StoreVersionState> svsSupplier,
      String replicaId) {
    StoragePartitionConfig storagePartitionConfig =
        new StoragePartitionConfig(storeConfig.getStoreVersionName(), partitionId, true);
    storagePartitionConfig.setReadOnly(true);
    storageService.openStoreForNewPartition(storeConfig, partitionId, svsSupplier, storagePartitionConfig);
    LOGGER.info(
        "Storage partition added with {} for replica {} for blob transfer",
        StoragePartitionAdjustmentTrigger.BEGIN_BLOB_TRANSFER,
        replicaId);
  }

  /**
   * Cancels a pending blob transfer for the given PCS if one is in progress.
   * This is fire-and-forget — used by UNSUBSCRIBE which doesn't delete files.
   */
  public void requestPendingBlobTransferCancellation(PartitionConsumptionState pcs) {
    CompletableFuture<Void> pendingTransfer = pcs.getPendingBlobTransfer();
    if (pendingTransfer != null) {
      String replicaId = pcs.getReplicaId();
      LOGGER.info("Cancelling pending blob transfer for replica: {}", replicaId);
      requestBlobTransferCancellation(replicaId);
      pcs.setPendingBlobTransfer(null);
    }
  }

  /**
   * Waits for blob transfer to reach a final state before returning.
   * Used before dropping a partition to ensure Netty has finished writing files.
   * Polls the BlobTransferStatusTrackingManager until the transfer reaches a final state
   * (null, TRANSFER_COMPLETED, or TRANSFER_CANCELLED).
   */
  public void cancelBlobTransferAndAwaitTermination(int partition, int timeoutInSeconds, String replicaId) {
    if (blobTransferManager == null) {
      return;
    }

    // Initiate cancellation first, it might be trigger by UNSUBSCRIBE before, but this is idempotent
    requestBlobTransferCancellation(replicaId);

    BlobTransferStatusTrackingManager trackingManager = blobTransferManager.getTransferStatusTrackingManager();
    if (trackingManager == null || trackingManager.isTransferInFinalState(replicaId)) {
      return;
    }

    try {
      // Wait for blob transfer cancellation to complete by checking status transition
      // Poll until the blob transfer reaches final state (null or TRANSFER_COMPLETED or TRANSFER_CANCELLED)
      final int waitIntervalInSecond = 1;
      final int maxRetry = timeoutInSeconds / waitIntervalInSecond;

      BlobTransferStatus initialStatus = trackingManager.getTransferStatus(replicaId);

      BlobTransferStatus previousStatus = initialStatus;
      int retries = 0;
      while (!trackingManager.isTransferInFinalState(replicaId) && retries < maxRetry) {
        try {
          Thread.sleep(waitIntervalInSecond * 1000L);
          retries++;
          BlobTransferStatus currentStatus = trackingManager.getTransferStatus(replicaId);

          if (currentStatus != previousStatus) {
            LOGGER.info(
                "Blob transfer status changed for replica {} (attempt {}/{}): {} -> {}",
                replicaId,
                retries,
                maxRetry,
                previousStatus,
                currentStatus);
            previousStatus = currentStatus;
          }
        } catch (InterruptedException e) {
          LOGGER.warn("Interrupted while waiting for blob transfer to complete for replica {}", replicaId, e);
          Thread.currentThread().interrupt();
          break;
        }
      }

      BlobTransferStatus finalStatus = trackingManager.getTransferStatus(replicaId);
      LOGGER.info(
          "Blob transfer wait completed for replica {} after {} attempts. Status transition: {} -> {}, proceeding with partition drop",
          replicaId,
          retries,
          initialStatus,
          finalStatus);
    } finally {
      trackingManager.clearTransferStatusEnum(replicaId);
    }
  }

  /**
   * Validates directories after blob transfer completes.
   * On success: clean up any remaining temp directory.
   * On failure: clean up both partition and temp directories.
   */
  public void validateDirectoriesAfterBlobTransfer(
      String storeName,
      int versionNumber,
      int partition,
      boolean transferSuccessful,
      String replicaId) {
    String rocksDBPath = serverConfig.getRocksDBPath();
    String kafkaTopic = Version.composeKafkaTopic(storeName, versionNumber);

    if (transferSuccessful) {
      String tempPartitionDir = RocksDBUtils.composeTempPartitionDir(rocksDBPath, kafkaTopic, partition);
      File tempPartitionDirFile = new File(tempPartitionDir);
      if (tempPartitionDirFile.exists()) {
        LOGGER
            .error("Temp directory {} still exists after successful blob transfer for {}", tempPartitionDir, replicaId);
        try {
          RocksDBUtils.deleteDirectory(tempPartitionDir);
        } catch (Exception e) {
          LOGGER.error("Failed to clean up remaining temp directory: {}", tempPartitionDir, e);
        }
      }

      String partitionDir = RocksDBUtils.composePartitionDbDir(rocksDBPath, kafkaTopic, partition);
      File partitionDirFile = new File(partitionDir);
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
      RocksDBUtils.cleanupBothPartitionDirAndTempTransferredDir(storeName, versionNumber, partition, rocksDBPath);
    }
  }

  /**
   * Adjusts the storage partition after blob transfer completes.
   * Drops the blob-transfer-in-progress partition and creates a new one with default options (RocksDB open).
   */
  public void adjustStoragePartitionWhenBlobTransferComplete(
      StorageEngine storageEngine,
      int partition,
      String replicaId) {
    try {
      StoragePartitionConfig defaultStoragePartitionConfig =
          new StoragePartitionConfig(storageEngine.getStoreVersionName(), partition);
      storageEngine.adjustStoragePartition(
          partition,
          StoragePartitionAdjustmentTrigger.END_BLOB_TRANSFER,
          defaultStoragePartitionConfig);
    } catch (Exception e) {
      LOGGER.error(
          "Failed to adjust storage partition for replica {} after blob transfer completed, dropping the partition.",
          replicaId,
          e);
      storageEngine.dropPartition(partition, false);
    }
  }

  /**
   * Records blob transfer response stats.
   */
  public void updateBlobTransferResponseStats(
      String storeName,
      int versionNumber,
      boolean transferSucceeded,
      CompletableFuture<Void> future) {
    if (blobTransferManager == null || blobTransferManager.getAggVersionedBlobTransferStats() == null) {
      return;
    }

    // Don't record stats for VenicePeersNotFoundException (no peers available is not a transfer attempt)
    if (!transferSucceeded) {
      try {
        future.get();
      } catch (Exception e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        if (cause instanceof VenicePeersNotFoundException) {
          return;
        }
      }
    }

    try {
      blobTransferManager.getAggVersionedBlobTransferStats().recordBlobTransferResponsesCount(storeName, versionNumber);
      blobTransferManager.getAggVersionedBlobTransferStats()
          .recordBlobTransferResponsesBasedOnBoostrapStatus(storeName, versionNumber, transferSucceeded);
    } catch (Exception e) {
      LOGGER
          .error("Failed to update blob transfer response stats for store {} version {}", storeName, versionNumber, e);
    }
  }

  /**
   * Determines the RocksDB table format to request for blob transfer.
   */
  public BlobTransferTableFormat getRequestTableFormat() {
    return serverConfig.getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled()
        ? BlobTransferTableFormat.PLAIN_TABLE
        : BlobTransferTableFormat.BLOCK_BASED_TABLE;
  }

  /**
   * Signals the blob transfer manager to cancel a transfer for the given replica.
   */
  public void requestBlobTransferCancellation(String replicaId) {
    if (blobTransferManager != null && blobTransferManager.getTransferStatusTrackingManager() != null) {
      blobTransferManager.getTransferStatusTrackingManager().cancelTransfer(replicaId);
    }
  }

  /**
   * Clears the tracking manager status for the given replica.
   * Called after blob transfer is fully handled (success or failure) to prevent stale state.
   */
  public void clearTransferStatus(String replicaId) {
    if (blobTransferManager != null && blobTransferManager.getTransferStatusTrackingManager() != null) {
      blobTransferManager.getTransferStatusTrackingManager().clearTransferStatusEnum(replicaId);
    }
  }

  public BlobTransferManager getBlobTransferManager() {
    return blobTransferManager;
  }
}
