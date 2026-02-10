package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferStatus;
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
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.ConfigCommonUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
  private final boolean isIsolatedIngestion;

  // Per-replica locks to ensure mutual exclusion between blob transfer triggerred consumption start and cancel
  // operations
  private final Map<String, Lock> consumptionLocks = new VeniceConcurrentHashMap<>();

  // Per-replica consumption state tracking for coordinating start/stop/drop lifecycle
  private final Map<String, ReplicaConsumptionContext> replicaContexts = new VeniceConcurrentHashMap<>();

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
    this.isIsolatedIngestion = serverConfig != null && IngestionMode.ISOLATED.equals(serverConfig.getIngestionMode());
  }

  @Override
  public void startConsumption(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      Optional<PubSubPosition> pubSubPosition) {
    String storeVersion = storeConfig.getStoreVersionName();
    LOGGER.info("Retrieving storage engine for store {} partition {}", storeVersion, partition);
    StoreVersionInfo storeAndVersion =
        Utils.waitStoreVersionOrThrow(storeVersion, getStoreIngestionService().getMetadataRepo());
    Supplier<StoreVersionState> svsSupplier = () -> storageMetadataService.getStoreVersionState(storeVersion);
    syncStoreVersionConfig(storeAndVersion.getStore(), storeConfig);

    String replicaId = Utils.getReplicaId(storeVersion, partition);

    if (!isIsolatedIngestion) {
      ReplicaConsumptionContext replicaContext = getOrCreateReplicaContext(replicaId);

      if (replicaContext.state == ReplicaIntendedState.RUNNING) {
        LOGGER.info("startConsumption called for replica {} but it is already RUNNING. Ignoring duplicate.", replicaId);
        return;
      }

      if (replicaContext.state == ReplicaIntendedState.STOPPED) {
        LOGGER.info(
            "startConsumption: Waiting for blob transfer and PubSub consumption to stop for replica {}.",
            replicaId);
        // TODO: Refactor the ingestion service to take in blob ingestion/transfer, pubsub ingestion logic.
        int stopConsumptionTimeout = serverConfig.getStopConsumptionTimeoutInSeconds();
        stopBlobTransferAndWait(storeConfig, partition, stopConsumptionTimeout);
        getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, 1, stopConsumptionTimeout, false);
      }

      replicaContext.state = ReplicaIntendedState.RUNNING;
      LOGGER.info("Replica {} state set to RUNNING.", replicaId);
    }

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
      getStoreIngestionService().startConsumption(storeConfig, partition, pubSubPosition);
      LOGGER.info(
          "Completed starting consumption in ingestion service for store {} partition {}",
          storeVersion,
          partition);
    };

    boolean blobTransferActiveInReceiver = shouldEnableBlobTransfer(storeAndVersion.getStore());

    if (!blobTransferActiveInReceiver || blobTransferManager == null) {
      runnable.run();
    } else {
      // Status: null -> TRANSFER_NOT_STARTED
      blobTransferManager.getTransferStatusTrackingManager().initialTransfer(replicaId);

      BlobTransferTableFormat requestTableFormat =
          serverConfig.getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled()
              ? BlobTransferTableFormat.PLAIN_TABLE
              : BlobTransferTableFormat.BLOCK_BASED_TABLE;

      // Status: TRANSFER_NOT_STARTED -> TRANSFER_STARTED
      CompletionStage<Void> bootstrapFuture = bootstrapFromBlobs(
          storeAndVersion.getStore(),
          storeAndVersion.getVersion().getNumber(),
          partition,
          requestTableFormat,
          serverConfig.getBlobTransferDisabledOffsetLagThreshold(),
          serverConfig.getBlobTransferDisabledTimeLagThresholdInMinutes(),
          storeConfig,
          svsSupplier);

      // Status: (normal case) TRANSFER_STARTED -> TRANSFER_COMPLETED
      // Status: (cancel in half) TRANSFER_STARTED -> TRANSFER_CANCEL_REQUESTED -> TRANSFER_CANCELLED
      bootstrapFuture.whenComplete((result, throwable) -> {
        Lock consumptionLock = consumptionLocks.computeIfAbsent(replicaId, k -> new ReentrantLock());
        consumptionLock.lock();
        try {
          // Check if cancellation is in progress or completed
          if (blobTransferManager.getTransferStatusTrackingManager().isBlobTransferCancelRequested(replicaId)) {
            LOGGER.info(
                "Blob transfer cancellation was requested for replica {}. Discarding bootstrap result and skipping consumption startup.",
                replicaId);
            blobTransferManager.getTransferStatusTrackingManager().markTransferCancelled(replicaId);
            return;
          }

          blobTransferManager.getTransferStatusTrackingManager().markTransferCompleted(replicaId);
          runnable.run();
        } finally {
          consumptionLock.unlock();
          LOGGER.info("Released consumption lock for replica {} after transfer completion check.", replicaId);
        }
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
   * If the blob transfer is cancelled (e.g., due to partition drop):
   * VeniceBlobTransferCancelledException is propagated to the caller to signal that consumption should not start.
   *
   * Exception handling:
   * - VeniceBlobTransferCancelledException: Propagated to caller (cancellation)
   * - All other exceptions: Swallowed and returns completed future with null (falls back to Kafka)
   */
  CompletionStage<Void> bootstrapFromBlobs(
      Store store,
      int versionNumber,
      int partitionId,
      BlobTransferTableFormat tableFormat,
      long blobTransferDisabledOffsetLagThreshold,
      int blobTransferDisabledTimeLagThresholdInMinutes,
      VeniceStoreVersionConfig storeConfig,
      Supplier<StoreVersionState> svsSupplier) {
    String storeName = store.getName();
    String kafkaTopic = Version.composeKafkaTopic(storeName, versionNumber);
    String replicaId = Utils.getReplicaId(kafkaTopic, partitionId);

    // Open store for lag check and later metadata update for offset/StoreVersionState
    // If the offset lag is below the blobTransferDisabledOffsetLagThreshold, it indicates there is not lagging and
    // can bootstrap from Kafka.
    storageService.openStore(storeConfig, svsSupplier);
    if (!isReplicaLaggedAndNeedBlobTransfer(
        store.getName(),
        versionNumber,
        partitionId,
        blobTransferDisabledOffsetLagThreshold,
        blobTransferDisabledTimeLagThresholdInMinutes,
        store.isHybrid())) {
      LOGGER.info("Replica: {} is not lagged, will consume from PubSub directly", replicaId);
      return CompletableFuture.completedFuture(null);
    } else {
      LOGGER.info("Replica: {} is lagged, will try to bootstrap via blob transfer", replicaId);
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

    // Pre-transfer validation and cleanup
    validateDirectoriesBeforeBlobTransfer(storeName, versionNumber, partitionId);
    addStoragePartitionWhenBlobTransferStart(partitionId, storeConfig, svsSupplier);

    return blobTransferManager.get(storeName, versionNumber, partitionId, tableFormat)
        .handle((inputStream, throwable) -> {
          updateBlobTransferResponseStats(throwable, storeName, versionNumber);
          if (throwable != null) {
            LOGGER.error(
                "Failed to bootstrap replica {} via blob transfer due to exception {}; will start Kafka ingestion unless cancelled.",
                replicaId,
                throwable);
          } else {
            LOGGER.info("Successfully bootstrapped replica {} from blobs transfer.", replicaId);
          }

          // Post-transfer validation and cleanup
          validateDirectoriesAfterBlobTransfer(storeName, versionNumber, partitionId, throwable == null);
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
   * Before bootstrapping from blobs transfer, validate the partition directory and temp partition directory.
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
   * After bootstrapping from blobs transfer, validate the partition directory and temp partition directory.
   */
  private void validateDirectoriesAfterBlobTransfer(
      String storeName,
      int versionNumber,
      int partitionId,
      boolean transferSuccessful) {
    String rocksDBPath = serverConfig.getRocksDBPath();
    String kafkaTopic = Version.composeKafkaTopic(storeName, versionNumber);
    String replicaId = Utils.getReplicaId(kafkaTopic, partitionId);

    String tempPartitionDir = RocksDBUtils.composeTempPartitionDir(rocksDBPath, kafkaTopic, partitionId);
    String partitionDir = RocksDBUtils.composePartitionDbDir(rocksDBPath, kafkaTopic, partitionId);

    File tempPartitionDirFile = new File(tempPartitionDir);
    File partitionDirFile = new File(partitionDir);

    // After successful transfer, temp directory should not exist due to rename and partition directory should exist
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
            Utils.getReplicaId(kafkaTopic, partitionId),
            Arrays.toString(partitionDirFile.list()));
      }
    } else {
      // After failed transfer, both directories should be cleaned up
      RocksDBUtils.cleanupBothPartitionDirAndTempTransferredDir(storeName, versionNumber, partitionId, rocksDBPath);
    }
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
   * A helper method to help decide if skip blob transfer and use PubSub ingestion directly by comparing ingestion
   * state and lag threshold.
   * 1. If `blobTransferDisabledOffsetLagThreshold` is negative, the offset lag check is skipped, and blob transfer
   * always runs. (This is because retained data may not be cleaned up unless a new host is added, making it difficult
   * to validate this feature. This is legacy field and will be retired once `blobTransferDisabledTimeLagThresholdInMinutes`
   * is fully enabled).
   * 1. If the store is a batch store, check if the end of push is received
   * 2. If the store is a hybrid store, check the offset lag within the allowed threshold:
   *   - (1) If `blobTransferDisabledTimeLagThresholdInMinutes` is positive, check time lag against the persisted timestamp.
   *   - (2) Otherwise, fall back to check persisted offset lag with `blobTransferDisabledTimeLagThresholdInMinutes`.
   *
   * @param store the store name
   * @param versionNumber the version number
   * @param partition the partition number
   * @param blobTransferDisabledOffsetLagThreshold the maximum allowed offset lag threshold.
   *        This value is controlled by config BLOB_TRANSFER_DISABLED_OFFSET_LAG_THRESHOLD, and default is 100000L.
   *        If the offset lag is within this threshold, bootstrapping from Kafka is allowed, even if blob transfer is enabled.
   *        If the lag exceeds this threshold, bootstrapping should happen from blobs transfer firstly.
   * @param blobTransferDisabledTimeLagThresholdInMinutes the maximum allowed time lag threshold.
   * @param hybridStore whether the store is a hybrid store or not.
   *                    If it is a hybrid store, then check via the offset.
   *                    If it is a batch store, check if the batch push is done or not.
   * @return true if the store is lagged and needs to bootstrap from blob transfer, else false then bootstrap from Kafka.
   */
  public boolean isReplicaLaggedAndNeedBlobTransfer(
      String store,
      int versionNumber,
      int partition,
      long blobTransferDisabledOffsetLagThreshold,
      int blobTransferDisabledTimeLagThresholdInMinutes,
      boolean hybridStore) {
    String topicName = Version.composeKafkaTopic(store, versionNumber);
    OffsetRecord offsetRecord =
        getStorageMetadataService().getLastOffset(topicName, partition, getStoreIngestionService().getPubSubContext());
    if (offsetRecord == null) {
      LOGGER.warn("Offset record not found for: {}", Utils.getReplicaId(topicName, partition));
      return true;
    }
    /**
     * Legacy way of using offset threshold to determine if a replica is lagged and need blob transfer.
     * We should remove this once the time-lag based threshold check is fully rolled out.
     */
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
      return true;
    }
    /**
     * If the time-lag threshold is active, it will refer to the number to make decision. Otherwise fallback to offset
     * base lag check.
     */
    if (blobTransferDisabledTimeLagThresholdInMinutes > 0) {
      LOGGER.info(
          "Checking time lag for hybrid store replica {} with heartbeat timestamp lag {} ms and offset HB timestamp {}.",
          Utils.getReplicaId(topicName, partition),
          LatencyUtils.getElapsedTimeFromMsToMs(offsetRecord.getHeartbeatTimestamp()),
          offsetRecord.getHeartbeatTimestamp());
      return LatencyUtils.getElapsedTimeFromMsToMs(offsetRecord.getHeartbeatTimestamp()) > TimeUnit.MINUTES
          .toMillis(blobTransferDisabledTimeLagThresholdInMinutes);
    }

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

    LOGGER.info(
        "Lag check before blob transfer: Replica {} offset is {}. Bootstrapping from blob transfer.",
        Utils.getReplicaId(topicName, partition),
        offsetRecord);
    return true;
  }

  @Override
  public CompletableFuture<Void> stopConsumption(VeniceStoreVersionConfig storeConfig, int partition) {
    if (!isIsolatedIngestion) {
      String storeVersion = storeConfig.getStoreVersionName();
      String replicaId = Utils.getReplicaId(storeVersion, partition);
      ReplicaConsumptionContext replicaContext = getOrCreateReplicaContext(replicaId);

      if (replicaContext.state != ReplicaIntendedState.RUNNING) {
        LOGGER.info(
            "stopConsumption called for replica {} but state is {} (not RUNNING). Skipping.",
            replicaId,
            replicaContext.state);
        return CompletableFuture.completedFuture(null);
      }
      replicaContext.state = ReplicaIntendedState.STOPPED;
      LOGGER.info("Replica {} state set to STOPPED.", replicaId);
    }

    cancelBlobTransferIfInProgressInternal(storeConfig, partition);
    return getStoreIngestionService().stopConsumption(storeConfig, partition);
  }

  private void cancelBlobTransferIfInProgressInternal(VeniceStoreVersionConfig storeConfig, int partition) {
    if (blobTransferManager == null) {
      return;
    }

    String storeVersion = storeConfig.getStoreVersionName();
    String replicaId = Utils.getReplicaId(storeVersion, partition);

    try {
      StoreVersionInfo storeAndVersion =
          Utils.waitStoreVersionOrThrow(storeVersion, getStoreIngestionService().getMetadataRepo());
      Store store = storeAndVersion.getStore();

      boolean blobTransferActiveInReceiver = shouldEnableBlobTransfer(store);

      if (!blobTransferActiveInReceiver) {
        return;
      }

      Lock consumptionLock = consumptionLocks.computeIfAbsent(replicaId, k -> new ReentrantLock());
      consumptionLock.lock();

      try {
        blobTransferManager.getTransferStatusTrackingManager().cancelTransfer(replicaId);
      } finally {
        consumptionLock.unlock();
        LOGGER.info("Released consumption lock for replica {} after initiating cancellation", replicaId);
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Exception while canceling blob transfer for replica {} during OFFLINE transition. Proceeding with state transition.",
          replicaId,
          e);
    }
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
    String storeVersion = storeConfig.getStoreVersionName();
    String replicaId = Utils.getReplicaId(storeVersion, partition);
    if (!isIsolatedIngestion) {
      LOGGER.info(
          "dropStoragePartitionGracefully: Replica {} state {} before gracefully dropping",
          replicaId,
          getOrCreateReplicaContext(replicaId));
    }

    // Stop consumption of the partition.
    final int waitIntervalInSecond = 1;
    final int maxRetry = timeoutInSeconds / waitIntervalInSecond;
    stopBlobTransferAndWait(storeConfig, partition, maxRetry);
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, waitIntervalInSecond, maxRetry, true);

    try {
      return getStoreIngestionService().dropStoragePartitionGracefully(storeConfig, partition);
    } finally {
      if (!isIsolatedIngestion) {
        replicaContexts.remove(replicaId);
        LOGGER.info("dropStoragePartitionGracefully: Replica {} context removed.", replicaId);
      }
    }
  }

  private void stopBlobTransferAndWait(VeniceStoreVersionConfig storeConfig, int partition, int timeoutInSeconds) {
    if (blobTransferManager == null) {
      return;
    }

    String storeVersion = storeConfig.getStoreVersionName();
    String replicaId = Utils.getReplicaId(storeVersion, partition);

    if (blobTransferManager.getTransferStatusTrackingManager().isTransferInFinalState(replicaId)) {
      return;
    }

    try {
      // Wait for blob transfer cancellation to complete by checking status transition
      // Poll until the blob transfer reaches final state (null or TRANSFER_COMPLETED or TRANSFER_CANCELLED)
      final int waitIntervalInSecond = 1;
      final int maxRetry = timeoutInSeconds / waitIntervalInSecond;
      int retries = 0;
      while (!blobTransferManager.getTransferStatusTrackingManager().isTransferInFinalState(replicaId)
          && retries < maxRetry) {
        try {
          Thread.sleep(waitIntervalInSecond * 1000L);
          retries++;
          BlobTransferStatus currentStatus =
              blobTransferManager.getTransferStatusTrackingManager().getTransferStatus(replicaId);
          LOGGER.info(
              "Waiting for blob transfer to complete for replica {} (attempt {}/{}). Current status: {}",
              replicaId,
              retries,
              maxRetry,
              currentStatus);
        } catch (InterruptedException e) {
          LOGGER.warn("Interrupted while waiting for blob transfer to complete for replica {}", replicaId, e);
          Thread.currentThread().interrupt();
          break;
        }
      }

      LOGGER.info(
          "Blob transfer for replica {} (final status: {}), proceeding with partition drop",
          replicaId,
          blobTransferManager.getTransferStatusTrackingManager().getTransferStatus(replicaId));
    } finally {
      blobTransferManager.getTransferStatusTrackingManager().clearTransferStatusEnum(replicaId);
      consumptionLocks.remove(replicaId);
    }
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

  StorageMetadataService getStorageMetadataService() {
    return storageMetadataService;
  }

  private ReplicaConsumptionContext getOrCreateReplicaContext(String replicaId) {
    return replicaContexts.computeIfAbsent(replicaId, k -> new ReplicaConsumptionContext());
  }

  // For test assertions
  ReplicaIntendedState getReplicaIntendedState(String replicaId) {
    ReplicaConsumptionContext context = replicaContexts.get(replicaId);
    return context == null ? ReplicaIntendedState.NOT_EXIST : context.state;
  }

  void removeReplicaConsumptionContext(String replicaId) {
    replicaContexts.remove(replicaId);
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
   * Skip counting if the exception is VenicePeersNotFoundException due to no actual transfer happened.
   */
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
      // Record the blob transfer request count.
      blobTransferManager.getAggVersionedBlobTransferStats().recordBlobTransferResponsesCount(storeName, version);
      // Record the blob transfer response based on the blob transfer status.
      blobTransferManager.getAggVersionedBlobTransferStats()
          .recordBlobTransferResponsesBasedOnBoostrapStatus(storeName, version, throwable == null);
    } catch (Exception e) {
      LOGGER.error("Failed to update blob transfer response stats for store {} version {}", storeName, version, e);
    }
  }

  /**
   * Determines whether blob transfer should be enabled based on the combined logic of
   * store-level and server-level configurations.
   *
   * For the DaVinci client:
   *  it uses the store's blob transfer enabled flag (BLOB_TRANSFER_ENABLED) directly.
   *
   * For the server:
   *  it uses the combination of store-level (BLOB_TRANSFER_IN_SERVER_ENABLED) and server-level (BLOB_TRANSFER_RECEIVER_SERVER_POLICY) config:
   *
   * - Default (Disabled): If both configurations are NOT_SPECIFIED, feature is disabled
   * - Scope-Specific Enablement: If one is NOT_SPECIFIED and other is ENABLED, feature is enabled
   * - Scope-Specific Disablement: If either configuration is DISABLED, feature is disabled
   * - Enabled in Both Scopes: If both are ENABLED, feature is enabled
   *
   * @param store The store metadata containing store-level blob transfer configuration
   * @return true if blob transfer should be enabled, false otherwise
   */
  private boolean shouldEnableBlobTransfer(Store store) {
    boolean isDaVinciClient = storeIngestionService.isDaVinciClient();

    // For DaVinci clients, use the store's blob transfer enabled flag
    if (isDaVinciClient) {
      boolean blobTransferEnabledForDVC = store.isBlobTransferEnabled();
      LOGGER.info("DaVinci client detected. Blob transfer enabled {}", blobTransferEnabledForDVC);
      return blobTransferEnabledForDVC;
    }

    // For server mode, apply the combined logic
    String blobTransferInServerPolicyForStoreLevel = store.getBlobTransferInServerEnabled(); // Store-level
    ConfigCommonUtils.ActivationState blobTransferInServerPolicyForNodeLevel =
        serverConfig.getBlobTransferReceiverServerPolicy(); // Server-level

    LOGGER.info(
        "Evaluating blob transfer enablement for store {}. Store-level: {}, Server-level: {} in Server.",
        store.getName(),
        blobTransferInServerPolicyForStoreLevel,
        blobTransferInServerPolicyForNodeLevel);

    // case 1: If either configuration explicitly disables, feature is disabled
    if (ConfigCommonUtils.ActivationState.DISABLED.name().equals(blobTransferInServerPolicyForStoreLevel)
        || ConfigCommonUtils.ActivationState.DISABLED.equals(blobTransferInServerPolicyForNodeLevel)) {
      return false;
    }

    // case 2: If either configuration enables (and the other is not disabled), feature is enabled
    if (ConfigCommonUtils.ActivationState.ENABLED.name().equals(blobTransferInServerPolicyForStoreLevel)
        || ConfigCommonUtils.ActivationState.ENABLED.equals(blobTransferInServerPolicyForNodeLevel)) {
      return true;
    }

    // case 3: Default case, both are NOT_SPECIFIED or null, feature is disabled
    return false;
  }

  enum ReplicaIntendedState {
    /** The replica has never been started on this host, or has been dropped. */
    NOT_EXIST,
    /** Consumption is active (blob transfer in progress or Kafka consumer subscribed). */
    RUNNING,
    /** A stop has been requested (but not yet dropped). */
    STOPPED
  }

  static class ReplicaConsumptionContext {
    ReplicaIntendedState state = ReplicaIntendedState.NOT_EXIST;
  }
}
