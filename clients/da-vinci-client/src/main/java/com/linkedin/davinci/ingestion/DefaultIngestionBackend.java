package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The default ingestion backend implementation.
 *
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
  public void startConsumption(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      Optional<PubSubPosition> pubSubPosition,
      String replicaId) {
    String storeVersion = storeConfig.getStoreVersionName();
    LOGGER.info("Retrieving storage engine for replica {}", replicaId);

    StoreVersionInfo storeAndVersion =
        Utils.waitStoreVersionOrThrow(storeVersion, getStoreIngestionService().getMetadataRepo());
    Supplier<StoreVersionState> svsSupplier = () -> storageMetadataService.getStoreVersionState(storeVersion);
    syncStoreVersionConfig(storeAndVersion.getStore(), storeConfig);

    // Blob transfer is now handled inside StoreIngestionTask's processCommonConsumerAction SUBSCRIBE handler.
    // DefaultIngestionBackend only needs to open the store and start consumption.
    StorageEngine storageEngine = storageService.openStoreForNewPartition(storeConfig, partition, svsSupplier);
    topicStorageEngineReferenceMap.compute(storeVersion, (key, storageEngineAtomicReference) -> {
      if (storageEngineAtomicReference != null) {
        storageEngineAtomicReference.set(storageEngine);
      }
      return storageEngineAtomicReference;
    });
    LOGGER.info("Retrieved storage engine for replica {}. Starting consumption in ingestion service", replicaId);
    getStoreIngestionService().startConsumption(storeConfig, partition, pubSubPosition);
    LOGGER.info("Completed starting consumption in ingestion service for replica {}", replicaId);
  }

  @Override
  public CompletableFuture<Void> stopConsumption(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      String replicaId) {
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
      boolean removeEmptyStorageEngine,
      String replicaId) {
    LOGGER.info("dropStoragePartitionGracefully: Dropping replica {}", replicaId);

    // Blob transfer cancellation is now handled inside SIT's DROP_PARTITION action handler.
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

  StorageMetadataService getStorageMetadataService() {
    return storageMetadataService;
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
