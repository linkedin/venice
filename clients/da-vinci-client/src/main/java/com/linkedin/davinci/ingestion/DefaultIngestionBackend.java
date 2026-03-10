package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.api.PubSubPosition;
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
 */
public class DefaultIngestionBackend implements IngestionBackend {
  private static final Logger LOGGER = LogManager.getLogger(DefaultIngestionBackend.class);
  private final StorageMetadataService storageMetadataService;
  private final StorageService storageService;
  private final KafkaStoreIngestionService storeIngestionService;
  private final VeniceServerConfig serverConfig;
  private final Map<String, AtomicReference<StorageEngine>> topicStorageEngineReferenceMap =
      new VeniceConcurrentHashMap<>();

  // Per-replica consumption state tracking for coordinating start/stop/drop lifecycle
  private final Map<String, ReplicaConsumptionContext> replicaContexts = new VeniceConcurrentHashMap<>();

  public DefaultIngestionBackend(
      StorageMetadataService storageMetadataService,
      KafkaStoreIngestionService storeIngestionService,
      StorageService storageService,
      VeniceServerConfig serverConfig) {
    this.storageMetadataService = storageMetadataService;
    this.storeIngestionService = storeIngestionService;
    this.storageService = storageService;
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
    Supplier<StoreVersionState> svsSupplier = () -> storageMetadataService.getStoreVersionState(storeVersion);

    ReplicaConsumptionContext replicaContext = getOrCreateReplicaContext(replicaId);

    if (replicaContext.state == ReplicaIntendedState.RUNNING) {
      LOGGER.info("startConsumption called for replica {} but it is already RUNNING. Ignoring duplicate.", replicaId);
      return;
    }

    if (replicaContext.state == ReplicaIntendedState.STOPPED) {
      LOGGER.info("startConsumption: Waiting for consumption to stop for replica {}.", replicaId);
      int stopConsumptionTimeout = serverConfig.getStopConsumptionTimeoutInSeconds();
      getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, 1, stopConsumptionTimeout, false);
    }

    replicaContext.state = ReplicaIntendedState.RUNNING;
    LOGGER.info("Replica {} state set to RUNNING.", replicaId);

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

    return getStoreIngestionService().stopConsumption(storeConfig, partition);
  }

  @Override
  public void killConsumptionTask(String topicName) {
    getStoreIngestionService().killConsumptionTask(topicName);
  }

  @Override
  public void shutdownIngestionTask(String topicName) {
    getStoreIngestionService().shutdownStoreIngestionTask(topicName);
    // Clean up per-replica state to prevent stale RUNNING state from blocking re-subscription on restart
    String replicaPrefix = topicName + "-";
    replicaContexts.keySet().removeIf(replicaId -> replicaId.startsWith(replicaPrefix));
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
    LOGGER.info(
        "dropStoragePartitionGracefully: Replica {} state {} before gracefully dropping",
        replicaId,
        getOrCreateReplicaContext(replicaId).state);

    // Stop consumption of the partition.
    final int waitIntervalInSecond = 1;
    final int maxRetry = timeoutInSeconds / waitIntervalInSecond;
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, waitIntervalInSecond, maxRetry, true);

    try {
      return getStoreIngestionService().dropStoragePartitionGracefully(storeConfig, partition);
    } finally {
      replicaContexts.remove(replicaId);
      LOGGER.info("dropStoragePartitionGracefully: Replica {} context removed.", replicaId);
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
