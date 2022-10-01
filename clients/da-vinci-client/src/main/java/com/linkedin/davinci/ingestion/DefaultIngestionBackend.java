package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The default ingestion backend implementation. Ingestion will be done in the same JVM as the application.
 */
public class DefaultIngestionBackend implements DaVinciIngestionBackend, VeniceIngestionBackend {
  private static final Logger LOGGER = LogManager.getLogger(DefaultIngestionBackend.class);
  private final StorageMetadataService storageMetadataService;
  private final StorageService storageService;
  private final KafkaStoreIngestionService storeIngestionService;
  private final Map<String, AtomicReference<AbstractStorageEngine>> topicStorageEngineReferenceMap =
      new VeniceConcurrentHashMap<>();

  public DefaultIngestionBackend(
      StorageMetadataService storageMetadataService,
      KafkaStoreIngestionService storeIngestionService,
      StorageService storageService) {
    this.storageMetadataService = storageMetadataService;
    this.storeIngestionService = storeIngestionService;
    this.storageService = storageService;
  }

  @Override
  public void startConsumption(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      Optional<LeaderFollowerStateType> leaderState) {
    LOGGER.info("Retrieving storage engine for store {} partition {}", storeConfig.getStoreVersionName(), partition);
    Utils.waitStoreVersionOrThrow(storeConfig.getStoreVersionName(), getStoreIngestionService().getMetadataRepo());
    AbstractStorageEngine storageEngine = getStorageService().openStoreForNewPartition(storeConfig, partition);
    if (topicStorageEngineReferenceMap.containsKey(storeConfig.getStoreVersionName())) {
      topicStorageEngineReferenceMap.get(storeConfig.getStoreVersionName()).set(storageEngine);
    }
    LOGGER.info(
        "Retrieved storage engine for store {} partition {}. Starting consumption in ingestion service",
        storeConfig.getStoreVersionName(),
        partition);
    getStoreIngestionService().startConsumption(storeConfig, partition, leaderState);
    LOGGER.info(
        "Completed starting consumption in ingestion service for store {} partition {}",
        storeConfig.getStoreVersionName(),
        partition);
  }

  @Override
  public void stopConsumption(VeniceStoreVersionConfig storeConfig, int partition) {
    getStoreIngestionService().stopConsumption(storeConfig, partition);
  }

  @Override
  public void killConsumptionTask(String topicName) {
    getStoreIngestionService().killConsumptionTask(topicName);
  }

  @Override
  public void removeStorageEngine(String topicName) {
    getStorageService().removeStorageEngine(topicName);
  }

  @Override
  public void dropStoragePartitionGracefully(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      int timeoutInSeconds,
      boolean removeEmptyStorageEngine) {
    String topicName = storeConfig.getStoreVersionName();
    // Delete this replica from meta system store if exists.
    getStoreIngestionService().getMetaSystemStoreReplicaStatusNotifier()
        .ifPresent(systemStoreReplicaStatusNotifier -> systemStoreReplicaStatusNotifier.drop(topicName, partition));
    // Stop consumption of the partition.
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, 1, timeoutInSeconds);
    // Drops corresponding data partition from storage.
    getStorageService().dropStorePartition(storeConfig, partition, removeEmptyStorageEngine);

  }

  @Override
  public void promoteToLeader(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    LOGGER.info("Promoting partition: {} of topic: {} to leader.", partition, storeConfig.getStoreVersionName());
    getStoreIngestionService().promoteToLeader(storeConfig, partition, leaderSessionIdChecker);
  }

  @Override
  public void demoteToStandby(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    LOGGER.info("Demoting partition: {} of topic: {} to standby.", partition, storeConfig.getStoreVersionName());
    getStoreIngestionService().demoteToStandby(storeConfig, partition, leaderSessionIdChecker);
  }

  @Override
  public void addIngestionNotifier(VeniceNotifier ingestionListener) {
    getStoreIngestionService().addCommonNotifier(ingestionListener);
  }

  @Override
  public void addLeaderFollowerIngestionNotifier(VeniceNotifier ingestionListener) {
    getStoreIngestionService().addLeaderFollowerModelNotifier(ingestionListener);
  }

  @Override
  public void addPushStatusNotifier(VeniceNotifier pushStatusNotifier) {
    getStoreIngestionService().addCommonNotifier(pushStatusNotifier);
  }

  @Override
  public void setStorageEngineReference(
      String topicName,
      AtomicReference<AbstractStorageEngine> storageEngineReference) {
    topicStorageEngineReferenceMap.put(topicName, storageEngineReference);
  }

  @Override
  public StorageMetadataService getStorageMetadataService() {
    return storageMetadataService;
  }

  @Override
  public KafkaStoreIngestionService getStoreIngestionService() {
    return storeIngestionService;
  }

  @Override
  public StorageService getStorageService() {
    return storageService;
  }

  @Override
  public void close() {
    // Do nothing here, since this is only a wrapper class.
  }
}
