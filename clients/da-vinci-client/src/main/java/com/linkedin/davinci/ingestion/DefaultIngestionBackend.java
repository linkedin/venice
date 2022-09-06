package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DefaultIngestionBackend is the default ingestion backend implementation. Ingestion will be done in the same JVM as the application.
 */
public class DefaultIngestionBackend implements DaVinciIngestionBackend, VeniceIngestionBackend {
  private static final Logger logger = LogManager.getLogger(DefaultIngestionBackend.class);
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
    logger.info("Retrieving storage engine for store " + storeConfig.getStoreVersionName() + " partition " + partition);
    Utils.waitStoreVersionOrThrow(storeConfig.getStoreVersionName(), getStoreIngestionService().getMetadataRepo());
    AbstractStorageEngine storageEngine = getStorageService().openStoreForNewPartition(storeConfig, partition);
    if (topicStorageEngineReferenceMap.containsKey(storeConfig.getStoreVersionName())) {
      topicStorageEngineReferenceMap.get(storeConfig.getStoreVersionName()).set(storageEngine);
    }
    logger.info(
        "Retrieved storage engine for store " + storeConfig.getStoreVersionName() + " partition " + partition
            + ". Starting consumption in ingestion service");
    getStoreIngestionService().startConsumption(storeConfig, partition, leaderState);
    logger.info(
        "Completed starting consumption in ingestion service for store " + storeConfig.getStoreVersionName()
            + " partition " + partition);
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
    if (storeIngestionService.isPartitionConsuming(storeConfig, partition)) {
      long startTimeInMs = System.currentTimeMillis();
      getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, 1, timeoutInSeconds);
      logger.info(
          String.format(
              "Partition: %d of topic: %s has stopped consumption in %d ms.",
              partition,
              storeConfig.getStoreVersionName(),
              LatencyUtils.getElapsedTimeInMs(startTimeInMs)));
    }
    getStoreIngestionService().resetConsumptionOffset(storeConfig, partition);
    getStorageService().dropStorePartition(storeConfig, partition, removeEmptyStorageEngine);
    logger.info("Partition: " + partition + " of topic: " + storeConfig.getStoreVersionName() + " has been dropped.");
  }

  @Override
  public void promoteToLeader(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    logger
        .info("Promoting partition: " + partition + " of topic: " + storeConfig.getStoreVersionName() + " to leader.");
    getStoreIngestionService().promoteToLeader(storeConfig, partition, leaderSessionIdChecker);
  }

  @Override
  public void demoteToStandby(
      VeniceStoreVersionConfig storeConfig,
      int partition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    logger
        .info("Demoting partition: " + partition + " of topic: " + storeConfig.getStoreVersionName() + " to standby.");
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
