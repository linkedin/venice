package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.log4j.Logger;


/**
 * DefaultIngestionBackend is the default ingestion backend implementation. Ingestion will be done in the same JVM as the application.
 */
public class DefaultIngestionBackend implements DaVinciIngestionBackend, VeniceIngestionBackend {
  private static final Logger logger = Logger.getLogger(DefaultIngestionBackend.class);
  private final StorageMetadataService storageMetadataService;
  private final StorageService storageService;
  private final KafkaStoreIngestionService storeIngestionService;
  private final Map<String, AtomicReference<AbstractStorageEngine>> topicStorageEngineReferenceMap = new VeniceConcurrentHashMap<>();


  public DefaultIngestionBackend(StorageMetadataService storageMetadataService, KafkaStoreIngestionService storeIngestionService, StorageService storageService) {
    this.storageMetadataService = storageMetadataService;
    this.storeIngestionService = storeIngestionService;
    this.storageService = storageService;
  }

  @Override
  public void startConsumption(VeniceStoreConfig storeConfig, int partition, Optional<LeaderFollowerStateType> leaderState) {
    logger.info("Retrieving storage engine for store " + storeConfig.getStoreName() + " partition " + partition);
    AbstractStorageEngine storageEngine = getStorageService().openStoreForNewPartition(storeConfig, partition);
    if (topicStorageEngineReferenceMap.containsKey(storeConfig.getStoreName())) {
        topicStorageEngineReferenceMap.get(storeConfig.getStoreName()).set(storageEngine);
    }
    logger.info("Retrieved storage engine for store " + storeConfig.getStoreName() + " partition " + partition
        + ". Starting consumption in ingestion service");
    getStoreIngestionService().startConsumption(storeConfig, partition, leaderState);
    logger.info("Completed starting consumption in ingestion service for store " + storeConfig.getStoreName() + " partition " + partition);
  }

  @Override
  public void stopConsumption(VeniceStoreConfig storeConfig, int partition) {
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
  public void dropStoragePartitionGracefully(VeniceStoreConfig storeConfig, int partition, int timeoutInSeconds) {
    if (storeIngestionService.isPartitionConsuming(storeConfig, partition)) {
      long startTimeInMs = System.currentTimeMillis();
      getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, 1, timeoutInSeconds);
      logger.info(String.format("Partition: %d of topic: %s has stopped consumption in %d ms.", partition, storeConfig.getStoreName(),
          LatencyUtils.getElapsedTimeInMs(startTimeInMs)));
    }
    getStoreIngestionService().resetConsumptionOffset(storeConfig, partition);
    getStorageService().dropStorePartition(storeConfig, partition);
    logger.info("Partition: " + partition + " of topic: " + storeConfig.getStoreName() + " has been dropped.");
  }

  @Override
  public void promoteToLeader(VeniceStoreConfig storeConfig, int partition,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    logger.info("Promoting partition: " + partition + " of topic: " + storeConfig.getStoreName() + " to leader.");
    getStoreIngestionService().promoteToLeader(storeConfig, partition, leaderSessionIdChecker);
  }

  @Override
  public void demoteToStandby(VeniceStoreConfig storeConfig, int partition,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    logger.info("Demoting partition: " + partition + " of topic: " + storeConfig.getStoreName() + " to standby.");
    getStoreIngestionService().demoteToStandby(storeConfig, partition, leaderSessionIdChecker);
  }

  @Override
  public void addIngestionNotifier(VeniceNotifier ingestionListener) {
    getStoreIngestionService().addCommonNotifier(ingestionListener);
  }

  @Override
  public void addOnlineOfflineIngestionNotifier(VeniceNotifier ingestionListener) {
    getStoreIngestionService().addOnlineOfflineModelNotifier(ingestionListener);
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
  public void setStorageEngineReference(String topicName, AtomicReference<AbstractStorageEngine> storageEngineReference) {
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
