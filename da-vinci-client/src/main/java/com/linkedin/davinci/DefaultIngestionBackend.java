package com.linkedin.davinci;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


/**
 * DefaultIngestionBackend is the default ingestion backend implementation. Ingestion will be done in the same JVM as the application.
 */
public class DefaultIngestionBackend implements IngestionBackend {
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
  public void startConsumption(VeniceStoreConfig storeConfig, int partition) {
    AbstractStorageEngine storageEngine = getStorageService().openStoreForNewPartition(storeConfig, partition);
    if (topicStorageEngineReferenceMap.containsKey(storeConfig.getStoreName())) {
      topicStorageEngineReferenceMap.get(storeConfig.getStoreName()).set(storageEngine);
    }
    getStoreIngestionService().startConsumption(storeConfig, partition);
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
  public void unsubscribeTopicPartition(VeniceStoreConfig storeConfig, int partition, int timeoutInSeconds) {
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partition, 1, timeoutInSeconds);
    getStorageService().dropStorePartition(storeConfig, partition);
  }

  @Override
  public void promoteToLeader(VeniceStoreConfig storeConfig, int partition,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker leaderSessionIdChecker) {
    getStoreIngestionService().promoteToLeader(storeConfig, partition, leaderSessionIdChecker);
  }

  @Override
  public void demoteToStandby(VeniceStoreConfig storeConfig, int partition,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker leaderSessionIdChecker) {
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
