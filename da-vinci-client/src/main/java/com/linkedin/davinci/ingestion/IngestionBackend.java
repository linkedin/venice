package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;


public interface IngestionBackend extends Closeable {

  void startConsumption(VeniceStoreConfig storeConfig, int partition);

  void stopConsumption(VeniceStoreConfig storeConfig, int partition);

  void killConsumptionTask(String topicName);

  void removeStorageEngine(String topicName);

  void unsubscribeTopicPartition(VeniceStoreConfig storeConfig, int partition, int timeoutInSeconds);

  void promoteToLeader(VeniceStoreConfig storeConfig, int partition,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker leaderSessionIdChecker);

  void demoteToStandby(VeniceStoreConfig storeConfig, int partition,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker leaderSessionIdChecker);

  void addIngestionNotifier(VeniceNotifier ingestionListener);

  void addOnlineOfflineIngestionNotifier(VeniceNotifier ingestionListener);

  void addLeaderFollowerIngestionNotifier(VeniceNotifier ingestionListener);

  void addPushStatusNotifier(VeniceNotifier pushStatusNotifier);
  
  // This API is used by Da Vinci to speed up storage engine retrieval for read path.
  void setStorageEngineReference(String topicName, AtomicReference<AbstractStorageEngine> storageEngineReference);

  StorageMetadataService getStorageMetadataService();

  KafkaStoreIngestionService getStoreIngestionService();

  StorageService getStorageService();
}
