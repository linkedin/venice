package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.store.AbstractStorageEngine;
import java.util.concurrent.atomic.AtomicReference;


public interface DaVinciIngestionBackend extends IngestionBackendBase {

  // removeStorageEngine removes the whole storage engine and delete all the data from disk.
  void removeStorageEngine(String topicName);

  // unsubscribeTopicPartition will stop subscribe topic's partition and delete partition data from storage.
  void unsubscribeTopicPartition(VeniceStoreConfig storeConfig, int partition, int timeoutInSeconds);

  // setStorageEngineReference is used by Da Vinci exclusively to speed up storage engine retrieval for read path.
  void setStorageEngineReference(String topicName, AtomicReference<AbstractStorageEngine> storageEngineReference);
}
