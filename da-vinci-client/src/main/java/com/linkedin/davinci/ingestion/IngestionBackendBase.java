package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import java.io.Closeable;


public interface IngestionBackendBase extends Closeable {

  void startConsumption(VeniceStoreConfig storeConfig, int partition);

  void stopConsumption(VeniceStoreConfig storeConfig, int partition);

  void killConsumptionTask(String topicName);

  void addIngestionNotifier(VeniceNotifier ingestionListener);

  StorageMetadataService getStorageMetadataService();

  KafkaStoreIngestionService getStoreIngestionService();

  StorageService getStorageService();
}
