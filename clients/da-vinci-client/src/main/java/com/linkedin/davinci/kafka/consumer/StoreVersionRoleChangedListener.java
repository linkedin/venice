package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreVersionRoleChangedListener implements StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(StoreDataChangedListener.class);
  private final String storeName;
  private TopicPartitionReplicaRole.VersionRole versionRole;
  private StoreIngestionTask storeIngestionTask;

  public StoreVersionRoleChangedListener(StoreIngestionTask storeIngestionTask) {
    this.storeName = storeIngestionTask.getVersionTopic().getStoreName();
    this.versionRole = storeIngestionTask.getStoreVersionRole();
    this.storeIngestionTask = storeIngestionTask;
  }

  @Override
  public void handleStoreCreated(Store store) {
    // no-op
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    // np-op
  }

  @Override
  public void handleStoreChanged(Store store) {
    if (!store.getName().equals(storeName)) {
      return;
    }

    TopicPartitionReplicaRole.VersionRole newVersionRole = storeIngestionTask.getStoreVersionRole();
    if (!versionRole.equals(newVersionRole)) {
      LOGGER.info(
          "Store version role changed from previous version role: {} to new version role: {}",
          versionRole,
          newVersionRole);
      this.storeIngestionTask.versionRoleChangeToTriggerResubscribe();
      this.versionRole = newVersionRole;
    }
  }
}
