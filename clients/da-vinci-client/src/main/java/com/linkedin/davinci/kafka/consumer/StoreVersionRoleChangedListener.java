package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreVersionRoleChangedListener implements StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(StoreDataChangedListener.class);
  private final String storeName;

  private final int versionNumber;
  private TopicPartitionReplicaRole.VersionRole versionRole;
  private StoreIngestionTask storeIngestionTask;

  public StoreVersionRoleChangedListener(StoreIngestionTask storeIngestionTask, Store store, int versionNumber) {
    this.versionNumber = versionNumber;
    this.storeName = storeIngestionTask.getVersionTopic().getStoreName();
    this.versionRole = getStoreVersionRole(store);
    this.storeIngestionTask = storeIngestionTask;
    storeIngestionTask.setVersionRole(versionRole);
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

    TopicPartitionReplicaRole.VersionRole newVersionRole = getStoreVersionRole(store);
    if (!versionRole.equals(newVersionRole)) {
      LOGGER.info(
          "Store version role changed from previous version role: {} to new version role: {}",
          versionRole,
          newVersionRole);
      this.storeIngestionTask.setVersionRole(versionRole);
      this.versionRole = newVersionRole;
      this.storeIngestionTask.versionRoleChangeToTriggerResubscribe(newVersionRole);
    }
  }

  private TopicPartitionReplicaRole.VersionRole getStoreVersionRole(Store store) {
    try {
      int currentVersionNumber = store.getCurrentVersion();
      if (currentVersionNumber < versionNumber) {
        return TopicPartitionReplicaRole.VersionRole.FUTURE;
      } else if (currentVersionNumber > versionNumber) {
        return TopicPartitionReplicaRole.VersionRole.BACKUP;
      } else {
        return TopicPartitionReplicaRole.VersionRole.CURRENT;
      }
    } catch (VeniceNoStoreException e) {
      LOGGER.error("Unable to find store meta-data for {}", storeName, e);
      throw e;
    }
  }
}
