package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreVersionRoleChangedListener implements StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(StoreDataChangedListener.class);
  private final String storeName;

  private boolean isCurrentVersion;
  private StoreIngestionTask storeIngestionTask;

  private BooleanSupplier isCurrentVersionSupplier;

  public StoreVersionRoleChangedListener(
      PubSubTopic versionTopic,
      BooleanSupplier isCurrentVersionSupplier,
      StoreIngestionTask storeIngestionTask) {
    this.storeName = versionTopic.getStoreName();
    this.isCurrentVersionSupplier = isCurrentVersionSupplier;
    this.isCurrentVersion = isCurrentVersionSupplier.getAsBoolean();
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

    if (isCurrentVersionSupplier.getAsBoolean() != isCurrentVersion) {
      LOGGER.info("Store version role changed from current version {} to non-current version: {}");
      this.storeIngestionTask.versionRoleChange();
      this.isCurrentVersion = isCurrentVersionSupplier.getAsBoolean();
    }
  }
}
