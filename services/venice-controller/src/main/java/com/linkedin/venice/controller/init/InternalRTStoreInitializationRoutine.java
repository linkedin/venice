package com.linkedin.venice.controller.init;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class InternalRTStoreInitializationRoutine implements ClusterLeaderInitializationRoutine {
  private static final Logger LOGGER = LogManager.getLogger(InternalRTStoreInitializationRoutine.class);

  private final Function<String, String> storeNameSupplier;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final VeniceHelixAdmin admin;
  private final String keySchema;
  private final String valueSchema;

  public InternalRTStoreInitializationRoutine(
      Function<String, String> storeNameSupplier,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      VeniceHelixAdmin admin,
      String keySchema,
      String valueSchema) {
    this.storeNameSupplier = storeNameSupplier;
    this.multiClusterConfigs = multiClusterConfigs;
    this.admin = admin;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
  }

  /**
   * @see ClusterLeaderInitializationRoutine#execute(String)
   */
  @Override
  public void execute(String clusterName) {
    String storeName = storeNameSupplier.apply(clusterName);
    Store store = admin.getStore(clusterName, storeName);
    if (store == null) {
      admin.createStore(clusterName, storeName, VeniceConstants.SYSTEM_STORE_OWNER, keySchema, valueSchema, true);
      store = admin.getStore(clusterName, storeName);
      if (store == null) {
        throw new VeniceException("Unable to create or fetch store " + storeName);
      }
    } else {
      LOGGER.info("Internal store {} already exists in cluster {}", storeName, clusterName);
    }

    if (!store.isHybrid()) {
      UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setHybridOffsetLagThreshold(100L)
          .setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(7));
      admin.updateStore(clusterName, storeName, updateStoreQueryParams);
      store = admin.getStore(clusterName, storeName);
      if (!store.isHybrid()) {
        throw new VeniceException("Unable to update store " + storeName + " to a hybrid store");
      }
      LOGGER.info("Enabled hybrid for internal store " + storeName + " in cluster " + clusterName);
    }

    if (store.getCurrentVersion() <= 0) {
      int partitionCount = multiClusterConfigs.getControllerConfig(clusterName).getMinNumberOfPartitions();
      int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
      Version version = admin.incrementVersionIdempotent(
          clusterName,
          storeName,
          Version.guidBasedDummyPushId(),
          partitionCount,
          replicationFactor);
      // SOP is already sent by incrementVersionIdempotent. No need to write again.
      admin.writeEndOfPush(clusterName, storeName, version.getNumber(), false);
      store = admin.getStore(clusterName, storeName);
      if (store.getVersions().isEmpty()) {
        throw new VeniceException("Unable to initialize a version for store " + storeName);
      }
      LOGGER.info("Created a version for internal store {} in cluster {}", storeName, clusterName);
    }
  }
}
