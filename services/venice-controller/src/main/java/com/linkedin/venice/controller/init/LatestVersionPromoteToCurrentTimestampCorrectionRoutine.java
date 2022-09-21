package com.linkedin.venice.controller.init;

import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class LatestVersionPromoteToCurrentTimestampCorrectionRoutine implements ClusterLeaderInitializationRoutine {
  private static final Logger LOGGER = LogManager.getLogger(ClusterLeaderInitializationManager.class);
  VeniceHelixAdmin veniceHelixAdmin;

  public LatestVersionPromoteToCurrentTimestampCorrectionRoutine(VeniceHelixAdmin veniceHelixAdmin) {
    this.veniceHelixAdmin = veniceHelixAdmin;
  }

  /**
   * @see ClusterLeaderInitializationRoutine#execute(String)
   */
  @Override
  public void execute(String clusterToInit) {
    for (Store store: veniceHelixAdmin.getAllStores(clusterToInit)) {
      /**
       * If latest version promotion to current timestamp field is smaller than the version creation of the current version,
       * update it with the version creation time of current version + 24 hours.
       */
      Optional<Version> currentVersion = store.getVersion(store.getCurrentVersion());
      if (currentVersion.isPresent()) {
        long createdTime = currentVersion.get().getCreatedTime();
        if (createdTime > store.getLatestVersionPromoteToCurrentTimestamp()) {

          String storeName = store.getName();
          veniceHelixAdmin.storeMetadataUpdate(clusterToInit, storeName, storeToUpdate -> {
            storeToUpdate.setLatestVersionPromoteToCurrentTimestamp(createdTime + Time.MS_PER_DAY);
            return storeToUpdate;
          });

          LOGGER.info(
              "Correct the latest version promote to current timestamp to: {} for store: {}",
              createdTime + Time.MS_PER_DAY,
              storeName);
        }
      }
    }
  }

  @Override
  public String toString() {
    return "LatestVersionPromoteToCurrentTimestampCorrectionRoutine{}";
  }
}
