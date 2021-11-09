package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


/**
 * This service is in charge of cleaning up backup versions based on retention policy defined on store basis.
 * If it is not specified, the retention policy will be controlled by config: {@link ConfigKeys#CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS}.
 * The backup versions will become eligible for removal if the latest current version has been promoted for more
 * than configured retention time period.
 * If the specified retention time is 0, this service won't delete the backup version right after the latest version is
 * promoted to the new current version since there could be a delay before Routers receive the new version promotion notification.
 * Currently, the minimal retention time is hard-coded as 1 hour here: {@link StoreBackupVersionCleanupService#MINIMAL_BACKUP_VERSION_CLEANUP_DELAY}
 * to accommodate the delay between Controller and Router.
 */
public class StoreBackupVersionCleanupService extends AbstractVeniceService {
  private static final Logger LOGGER = Logger.getLogger(StoreBackupVersionCleanupService.class);
  /**
   * The minimal delay to cleanup backup version, and this is used to make sure all the Routers have enough
   * time to switch to the new promoted version.
   */
  private static final long MINIMAL_BACKUP_VERSION_CLEANUP_DELAY = TimeUnit.HOURS.toMillis(1);

  private final VeniceHelixAdmin admin;
  private final VeniceControllerMultiClusterConfig multiClusterConfig;
  private final Set<String> allClusters;
  private final Thread cleanupThread;
  private final long sleepInterval;
  private final long defaultBackupVersionRetentionMs;
  private boolean stop = false;

  private final Time time;

  public StoreBackupVersionCleanupService(VeniceHelixAdmin admin, VeniceControllerMultiClusterConfig multiClusterConfig) {
    this(admin, multiClusterConfig, new SystemTime());
  }

  protected StoreBackupVersionCleanupService(VeniceHelixAdmin admin, VeniceControllerMultiClusterConfig multiClusterConfig, Time time) {
    this.admin = admin;
    this.multiClusterConfig = multiClusterConfig;
    this.allClusters = multiClusterConfig.getClusters();
    this.cleanupThread = new Thread(new StoreBackupVersionCleanupTask(), "StoreBackupVersionCleanupTask");
    this.sleepInterval = TimeUnit.MINUTES.toMillis(5);
    this.defaultBackupVersionRetentionMs = multiClusterConfig.getBackupVersionDefaultRetentionMs();
    this.time = time;
  }

  @Override
  public boolean startInner() throws Exception {
    cleanupThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop = true;
    cleanupThread.interrupt();
  }

  protected static boolean whetherStoreReadyToBeCleanup(Store store, long defaultBackupVersionRetentionMs, Time time) {
    long backupVersionRetentionMs = store.getBackupVersionRetentionMs();
    if (backupVersionRetentionMs < 0) {
      backupVersionRetentionMs = defaultBackupVersionRetentionMs;
    }
    if (backupVersionRetentionMs < MINIMAL_BACKUP_VERSION_CLEANUP_DELAY) {
      backupVersionRetentionMs = MINIMAL_BACKUP_VERSION_CLEANUP_DELAY;
    }
    long latestVersionPromoteToCurrentTimestamp = store.getLatestVersionPromoteToCurrentTimestamp();
    return latestVersionPromoteToCurrentTimestamp + backupVersionRetentionMs < time.getMilliseconds();
  }

  /**
   * Using a separate function for store cleanup is to make it easy for testing.
   * @param store
   *
   * @return whether any backup version is removed or not
   */
  protected boolean cleanupBackupVersion(Store store, String clusterName) {
    if (!whetherStoreReadyToBeCleanup(store, defaultBackupVersionRetentionMs, time)) {
      // not ready to cleanup backup versions yet
      return false;
    }

    List<Version> versions = store.getVersions();
    List<Version> readyToBeRemovedVersions = new ArrayList<>();
    int currentVersion = store.getCurrentVersion();
    versions.forEach (v -> {
      if (v.getNumber() < currentVersion) {
        readyToBeRemovedVersions.add(v);
      }
    });
    if (readyToBeRemovedVersions.isEmpty()) {
      return false;
    }
    String storeName = store.getName();
    LOGGER.info("Started removing backup versions according to retention policy for store: " + storeName +  " in cluster: " + clusterName);
    readyToBeRemovedVersions.forEach(v -> {
      int versionNum = v.getNumber();
      LOGGER.info("Version: " + versionNum + " of store: " + storeName + " in cluster: " + clusterName +
          " will be removed according to backup version retention policy");
      try {
        admin.deleteOneStoreVersion(clusterName, storeName, versionNum);
      } catch (Exception e) {
        LOGGER.error("Encountered exception while trying to delete version: " + versionNum + " store: " + storeName +
            " in cluster: " + clusterName, e);
      }
    });
    LOGGER.info("Finished removing backup versions according to retention policy for store: " + storeName +  " in cluster: " + clusterName);
    return true;
  }

  private class StoreBackupVersionCleanupTask implements Runnable {

    @Override
    public void run() {
      boolean interruptReceived = false;
      while (!stop) {
        try {
          time.sleep(sleepInterval);
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException during sleep in StoreBackupVersionCleanupTask thread");
          break;
        }
        // loop all the clusters
        for (String clusterName : allClusters) {
          boolean cleanupEnabled = multiClusterConfig.getControllerConfig(clusterName).isBackupVersionRetentionBasedCleanupEnabled();
          if (!cleanupEnabled || !admin.isLeaderControllerFor(clusterName)) {
            // Only do backup version retention with cluster level config enabled in master controller for current cluster
            continue;
          }
          // Get all stores for current cluster
          List<Store> stores = admin.getAllStores(clusterName);
          for (Store store: stores) {
            boolean didCleanup = false;
            try {
              didCleanup = cleanupBackupVersion(store, clusterName);
            } catch (Exception e) {
              LOGGER.error("Encountered exception while handling backup version cleanup for store: " + store.getName() + " in cluster: " + clusterName, e);
            }
            if (didCleanup) {
              try {
                time.sleep(sleepInterval);
              } catch (InterruptedException e) {
                interruptReceived = true;
                LOGGER.error("Received InterruptedException during sleep in StoreBackupVersionCleanupTask thread");
                break;
              }
            }
          }
          if (interruptReceived) {
            break;
          }
        }
      }
      LOGGER.info("StoreBackupVersionCleanupTask stopped.");
    }
  }
}
