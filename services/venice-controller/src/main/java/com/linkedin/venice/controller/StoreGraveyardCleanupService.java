package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.PreconditionCheckFailedException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service is in charge of removing stores under /venice/<cluster>/StoreGraveyard zk path.
 *
 * A store is removable from graveyard when the following resources do not exist:
 * 1. Kafka topics, including user store and its system stores version, streaming reprocessing and real-time topics.
 * 2. Helix resources, including resources created by Helix for user store and its system stores versions.
 * 3. Offline pushes, including OfflinePushes zk nodes created by Venice for user store and its system stores versions.
 *
 * The service runs in parent controller only. Parent controller find removable stores and calls child controllers.
 * Each child controller checks if the store can be removed from graveyard in the child colo. If so, remove it and
 * respond OK. Otherwise, respond with error. If all child controllers respond OK, parent controller will remove
 * the store from graveyard in parent colo.
 *
 * The service only removes batch-only stores for now because hybrid ETL needs graveyard to guarantee version numbers
 * are not reused in recreated stores before ETL off-boarding steps.
 */
public class StoreGraveyardCleanupService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(StoreGraveyardCleanupService.class);

  private final VeniceParentHelixAdmin admin;
  private final VeniceControllerMultiClusterConfig multiClusterConfig;
  private final Thread cleanupThread;
  private final long sleepIntervalBetweenListFetchMs; // default is 15 min
  private final long sleepIntervalBetweenStore = TimeUnit.MINUTES.toMillis(1);
  private final Time time = new SystemTime();
  private boolean stop = false;

  public StoreGraveyardCleanupService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig) {
    this.admin = admin;
    this.multiClusterConfig = multiClusterConfig;
    this.sleepIntervalBetweenListFetchMs = multiClusterConfig.getGraveyardCleanupSleepIntervalBetweenListFetchMs();
    this.cleanupThread = new Thread(new StoreGraveyardCleanupTask(), "StoreGraveyardCleanupTask");
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

  private class StoreGraveyardCleanupTask implements Runnable {
    @Override
    public void run() {
      LOGGER.info("Started running {}", getClass().getSimpleName());
      while (!stop) {
        try {
          time.sleep(sleepIntervalBetweenListFetchMs);
          // loop all clusters
          for (String clusterName: multiClusterConfig.getClusters()) {
            boolean cleanupForBatchOnlyStoreEnabled =
                multiClusterConfig.getControllerConfig(clusterName).isGraveyardCleanupForBatchOnlyStoreEnabled();
            if (!cleanupForBatchOnlyStoreEnabled || !admin.isLeaderControllerFor(clusterName)) {
              // Only clean up when config is enabled and this is the leader controller for current cluster
              continue;
            }
            // List all stores from graveyard for current cluster
            List<String> storeNames = admin.getStoreGraveyard().listStoreNamesFromGraveyard(clusterName);
            for (String storeName: storeNames) {
              boolean didCleanup = false;
              try {
                Store store = admin.getStoreGraveyard().getStoreFromGraveyard(clusterName, storeName);
                if (store != null && !store.isHybrid() && !store.isIncrementalPushEnabled()) {
                  admin.removeStoreFromGraveyard(clusterName, storeName);
                  didCleanup = true;
                }
              } catch (PreconditionCheckFailedException preconditionCheckFailedException) {
                // Ignore PreconditionCheckFailedException which indicates store graveyard is not ready for removal.
              } catch (Exception exception) {
                LOGGER.error(
                    "Encountered exception while handling store graveyard cleanup for store: {} in cluster: {}",
                    storeName,
                    clusterName,
                    exception);
              }
              if (didCleanup) {
                time.sleep(sleepIntervalBetweenStore);
              }
            }
          }
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException during sleep in {} thread", getClass().getSimpleName());
          break;
        }
      }
      LOGGER.info("{} stopped", getClass().getSimpleName());
    }
  }
}
