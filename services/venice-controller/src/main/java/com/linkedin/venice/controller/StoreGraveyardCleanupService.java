package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.ResourceStillExistsException;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.Stat;


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
 */
public class StoreGraveyardCleanupService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(StoreGraveyardCleanupService.class);

  private final VeniceParentHelixAdmin admin;
  private final VeniceControllerMultiClusterConfig multiClusterConfig;
  private final Thread cleanupThread;
  private final int sleepIntervalBetweenListFetchMinutes; // default is 15 min
  private final Time time = new SystemTime();
  private boolean stop = false;

  public StoreGraveyardCleanupService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig) {
    this.admin = admin;
    this.multiClusterConfig = multiClusterConfig;
    this.sleepIntervalBetweenListFetchMinutes =
        multiClusterConfig.getGraveyardCleanupSleepIntervalBetweenListFetchMinutes();
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
          time.sleep((long) sleepIntervalBetweenListFetchMinutes * Time.MS_PER_MINUTE);
          // loop all clusters
          for (String clusterName: multiClusterConfig.getClusters()) {
            VeniceControllerConfig clusterConfig = multiClusterConfig.getControllerConfig(clusterName);
            boolean cleanupEnabled = clusterConfig.isStoreGraveyardCleanupEnabled();
            int delayInMinutes = clusterConfig.getStoreGraveyardCleanupDelayMinutes();
            if (!cleanupEnabled || !admin.isLeaderControllerFor(clusterName)) {
              // Only clean up when config is enabled and this is the leader controller for current cluster
              continue;
            }
            // List all stores from graveyard for current cluster
            List<String> storeNames = admin.getStoreGraveyard().listStoreNamesFromGraveyard(clusterName);
            for (String storeName: storeNames) {
              boolean didCleanup = false;
              try {
                Stat stat = new Stat();
                admin.getStoreGraveyard().getStoreFromGraveyard(clusterName, storeName, stat);
                if ((time.getMilliseconds() - stat.getMtime()) / Time.MS_PER_MINUTE > delayInMinutes) {
                  admin.removeStoreFromGraveyard(clusterName, storeName);
                  didCleanup = true;
                }
              } catch (ResourceStillExistsException resourceStillExistsException) {
                // Ignore ResourceStillExistsException which indicates store graveyard is not ready for removal.
              } catch (Exception exception) {
                LOGGER.error(
                    "Encountered exception while handling store graveyard cleanup for store: {} in cluster: {}",
                    storeName,
                    clusterName,
                    exception);
              }
              if (didCleanup) {
                time.sleep(Time.MS_PER_MINUTE);
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
