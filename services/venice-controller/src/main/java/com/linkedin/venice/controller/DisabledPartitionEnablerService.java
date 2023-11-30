package com.linkedin.venice.controller;

import com.linkedin.venice.service.AbstractVeniceService;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DisabledPartitionEnablerService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(DisabledPartitionEnablerService.class);

  private final VeniceHelixAdmin admin;
  private final VeniceControllerMultiClusterConfig multiClusterConfig;
  private final Set<String> allClusters;
  private final Thread cleanupThread;
  private final long sleepInterval;
  private final AtomicBoolean stop = new AtomicBoolean(false);

  public DisabledPartitionEnablerService(
      VeniceHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig) {
    this.admin = admin;
    this.multiClusterConfig = multiClusterConfig;
    this.allClusters = multiClusterConfig.getClusters();
    this.sleepInterval = TimeUnit.HOURS.toMillis(10);
    this.cleanupThread = new Thread(new DisabledPartitionEnablerTask(), "StoreBackupVersionCleanupTask");
  }

  @Override
  public boolean startInner() throws Exception {
    cleanupThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    cleanupThread.interrupt();
  }

  private class DisabledPartitionEnablerTask implements Runnable {
    @Override
    public void run() {
      boolean interruptReceived = false;
      while (!stop.get()) {
        try {
          Thread.sleep(sleepInterval);
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException during sleep in DisabledPartitionEnablerTask thread");
          break;
        }
        // loop all the clusters
        for (String clusterName: allClusters) {
          if (!multiClusterConfig.getControllerConfig(clusterName).isEnableDisabledReplicaEnabled()
              || !admin.isLeaderControllerFor(clusterName)) {
            continue;
          }
          boolean didEnable = false;
          try {
            didEnable = admin.enableDisabledPartition(clusterName, "", true);
          } catch (Exception e) {
            LOGGER.error("Encountered exception while enabling disabled partition in cluster: {}", clusterName, e);
          }
          if (didEnable) {
            try {
              Thread.sleep(sleepInterval);
            } catch (InterruptedException e) {
              interruptReceived = true;
              LOGGER.error("Received InterruptedException during sleep in DisabledPartitionEnablerTask thread");
              break;
            }
          }
        }
        if (interruptReceived) {
          break;
        }
      }
      LOGGER.info("DisabledPartitionEnablerTask stopped.");
    }
  }
}
