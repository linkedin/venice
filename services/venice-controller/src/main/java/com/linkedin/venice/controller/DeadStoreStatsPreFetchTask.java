package com.linkedin.venice.controller;

import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This task is responsible for pre-fetching dead store stats to then be leveraged by getDeadStores() method.
 * In the case where fetching dead stores synchronously could take a long time, this task will run in the background to
 * keep the dead store stats up to date in the background.
 */
public class DeadStoreStatsPreFetchTask implements Runnable, Closeable {
  private static final String TASK_ID_FORMAT = DeadStoreStatsPreFetchTask.class.getSimpleName() + " [cluster: %s] ";

  private final String clusterName;
  private final VeniceHelixAdmin admin;
  private final long refreshIntervalMs;
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final String taskId;
  private final Logger logger;

  public DeadStoreStatsPreFetchTask(String clusterName, VeniceHelixAdmin admin, long refreshIntervalMs) {
    this.clusterName = clusterName;
    this.admin = admin;
    this.refreshIntervalMs = refreshIntervalMs;
    this.taskId = String.format(TASK_ID_FORMAT, clusterName);
    this.logger = LogManager.getLogger(taskId);
  }

  @Override
  public void run() {
    logger.info("Started {}", taskId);
    isRunning.set(true);
    try {
      logger.debug("Initial fetch of dead store stats for cluster: {}", clusterName);
      admin.preFetchDeadStoreStats(clusterName, getStoresInCluster());
    } catch (Exception e) {
      logger.error("Error during initial fetch of dead store stats for cluster: {}", clusterName, e);
    }

    while (isRunning.get()) {
      try {
        Utils.sleep(refreshIntervalMs);
        long startTime = System.currentTimeMillis();
        logger.debug("Fetching dead store stats for cluster: {}", clusterName);
        admin.preFetchDeadStoreStats(clusterName, getStoresInCluster());
        logger.info("Successfully refreshed dead store stats in {} ms", System.currentTimeMillis() - startTime);
      } catch (Exception e) {
        logger.error("Error while refreshing dead store stats for cluster: {}", clusterName, e);
      }
    }
    logger.info("Stopped {}", taskId);
  }

  private List<StoreInfo> getStoresInCluster() {
    return admin.getAllStores(clusterName)
        .stream()
        .filter(Objects::nonNull)
        .map(StoreInfo::fromStore)
        .collect(Collectors.toList());
  }

  @Override
  public void close() {
    isRunning.set(false);
  }
}
