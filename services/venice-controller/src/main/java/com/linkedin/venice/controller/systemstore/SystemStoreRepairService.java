package com.linkedin.venice.controller.systemstore;

import static java.lang.Thread.currentThread;

import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SystemStoreRepairService extends AbstractVeniceService {
  public static final Logger LOGGER = LogManager.getLogger(SystemStoreRepairService.class);
  private final int repairTaskIntervalInHours;
  private final VeniceParentHelixAdmin parentAdmin;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final AtomicLong badMetaStoreCount = new AtomicLong(0);
  private final AtomicLong badPushStatusStoreCount = new AtomicLong(0);
  private final int heartbeatWaitTimeSeconds;
  private ScheduledExecutorService checkServiceExecutor;
  private final int maxRepairRetry;
  // private final SystemStoreCheckStats systemStoreCheckStats;

  public SystemStoreRepairService(
      VeniceParentHelixAdmin parentAdmin,
      int repairTaskIntervalInHours,
      int maxRepairRetry,
      int heartbeatWaitTimeSeconds) {
    this.parentAdmin = parentAdmin;
    this.repairTaskIntervalInHours = repairTaskIntervalInHours;
    this.heartbeatWaitTimeSeconds = heartbeatWaitTimeSeconds;
    this.maxRepairRetry = maxRepairRetry;
    /*
    this.systemStoreCheckStats =
        new SystemStoreCheckStats(metricsRepository, clusterName, badMetaStoreCount::get, badPushStatusStoreCount::get);
    
     */
  }

  @Override
  public boolean startInner() {
    checkServiceExecutor = Executors.newScheduledThreadPool(1);
    isRunning.set(true);
    checkServiceExecutor.scheduleWithFixedDelay(
        new SystemStoreRepairTask(parentAdmin, maxRepairRetry, heartbeatWaitTimeSeconds, isRunning),
        repairTaskIntervalInHours,
        repairTaskIntervalInHours,
        TimeUnit.HOURS);
    LOGGER.info("SystemStoreRepairService is started.");
    return true;
  }

  @Override
  public void stopInner() {
    isRunning.set(false);
    checkServiceExecutor.shutdownNow();
    try {
      if (!checkServiceExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOGGER.warn("Current task in SystemStoreRepairService is not terminated after 5 seconds.");
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
    LOGGER.info("SystemStoreRepairService is shutdown.");
  }

}
