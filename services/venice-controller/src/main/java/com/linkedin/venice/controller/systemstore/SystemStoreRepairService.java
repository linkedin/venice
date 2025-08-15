package com.linkedin.venice.controller.systemstore;

import static java.lang.Thread.currentThread;

import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.stats.SystemStoreHealthCheckStats;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is the system store repair service in Venice parent controller. It will issue repair task {@link SystemStoreRepairTask}
 * to check system store health and try to repair bad system store at its best effort.
 */
public class SystemStoreRepairService extends AbstractVeniceService {
  public static final Logger LOGGER = LogManager.getLogger(SystemStoreRepairService.class);
  private final int repairTaskIntervalInSeconds;
  private final VeniceParentHelixAdmin parentAdmin;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final int heartbeatWaitTimeInSeconds;
  private final int versionRefreshThresholdInDays;
  private ScheduledExecutorService checkServiceExecutor;
  private final Map<String, SystemStoreHealthCheckStats> clusterToSystemStoreHealthCheckStatsMap =
      new VeniceConcurrentHashMap<>();

  public SystemStoreRepairService(
      VeniceParentHelixAdmin parentAdmin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository) {
    this.parentAdmin = parentAdmin;
    this.repairTaskIntervalInSeconds =
        multiClusterConfigs.getCommonConfig().getParentSystemStoreRepairCheckIntervalSeconds();
    this.versionRefreshThresholdInDays =
        multiClusterConfigs.getCommonConfig().getParentSystemStoreVersionRefreshThresholdInDays();
    this.heartbeatWaitTimeInSeconds =
        multiClusterConfigs.getCommonConfig().getParentSystemStoreHeartbeatCheckWaitTimeSeconds();
    for (String clusterName: multiClusterConfigs.getClusters()) {
      if (multiClusterConfigs.getControllerConfig(clusterName).isParentSystemStoreRepairServiceEnabled()) {
        getClusterToSystemStoreHealthCheckStatsMap()
            .put(clusterName, new SystemStoreHealthCheckStats(metricsRepository, clusterName));
      }
    }
  }

  @Override
  public boolean startInner() {
    checkServiceExecutor = Executors.newScheduledThreadPool(1);
    isRunning.set(true);
    checkServiceExecutor.scheduleWithFixedDelay(
        new SystemStoreRepairTask(
            parentAdmin,
            clusterToSystemStoreHealthCheckStatsMap,
            heartbeatWaitTimeInSeconds,
            versionRefreshThresholdInDays,
            isRunning),
        repairTaskIntervalInSeconds,
        repairTaskIntervalInSeconds,
        TimeUnit.SECONDS);
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

  final Map<String, SystemStoreHealthCheckStats> getClusterToSystemStoreHealthCheckStatsMap() {
    return clusterToSystemStoreHealthCheckStatsMap;
  }
}
