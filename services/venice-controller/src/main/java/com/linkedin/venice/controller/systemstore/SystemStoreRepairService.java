package com.linkedin.venice.controller.systemstore;

import static java.lang.Thread.currentThread;

import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.stats.SystemStoreHealthCheckStats;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.ReflectUtils;
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
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private ScheduledExecutorService checkServiceExecutor;
  private final Map<String, SystemStoreHealthCheckStats> clusterToSystemStoreHealthCheckStatsMap =
      new VeniceConcurrentHashMap<>();
  private SystemStoreHealthChecker healthChecker;

  public SystemStoreRepairService(
      VeniceParentHelixAdmin parentAdmin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository) {
    this.parentAdmin = parentAdmin;
    this.multiClusterConfigs = multiClusterConfigs;
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

    healthChecker = loadHealthChecker();

    checkServiceExecutor.scheduleWithFixedDelay(
        new SystemStoreRepairTask(
            parentAdmin,
            clusterToSystemStoreHealthCheckStatsMap,
            versionRefreshThresholdInDays,
            isRunning,
            healthChecker),
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
    closeChecker(healthChecker);
    LOGGER.info("SystemStoreRepairService is shutdown.");
  }

  private SystemStoreHealthChecker loadHealthChecker() {
    String className = multiClusterConfigs.getCommonConfig().getSystemStoreHealthCheckOverrideClassName();
    if (className != null && !className.isEmpty()) {
      try {
        Class<? extends SystemStoreHealthChecker> checkerClass = ReflectUtils.loadClass(className);
        SystemStoreHealthChecker checker = ReflectUtils.callConstructor(
            checkerClass,
            new Class[] { VeniceControllerMultiClusterConfig.class },
            new Object[] { multiClusterConfigs });
        LOGGER.info("Loaded system store health checker: {}", className);
        return checker;
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to load system store health checker class: {}. Falling back to heartbeat checker.",
            className,
            e);
      }
    }
    LOGGER.info("Using default heartbeat-based system store health checker.");
    return new HeartbeatBasedSystemStoreHealthChecker(parentAdmin, heartbeatWaitTimeInSeconds, isRunning);
  }

  private void closeChecker(SystemStoreHealthChecker checker) {
    if (checker != null) {
      try {
        checker.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing system store health checker: {}", checker.getClass().getName(), e);
      }
    }
  }

  final Map<String, SystemStoreHealthCheckStats> getClusterToSystemStoreHealthCheckStatsMap() {
    return clusterToSystemStoreHealthCheckStatsMap;
  }

  SystemStoreHealthChecker getHealthChecker() {
    return healthChecker;
  }
}
