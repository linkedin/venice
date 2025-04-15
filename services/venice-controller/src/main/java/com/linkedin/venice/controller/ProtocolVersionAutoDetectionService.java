package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.ProtocolVersionAutoDetectionStats;
import com.linkedin.venice.controllerapi.AdminOperationProtocolVersionControllerResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service is responsible for auto-detecting the admin operation protocol version for all clusters periodically.
 * Each time ProtocolVersionDetectionTask runs, it will:
 * 1. Get the current admin operation protocol versions from all controllers (parent + child) in the current cluster
 * and find the smallest version - good version to use.
 * 2. Get the admin operation protocol version from ZK.
 * 3. If the version in ZK is different from the current version, update the version in ZK.
 */
public class ProtocolVersionAutoDetectionService extends AbstractVeniceService {
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final Set<String> allClusters;
  private static final Logger LOGGER = LogManager.getLogger(ProtocolVersionAutoDetectionService.class);
  private final VeniceParentHelixAdmin veniceParentHelixAdmin;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final ProtocolVersionAutoDetectionStats stats;
  final ScheduledExecutorService executor;

  public ProtocolVersionAutoDetectionService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      ProtocolVersionAutoDetectionStats protocolVersionAutoDetectionStats) {
    this.veniceParentHelixAdmin = admin;
    this.multiClusterConfigs = multiClusterConfigs;
    this.allClusters = multiClusterConfigs.getClusters();
    this.stats = protocolVersionAutoDetectionStats;
    executor = Executors
        .newScheduledThreadPool(multiClusterConfigs.getAdminOperationProtocolVersionAutoDetectionThreadCount());
  }

  @Override
  public boolean startInner() throws Exception {
    executor.scheduleAtFixedRate(
        new ProtocolVersionDetectionTask(),
        0,
        multiClusterConfigs.getAdminOperationProtocolVersionAutoDetectionIntervalMs(),
        TimeUnit.MILLISECONDS);
    LOGGER.info("ProtocolVersionAutoDetectionService is started");
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    executor.shutdown();

    try {
      // Wait for the executor to terminate
      if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        executor.shutdownNow(); // Force shutdown if not terminated in 60 seconds
      }
    } catch (InterruptedException e) {
      LOGGER.error("Error shutting down ProtocolVersionAutoDetectionService", e);
      executor.shutdownNow();
    }

  }

  class ProtocolVersionDetectionTask implements Runnable {
    @Override
    public void run() {
      LOGGER.info("Started running {}", getClass().getSimpleName());
      while (!stop.get()) {
        try {
          for (String clusterName: allClusters) {
            if (!veniceParentHelixAdmin.isLeaderControllerFor(clusterName)) {
              continue;
            }
            // start the clock
            long startTime = System.currentTimeMillis();
            Long currentGoodVersion = getLocalAdminOperationProtocolVersionForAllConsumers(clusterName);
            Long upstreamVersion = getAdminOperationProtocolVersionInZK(clusterName);
            LOGGER.info(
                "Current good Admin Operation version for cluster {} is {} and upstream version is {}",
                clusterName,
                currentGoodVersion,
                upstreamVersion);
            if (upstreamVersion != -1 && !Objects.equals(currentGoodVersion, upstreamVersion)) {
              updateAdminOperationProtocolVersionInZK(clusterName, currentGoodVersion);
              LOGGER.info(
                  "Updated admin operation protocol version in ZK for cluster {} from {} to {}",
                  clusterName,
                  upstreamVersion,
                  currentGoodVersion);
            }
            long elapsedTimeFromMsToMs = LatencyUtils.getElapsedTimeFromMsToMs(startTime);
            stats.recordProtocolVersionAutoDetectionLatencySensor(elapsedTimeFromMsToMs);
          }
        } catch (Exception e) {
          LOGGER.warn("Received exception while running ProtocolVersionDetectionTask", e);
          stats.recordProtocolVersionAutoDetectionErrorSensor();
        } catch (Throwable throwable) {
          LOGGER.warn("Received a throwable while running ProtocolVersionDetectionTask", throwable);
          stats.recordProtocolVersionAutoDetectionThrowableSensor();
        }
      }
    }

    /**
     * Get the smallest local admin operation protocol version for all consumers in the given cluster.
     * This will help to ensure that all consumers are on the same page regarding the protocol version.
     *
     * @param clusterName The name of the cluster to check.
     * @return The smallest local admin operation protocol version for all consumers (parent + child controllers)
     * in the cluster.
     */
    public long getLocalAdminOperationProtocolVersionForAllConsumers(String clusterName) {
      // Get all versions for parent clusters
      Map<String, Long> parentClusterVersions =
          veniceParentHelixAdmin.getVeniceHelixAdmin().getAdminOperationVersionFromControllers(clusterName);
      Map<String, Map<String, Long>> allControllerVersions = new ConcurrentHashMap<>();
      allControllerVersions.put("parent", parentClusterVersions);

      // Get child controller clients for all colos
      Map<String, ControllerClient> controllerClientMap =
          veniceParentHelixAdmin.getVeniceHelixAdmin().getControllerClientMap(clusterName);

      // Forward the request to all colos
      for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
        ControllerClient controllerClient = entry.getValue();
        AdminOperationProtocolVersionControllerResponse response =
            controllerClient.getAdminOperationProtocolVersionFromControllers(clusterName);
        Map<String, Long> childColoMap = response.getControllerUrlToVersionMap();
        allControllerVersions.put(entry.getKey(), childColoMap);
      }

      LOGGER.info("All controller versions for cluster {}: {}", clusterName, allControllerVersions);
      return getSmallestVersion(allControllerVersions);
    }

    public Long getAdminOperationProtocolVersionInZK(String clusterName) {
      Map<String, Long> metadata = veniceParentHelixAdmin.getAdminTopicMetadata(clusterName, Optional.empty());
      return AdminTopicMetadataAccessor.getAdminOperationProtocolVersion(metadata);
    }

    public void updateAdminOperationProtocolVersionInZK(String clusterName, Long protocolVersion) {
      veniceParentHelixAdmin.updateAdminOperationProtocolVersion(clusterName, protocolVersion);
    }

    private long getSmallestVersion(Map<String, Map<String, Long>> versions) {
      return versions.values().stream().flatMap(m -> m.values().stream()).min(Long::compare).orElse(Long.MAX_VALUE);
    }
  }
}
