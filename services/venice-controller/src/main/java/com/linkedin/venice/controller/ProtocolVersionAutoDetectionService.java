package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.ProtocolVersionAutoDetectionStats;
import com.linkedin.venice.controllerapi.AdminOperationProtocolVersionControllerResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
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
 * To disable step 3, set the admin version for the cluster in ZK as -1.
 */
public class ProtocolVersionAutoDetectionService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(ProtocolVersionAutoDetectionService.class);
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final Set<String> allClusters;
  private final Map<String, ProtocolVersionAutoDetectionStats> clusterToStatsMap = new HashMap<>();
  private final VeniceParentHelixAdmin veniceParentHelixAdmin;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  final ScheduledExecutorService executor;

  public ProtocolVersionAutoDetectionService(
      VeniceParentHelixAdmin parentHelixAdmin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository,
      Optional<Map<String, ProtocolVersionAutoDetectionStats>> clusterToStatsMap) {
    this.veniceParentHelixAdmin = parentHelixAdmin;
    this.multiClusterConfigs = multiClusterConfigs;
    this.allClusters = multiClusterConfigs.getClusters();
    executor = Executors
        .newScheduledThreadPool(multiClusterConfigs.getAdminOperationProtocolVersionAutoDetectionThreadCount());

    if (clusterToStatsMap.isPresent()) {
      // If the caller has provided a map of stats, use it - for testing purpose
      this.clusterToStatsMap.putAll(clusterToStatsMap.get());
    } else {
      // Otherwise, create a new map of stats for each cluster - default option
      for (String clusterName: this.allClusters) {
        this.clusterToStatsMap.put(
            clusterName,
            new ProtocolVersionAutoDetectionStats(
                metricsRepository,
                "admin_operation_protocol_version_auto_detection_service_" + clusterName));
      }
    }
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

  private ProtocolVersionAutoDetectionStats getClusterStat(String clusterName) {
    return clusterToStatsMap.get(clusterName);
  }

  class ProtocolVersionDetectionTask implements Runnable {
    @Override
    public void run() {
      LOGGER.info("Started running {}", getClass().getSimpleName());
      while (!stop.get()) {
        for (String clusterName: allClusters) {
          if (!veniceParentHelixAdmin.isLeaderControllerFor(clusterName)) {
            continue;
          }
          try {
            // start the clock
            long startTime = System.currentTimeMillis();
            Long currentGoodVersion = getSmallestLocalAdminOperationProtocolVersionForAllConsumers(clusterName);
            Long upstreamVersion = AdminTopicMetadataAccessor.getAdminOperationProtocolVersion(
                veniceParentHelixAdmin.getAdminTopicMetadata(clusterName, Optional.empty()));
            LOGGER.info(
                "Current good Admin Operation version for cluster {} is {} and upstream version is {}",
                clusterName,
                currentGoodVersion,
                upstreamVersion);
            if (upstreamVersion != -1 && !Objects.equals(currentGoodVersion, upstreamVersion)) {
              veniceParentHelixAdmin.updateAdminOperationProtocolVersion(clusterName, currentGoodVersion);
              LOGGER.info(
                  "Updated admin operation protocol version in ZK for cluster {} from {} to {}",
                  clusterName,
                  upstreamVersion,
                  currentGoodVersion);
            }
            long elapsedTimeFromMsToMs = LatencyUtils.getElapsedTimeFromMsToMs(startTime);
            getClusterStat(clusterName).recordProtocolVersionAutoDetectionLatencySensor(elapsedTimeFromMsToMs);
          } catch (Exception e) {
            LOGGER.warn("Received exception while running ProtocolVersionDetectionTask", e);
            getClusterStat(clusterName).recordProtocolVersionAutoDetectionErrorSensor();
          } catch (Throwable throwable) {
            LOGGER.warn("Received a throwable while running ProtocolVersionDetectionTask", throwable);
            getClusterStat(clusterName).recordProtocolVersionAutoDetectionThrowableSensor();
          }
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
    public long getSmallestLocalAdminOperationProtocolVersionForAllConsumers(String clusterName) {
      // Map to store all consumers versions
      Map<String, Map<String, Long>> regionToControllerToVersionMap = new ConcurrentHashMap<>();

      // Get all versions for parent clusters
      Map<String, Long> parentClusterVersions =
          veniceParentHelixAdmin.getVeniceHelixAdmin().getAdminOperationVersionFromControllers(clusterName);
      regionToControllerToVersionMap.put("parentRegion", parentClusterVersions);

      // Get child controller clients for all regions
      Map<String, ControllerClient> controllerClientMap =
          veniceParentHelixAdmin.getVeniceHelixAdmin().getControllerClientMap(clusterName);

      // Forward the request to all regions
      for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
        Map<String, Long> controllerUrlToVersionMap = getControllerUrlToVersionMap(clusterName, entry);
        regionToControllerToVersionMap.put(entry.getKey(), controllerUrlToVersionMap);
      }

      LOGGER.info("All controller versions for cluster {}: {}", clusterName, regionToControllerToVersionMap);
      return getSmallestVersion(regionToControllerToVersionMap);
    }

    private Map<String, Long> getControllerUrlToVersionMap(
        String clusterName,
        Map.Entry<String, ControllerClient> regionToControllerClient) {
      ControllerClient controllerClient = regionToControllerClient.getValue();
      AdminOperationProtocolVersionControllerResponse response =
          controllerClient.getAdminOperationProtocolVersionFromControllers(clusterName);
      if (response.isError()) {
        throw new VeniceException(
            "Failed to get admin operation protocol version from child controller " + regionToControllerClient.getKey()
                + ": " + response.getError());
      }
      return response.getControllerUrlToVersionMap();
    }

    /**
     * Get the smallest version from a map of versions.
     *
     * @param versions A map of versions, where the key is regionName,
     *                and the value is another map of controllerUrl to admin operation protocol version.
     * @return The smallest version as a long.
     */
    private long getSmallestVersion(Map<String, Map<String, Long>> versions) {
      return versions.values().stream().flatMap(m -> m.values().stream()).min(Long::compare).orElse(Long.MAX_VALUE);
    }
  }
}
