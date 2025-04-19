package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.ProtocolVersionAutoDetectionStats;
import com.linkedin.venice.controllerapi.AdminOperationProtocolVersionControllerResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service is responsible for auto-detecting the admin operation protocol version for all clusters periodically.
 * ProtocolVersionAutoDetectionService is a background thread which wakes up regularly, each time it will:
 * 1. Get the current admin operation protocol versions from all controllers (parent + child) in the current cluster
 * and find the smallest version - good version to use.
 * 2. Get the admin operation protocol version from ZK.
 * 3. If the version in ZK is different from the current version, update the version in ZK.
 * To disable step 3, set the admin version for the cluster in ZK as -1.
 */
public class ProtocolVersionAutoDetectionService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(ProtocolVersionAutoDetectionService.class);
  private static final String PARENT_REGION_NAME = "parentRegion";
  private final ProtocolVersionAutoDetectionStats stats;
  private final VeniceHelixAdmin admin;
  private final String clusterName;
  private Thread runner;
  private ProtocolVersionDetectionTask protocolVersionDetectionTask;
  private final long sleepIntervalInMs;

  public ProtocolVersionAutoDetectionService(
      String clusterName,
      VeniceHelixAdmin admin,
      ProtocolVersionAutoDetectionStats stats,
      long sleepIntervalInMs) {
    this.admin = admin;
    this.stats = stats;
    this.clusterName = clusterName;
    this.sleepIntervalInMs = sleepIntervalInMs;
  }

  @Override
  public boolean startInner() throws Exception {
    protocolVersionDetectionTask = new ProtocolVersionDetectionTask();
    runner = new Thread(protocolVersionDetectionTask);
    runner.setName("ProtocolVersionDetectionTask");
    runner.setDaemon(true);
    runner.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    if (protocolVersionDetectionTask != null) {
      protocolVersionDetectionTask.setStop();
      runner.interrupt();
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
    // Map to store all consumers versions. key is the region name and value is a map of controller URL to version
    Map<String, Map<String, Long>> regionToControllerToVersionMap = new ConcurrentHashMap<>();

    // Get all versions for parent controllers
    Map<String, Long> parentClusterVersions = admin.getAdminOperationVersionFromControllers(clusterName);
    regionToControllerToVersionMap.put(PARENT_REGION_NAME, parentClusterVersions);

    // Get child controller clients for all regions
    Map<String, ControllerClient> controllerClientMap = admin.getControllerClientMap(clusterName);

    // Forward the request to all regions
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      Map<String, Long> controllerUrlToVersionMap = getControllerUrlToVersionMap(clusterName, entry);
      regionToControllerToVersionMap.put(entry.getKey(), controllerUrlToVersionMap);
    }

    LOGGER.info("All controller versions for cluster {}: {}", clusterName, regionToControllerToVersionMap);
    return regionToControllerToVersionMap.values()
        .stream()
        .flatMap(m -> m.values().stream())
        .min(Long::compare)
        .orElse(Long.MAX_VALUE);
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

  class ProtocolVersionDetectionTask implements Runnable {
    private volatile boolean stop = false;

    protected void setStop() {
      stop = true;
    }

    @Override
    public void run() {
      LOGGER.info("Started running {}", getClass().getSimpleName());
      while (!stop) {
        try {
          Thread.sleep(sleepIntervalInMs);
        } catch (InterruptedException e) {
          LOGGER.info("Received interruptedException while running ProtocolVersionDetectionTask, will exit");
          stats.recordProtocolVersionAutoDetectionErrorSensor();
          break;
        }

        try {
          // start the clock
          long startTime = System.currentTimeMillis();
          Long currentGoodVersion = getSmallestLocalAdminOperationProtocolVersionForAllConsumers(clusterName);
          Long upstreamVersion = AdminTopicMetadataAccessor
              .getAdminOperationProtocolVersion(admin.getAdminTopicMetadata(clusterName, Optional.empty()));
          LOGGER.info(
              "Current good Admin Operation version for cluster {} is {} and upstream version is {}",
              clusterName,
              currentGoodVersion,
              upstreamVersion);
          if (upstreamVersion != -1 && currentGoodVersion != Long.MAX_VALUE
              && !Objects.equals(currentGoodVersion, upstreamVersion)) {
            admin.updateAdminOperationProtocolVersion(clusterName, currentGoodVersion);
            LOGGER.info(
                "Updated admin operation protocol version in ZK for cluster {} from {} to {}",
                clusterName,
                upstreamVersion,
                currentGoodVersion);
          }
          long elapsedTimeInMs = LatencyUtils.getElapsedTimeFromMsToMs(startTime);
          stats.recordProtocolVersionAutoDetectionLatencySensor(elapsedTimeInMs);
        } catch (Exception e) {
          LOGGER.error("Received an exception while running ProtocolVersionDetectionTask", e);
          stats.recordProtocolVersionAutoDetectionErrorSensor();
          break;
        }
      }
      LOGGER.info("{} stopped", getClass().getSimpleName());
    }
  }
}
