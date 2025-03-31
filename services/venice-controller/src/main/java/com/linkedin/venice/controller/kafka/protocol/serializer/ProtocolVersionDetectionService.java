package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.LocalAdminOperationProtocolVersionResponse;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ProtocolVersionDetectionService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(ProtocolVersionDetectionService.class);
  private final VeniceParentHelixAdmin admin;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  final ScheduledExecutorService executor;

  public ProtocolVersionDetectionService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this.admin = admin;
    this.multiClusterConfigs = multiClusterConfigs;
    executor = Executors.newScheduledThreadPool(1);
  }

  @Override
  public boolean startInner() throws Exception {
    executor.scheduleAtFixedRate(
        new ProtocolVersionDetectionTask(multiClusterConfigs.getClusters()),
        0,
        TimeUnit.MINUTES.toMillis(10),
        TimeUnit.MILLISECONDS);
    LOGGER.info("service is started");
    return false;
  }

  @Override
  public void stopInner() throws Exception {
    executor.shutdown();

    try {
      // Wait for the executor to terminate
      if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        executor.shutdownNow(); // Force shutdown if not terminated in 60 seconds
      }
    } catch (InterruptedException e) {
      LOGGER.error("Error shutting down ProtocolVersionDetectionService", e);
      executor.shutdownNow();
    }

  }

  private class ProtocolVersionDetectionTask implements Runnable {
    private final Set<String> clusters;

    private ProtocolVersionDetectionTask(Set<String> clusters) {
      this.clusters = clusters;
    }

    @Override
    public void run() {
      for (String clusterName: admin.getClustersLeaderOf()) {
        if (admin.isLeaderControllerFor(clusterName)) {
          Long currentGoodVersion = getLocalAdminOperationProtocolVersionForAllConsumers(clusterName);
          Long upstreamVersion = getAdminOperationProtocolVersionInZK(clusterName);
          if (upstreamVersion != -1 && !Objects.equals(currentGoodVersion, upstreamVersion)) {
            updateAdminOperationProtocolVersionInZK(clusterName, upstreamVersion);
          }
        } else {
          LOGGER.info("Not the leader controller for cluster: {}, skipping protocol version detection.", clusterName);
        }
      }
    }

    /**
     * Get the smallest local admin operation protocol version for all consumers in the given cluster.
     * This will help to ensure that all consumers are on the same page regarding the protocol version.
     *
     * @param clusterName The name of the cluster to check.
     * @return The smallest local admin operation protocol version for all consumers in the cluster.
     */
    public long getLocalAdminOperationProtocolVersionForAllConsumers(String clusterName) {
      // TODO: Need to get all parent controllers as well
      Map<String, ControllerClient> controllerClientMap =
          admin.getVeniceHelixAdmin().getControllerClientMap(clusterName);

      long goodVersion = Long.MAX_VALUE;
      for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
        String consumerName = entry.getKey();
        ControllerClient controllerClient = entry.getValue();
        LocalAdminOperationProtocolVersionResponse response =
            controllerClient.getLocalAdminOperationProtocolVersion(clusterName);
        long protocolVersion = response.getAdminOperationProtocolVersion();
        LOGGER.info("Consumer: {} has protocol version: {}", consumerName, protocolVersion);
        goodVersion = Math.min(protocolVersion, goodVersion);
      }

      return goodVersion;
    }

    public Long getAdminOperationProtocolVersionInZK(String clusterName) {
      Map<String, Long> metadata = admin.getAdminTopicMetadata(clusterName, Optional.empty());
      return AdminTopicMetadataAccessor.getAdminOperationProtocolVersion(metadata);
    }

    public void updateAdminOperationProtocolVersionInZK(String clusterName, Long protocolVersion) {
      admin.updateAdminOperationProtocolVersion(clusterName, protocolVersion);
    }
  }
}
