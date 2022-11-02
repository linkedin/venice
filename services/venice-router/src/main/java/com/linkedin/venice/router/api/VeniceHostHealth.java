package com.linkedin.venice.router.api;

import com.linkedin.alpini.router.api.HostHealthMonitor;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.HostHealthStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@code VeniceHostHealth} the aggregate statistics for {@linkplain HostHealthStats}.
 * It recomputes the aggregate metrics for the host healthiness of the cluster.
 */
public class VeniceHostHealth implements HostHealthMonitor<Instance> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceHostHealth.class);
  private final int maxPendingConnectionPerHost;
  private final int routerPendingConnResumeThreshold;
  private final boolean statefulRouterHealthCheckEnabled;
  private final long fullPendingQueueServerOORMs;
  protected Set<String> unhealthyHosts = new ConcurrentSkipListSet<>();
  private final Map<String, Long> pendingRequestUnhealthyTimeMap = new VeniceConcurrentHashMap<>();

  private final LiveInstanceMonitor liveInstanceMonitor;
  private final StorageNodeClient storageNodeClient;
  private final RouteHttpRequestStats routeHttpRequestStats;
  private final AggHostHealthStats aggHostHealthStats;

  public VeniceHostHealth(
      LiveInstanceMonitor liveInstanceMonitor,
      StorageNodeClient storageNodeClient,
      VeniceRouterConfig config,
      RouteHttpRequestStats routeHttpRequestStats,
      AggHostHealthStats aggHostHealthStats) {
    this.routeHttpRequestStats = routeHttpRequestStats;
    this.statefulRouterHealthCheckEnabled = config.isStatefulRouterHealthCheckEnabled();
    this.maxPendingConnectionPerHost = config.getRouterUnhealthyPendingConnThresholdPerRoute();
    this.routerPendingConnResumeThreshold = config.getRouterPendingConnResumeThresholdPerRoute();
    this.fullPendingQueueServerOORMs = config.getFullPendingQueueServerOORMs();
    this.liveInstanceMonitor = liveInstanceMonitor;
    this.storageNodeClient = storageNodeClient;
    this.aggHostHealthStats = aggHostHealthStats;
  }

  /**
   * Mark that something is wrong with an entire host and it should not be used for queries.
   *
   * @param instance
   */
  public void setHostAsUnhealthy(Instance instance) {
    String identifier = instance.getNodeId();
    unhealthyHosts.add(identifier);
    LOGGER.info("Marking {} as unhealthy until it passes the next health check.", identifier);
    aggHostHealthStats.recordUnhealthyHostCountCausedByRouterHeartBeat(unhealthyHosts.size());
  }

  /**
   * If the host is marked as unhealthy before, remove it from the unhealthy host set and log this
   * status change.
   *
   * @param hostname
   */
  public void setHostAsHealthy(Instance hostname) {
    String identifier = hostname.getNodeId();
    if (unhealthyHosts.contains(identifier)) {
      unhealthyHosts.remove(identifier);
      LOGGER.info("Marking {} back to healthy host", identifier);
    }
    aggHostHealthStats.recordUnhealthyHostCountCausedByRouterHeartBeat(unhealthyHosts.size());
  }

  @Override
  public boolean isHostHealthy(Instance instance, String partitionName) {
    String nodeId = instance.getNodeId();
    if (!liveInstanceMonitor.isInstanceAlive(instance)) {
      aggHostHealthStats.recordUnhealthyHostOfflineInstance(nodeId);
      return false;
    }

    if (unhealthyHosts.contains(instance.getNodeId())) {
      aggHostHealthStats.recordUnhealthyHostHeartBeatFailure(instance.getNodeId());
      return false;
    }

    if (!storageNodeClient.isInstanceReadyToServe(nodeId)) {
      aggHostHealthStats.recordUnhealthyHostDelayJoin(nodeId);
      return false;
    }

    if (isPendingRequestQueueUnhealthy(instance.getNodeId())) {
      aggHostHealthStats.recordUnhealthyHostTooManyPendingRequest(nodeId);
      // Record the unhealthy node count because of pending queue check
      aggHostHealthStats.recordUnhealthyHostCountCausedByPendingQueue(pendingRequestUnhealthyTimeMap.size());
      return false;
    }

    return true;
  }

  private boolean isPendingRequestQueueUnhealthy(String nodeId) {
    long pendingRequestCount = routeHttpRequestStats.getPendingRequestCount(nodeId);
    aggHostHealthStats.recordPendingRequestCount(nodeId, pendingRequestCount);
    if (!statefulRouterHealthCheckEnabled) {
      return false;
    }
    Long unhealthyStartTime = pendingRequestUnhealthyTimeMap.get(nodeId);
    if (unhealthyStartTime != null) {
      if (pendingRequestCount > routerPendingConnResumeThreshold) {
        return true;
      } else {
        // Check whether the OOR duration has passed or not
        long duration = System.currentTimeMillis() - unhealthyStartTime;
        if (duration < fullPendingQueueServerOORMs) {
          return true;
        }
        if (pendingRequestUnhealthyTimeMap.remove(nodeId) != null) {
          routeHttpRequestStats.recordUnhealthyQueueDuration(nodeId, duration);
          aggHostHealthStats.recordPendingRequestUnhealthyDuration(nodeId, duration);
          aggHostHealthStats.recordUnhealthyHostCountCausedByPendingQueue(pendingRequestUnhealthyTimeMap.size());
        }
        return false;
      }
    }
    if (pendingRequestCount > maxPendingConnectionPerHost) {
      pendingRequestUnhealthyTimeMap.computeIfAbsent(nodeId, k -> System.currentTimeMillis());
      return true;
    }
    return false;
  }
}
