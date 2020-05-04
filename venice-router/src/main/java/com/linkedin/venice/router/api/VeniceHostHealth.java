package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.log4j.Logger;


public class VeniceHostHealth implements HostHealthMonitor<Instance> {

  private static final Logger logger = Logger.getLogger(VeniceHostHealth.class);
  private final int maxPendingConnectionPerHost;
  private final int routerPendingConnResumeThreshold;
  private final boolean statefulRouterHealthCheckEnabled;
  private final long fullPendingQueueServerOORMs;
  private Set<String> slowPartitionHosts = new ConcurrentSkipListSet<>();
  protected Set<String> unhealthyHosts = new ConcurrentSkipListSet<>();
  private Map<String, Long> pendingRequestUnhealthyTimeMap = new VeniceConcurrentHashMap<>();


  private final LiveInstanceMonitor liveInstanceMonitor;
  private final StorageNodeClient storageNodeClient;
  private RouteHttpRequestStats routeHttpRequestStats;
  private final AggHostHealthStats aggHostHealthStats;

  public VeniceHostHealth(LiveInstanceMonitor liveInstanceMonitor, StorageNodeClient storageNodeClient,
      RouteHttpRequestStats routeHttpRequestStats, boolean statefulRouterHealthCheckEnabled,
      int maxPendingConnectionPerHost, int routerPendingConnResumeThreshold,
      long fullPendingQueueServerOORMs, AggHostHealthStats aggHostHealthStats) {
    this.routeHttpRequestStats = routeHttpRequestStats;
    this.statefulRouterHealthCheckEnabled = statefulRouterHealthCheckEnabled;
    this.maxPendingConnectionPerHost = maxPendingConnectionPerHost;
    this.routerPendingConnResumeThreshold = routerPendingConnResumeThreshold;
    this.fullPendingQueueServerOORMs = fullPendingQueueServerOORMs;
    this.liveInstanceMonitor = liveInstanceMonitor;
    this.storageNodeClient = storageNodeClient;
    this.aggHostHealthStats = aggHostHealthStats;
  }

  /**
   * Indicate that a partition on a host has fallen behind in consumption and should not be used
   * for queries for that partition.
   *
   * @param hostName
   * @param partitionName
   */
  public void setPartitionAsSlow(Instance hostName, String partitionName){
    String identifier = hostPartitionString(hostName, partitionName);
    slowPartitionHosts.add(identifier);
    logger.info(identifier + " is slow, marking as unhealthy until it passes the next health check.");
  }

  /**
   * Mark that something is wrong with an entire host and it should not be used for queries.
   *
   * @param hostName
   */
  public void setHostAsUnhealthy(Instance hostName){
    String identifier = hostName.getUrl();
    unhealthyHosts.add(identifier);
    logger.info("Marking " + identifier + " as unhealthy until it passes the next health check.");
  }

  /**
   * If the host is marked as unhealthy before, remove it from the unhealthy host set and log this
   * status change.
   *
   * @param hostname
   */
  public void setHostAsHealthy(Instance hostname) {
    String identifier = hostname.getUrl();
    if (unhealthyHosts.contains(identifier)) {
      unhealthyHosts.remove(identifier);
      logger.info("Marking " + identifier + " back to healthy host");
    }
  }

  @Override
  public boolean isHostHealthy(Instance instance, String partitionName) {
    String nodeId = instance.getNodeId();
    if (!liveInstanceMonitor.isInstanceAlive(instance)) {
      aggHostHealthStats.recordUnhealthyHostOfflineInstance(nodeId);
      return false;
    }
    if (!storageNodeClient.isInstanceReadyToServe(nodeId)) {
      aggHostHealthStats.recordUnhealthyHostDelayJoin(nodeId);
      return false;
    }
    if (slowPartitionHosts.contains(hostPartitionString(instance, partitionName))) {
      aggHostHealthStats.recordUnhealthyHostSlowPartition(nodeId);
      return false;
    }
    if (isPendingRequestQueueUnhealthy(instance.getNodeId())) {
      aggHostHealthStats.recordUnhealthyHostTooManyPendingRequest(nodeId);
      // Record the unhealthy node count because of pending queue check
      aggHostHealthStats.recordUnhealthyHostCountCausedByPendingQueue(pendingRequestUnhealthyTimeMap.size());
      return false;
    }
    if (unhealthyHosts.contains(instance.getUrl())) {
      aggHostHealthStats.recordUnhealthyHostHeartBeatFailure(nodeId);
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

  private static String hostPartitionString(Instance host, String partition){
    return host.getHost() + ":" + host.getPort() + "_" + partition;
  }
}
