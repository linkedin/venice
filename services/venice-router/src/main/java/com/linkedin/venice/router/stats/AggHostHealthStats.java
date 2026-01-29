package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.StatsUtils;
import io.tehuti.metrics.MetricsRepository;


public class AggHostHealthStats extends AbstractVeniceAggStats<HostHealthStats> {
  public AggHostHealthStats(String clusterName, MetricsRepository metricsRepository) {
    super(
        clusterName,
        metricsRepository,
        (repo, hostName, cluster) -> new HostHealthStats(repo, StatsUtils.convertHostnameToMetricName(hostName)),
        false);
  }

  private HostHealthStats getHostStats(String hostName) {
    return getStoreStats(hostName);
  }

  public void recordUnhealthyHostOfflineInstance(String hostName) {
    totalStats.recordUnhealthyHostOfflineInstance();
    getHostStats(hostName).recordUnhealthyHostOfflineInstance();
  }

  public void recordUnhealthyHostTooManyPendingRequest(String hostName) {
    totalStats.recordUnhealthyHostTooManyPendingRequest();
    getHostStats(hostName).recordUnhealthyHostTooManyPendingRequest();
  }

  public void recordUnhealthyHostHeartBeatFailure(String hostName) {
    totalStats.recordUnhealthyHostHeartBeatFailure();
    getHostStats(hostName).recordUnhealthyHostHeartBeatFailure();
  }

  public void recordLeakedPendingRequestCount(String hostName) {
    totalStats.recordLeakedPendingRequestCount();
    getHostStats(hostName).recordLeakedPendingRequestCount();
  }

  public void recordPendingRequestUnhealthyDuration(String hostName, double duration) {
    totalStats.recordUnhealthyPendingQueueDuration(duration);
    getHostStats(hostName).recordUnhealthyPendingQueueDuration(duration);
  }

  public void recordUnhealthyHostCountCausedByPendingQueue(int count) {
    totalStats.recordUnhealthyHostCountCausedByPendingQueue(count);
  }

  public void recordUnhealthyHostCountCausedByRouterHeartBeat(int count) {
    totalStats.recordUnhealthyHostCountCausedByRouterHeartBeat(count);
  }

  public void recordUnhealthyHostDelayJoin(String hostName) {
    totalStats.recordUnhealthyHostDelayJoin();
    getHostStats(hostName).recordUnhealthyHostDelayJoin();
  }
}
