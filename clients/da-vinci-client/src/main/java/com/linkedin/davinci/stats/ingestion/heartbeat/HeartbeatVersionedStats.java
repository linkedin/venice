package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.stats.AbstractVeniceAggVersionedStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.stats.StatsSupplier;
import com.linkedin.venice.stats.dimensions.ReplicaState;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.function.Supplier;


public class HeartbeatVersionedStats extends AbstractVeniceAggVersionedStats<HeartbeatStat, HeartbeatStatReporter> {
  private final Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> leaderMonitors;
  private final Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> followerMonitors;

  // OpenTelemetry metrics per store
  private final Map<String, HeartbeatOtelStats> otelStatsMap;
  private final String clusterName;

  public HeartbeatVersionedStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      Supplier<HeartbeatStat> statsInitiator,
      StatsSupplier<HeartbeatStatReporter> reporterSupplier,
      Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> leaderMonitors,
      Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> followerMonitors,
      String clusterName) {
    super(metricsRepository, metadataRepository, statsInitiator, reporterSupplier, true);
    this.leaderMonitors = leaderMonitors;
    this.followerMonitors = followerMonitors;
    this.clusterName = clusterName;
    this.otelStatsMap = new VeniceConcurrentHashMap<>();
  }

  public void recordLeaderLag(String storeName, int version, String region, long heartbeatTs) {
    // Calculate current time and delay once for both Tehuti and OTel metrics
    long currentTime = System.currentTimeMillis();
    long delay = currentTime - heartbeatTs;

    // Tehuti metrics
    getStats(storeName, version).recordReadyToServeLeaderLag(region, delay, currentTime);

    // OTel metrics
    getOrCreateOtelStats(storeName).recordHeartbeatDelayOtelMetrics(
        version,
        region,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE, // Leaders are always ready to serve
        delay);
  }

  public void recordFollowerLag(
      String storeName,
      int version,
      String region,
      long heartbeatTs,
      boolean isReadyToServe) {
    // Calculate current time and delay once for all metrics
    long currentTime = System.currentTimeMillis();
    long delay = currentTime - heartbeatTs;

    // If the partition is ready to serve, report it's lag to the main lag metric. Otherwise, report it
    // to the catch up metric.
    // The metric which isn't updated is squelched by reporting delay=0 (to appear caught up and mute alerts)
    long readyToServeDelay = isReadyToServe ? delay : 0;
    long catchingUpDelay = isReadyToServe ? 0 : delay;

    // Record to both Tehuti sensors (one gets actual delay, other gets 0 for squelching)
    getStats(storeName, version).recordReadyToServeFollowerLag(region, readyToServeDelay, currentTime);
    getStats(storeName, version).recordCatchingUpFollowerLag(region, catchingUpDelay, currentTime);

    // Record to both OTel dimensions (one gets actual delay, other gets 0 for squelching)
    HeartbeatOtelStats otelStats = getOrCreateOtelStats(storeName);
    otelStats.recordHeartbeatDelayOtelMetrics(
        version,
        region,
        ReplicaType.FOLLOWER,
        ReplicaState.READY_TO_SERVE,
        readyToServeDelay);
    otelStats.recordHeartbeatDelayOtelMetrics(
        version,
        region,
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        catchingUpDelay);
  }

  @Override
  public synchronized void loadAllStats() {
    // No-op
  }

  @Override
  public void handleStoreCreated(Store store) {
    // No-op
  }

  @Override
  public void handleStoreChanged(Store store) {
    if (isStoreAssignedToThisNode(store.getName())) {
      updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
    }
  }

  @Override
  protected void onVersionInfoUpdated(String storeName, int currentVersion, int futureVersion) {
    // Update OTel stats version cache when versions change
    HeartbeatOtelStats otelStats = otelStatsMap.get(storeName);
    if (otelStats != null) {
      otelStats.updateVersionInfo(currentVersion, futureVersion);
    }
  }

  boolean isStoreAssignedToThisNode(String store) {
    if (leaderMonitors == null || followerMonitors == null) {
      // TODO: We have to do this because theres a self call in the constructor
      // of the superclass of this class. We shouldn't have to do this
      return false;
    }
    return leaderMonitors.containsKey(store) || followerMonitors.containsKey(store);
  }

  /**
   * Gets or creates OTel stats for a store.
   * Version info will be initialized via onVersionInfoUpdated() callback.
   */
  private HeartbeatOtelStats getOrCreateOtelStats(String storeName) {
    return otelStatsMap.computeIfAbsent(storeName, key -> {
      HeartbeatOtelStats stats = new HeartbeatOtelStats(getMetricsRepository(), storeName, clusterName);
      // Initialize version cache with current values
      stats.updateVersionInfo(getCurrentVersion(storeName), getFutureVersion(storeName));
      return stats;
    });
  }
}
