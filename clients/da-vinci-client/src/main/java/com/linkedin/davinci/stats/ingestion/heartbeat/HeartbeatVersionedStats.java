package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.stats.AbstractVeniceAggVersionedStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.function.Supplier;


public class HeartbeatVersionedStats extends AbstractVeniceAggVersionedStats<HeartbeatStat, HeartbeatStatReporter> {
  private final Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> leaderMonitors;
  private final Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> followerMonitors;

  public HeartbeatVersionedStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      Supplier<HeartbeatStat> statsInitiator,
      StatsSupplier<HeartbeatStatReporter> reporterSupplier,
      Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> leaderMonitors,
      Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> followerMonitors) {
    super(metricsRepository, metadataRepository, statsInitiator, reporterSupplier, true);
    this.leaderMonitors = leaderMonitors;
    this.followerMonitors = followerMonitors;
  }

  public void recordLeaderLag(String storeName, int version, String region, long heartbeatTs) {
    getStats(storeName, version).recordReadyToServeLeaderLag(region, heartbeatTs);
  }

  public void recordFollowerLag(
      String storeName,
      int version,
      String region,
      long heartbeatTs,
      boolean isReadyToServe) {
    // If the partition is ready to serve, report it's lag to the main lag metric. Otherwise, report it
    // to the catch up metric.
    // The metric which isn't updated is squelched by reporting the currentTime (so as to appear caught up and mute
    // alerts)
    if (isReadyToServe) {
      getStats(storeName, version).recordReadyToServeFollowerLag(region, heartbeatTs);
      getStats(storeName, version).recordCatchingUpFollowerLag(region, System.currentTimeMillis());
    } else {
      getStats(storeName, version).recordReadyToServeFollowerLag(region, System.currentTimeMillis());
      getStats(storeName, version).recordCatchingUpFollowerLag(region, heartbeatTs);
    }
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

  boolean isStoreAssignedToThisNode(String store) {
    if (leaderMonitors == null || followerMonitors == null) {
      // TODO: We have to do this because theres a self call in the constructor
      // of the superclass of this class. We shouldn't have to do this
      return false;
    }
    return leaderMonitors.containsKey(store) || followerMonitors.containsKey(store);
  }
}
