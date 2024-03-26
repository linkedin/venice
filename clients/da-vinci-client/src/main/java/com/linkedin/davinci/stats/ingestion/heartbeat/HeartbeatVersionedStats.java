package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.stats.AbstractVeniceAggVersionedStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.function.Supplier;


public class HeartbeatVersionedStats extends AbstractVeniceAggVersionedStats<HeartbeatStat, HeartbeatStatReporter> {
  private final Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> leaderMonitors;
  private final Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> followerMonitors;

  public HeartbeatVersionedStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      Supplier<HeartbeatStat> statsInitiator,
      StatsSupplier<HeartbeatStatReporter> reporterSupplier,
      Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> leaderMonitors,
      Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> followerMonitors) {
    super(metricsRepository, metadataRepository, statsInitiator, reporterSupplier, true);
    this.leaderMonitors = leaderMonitors;
    this.followerMonitors = followerMonitors;
  }

  public void recordLeaderLag(String storeName, int version, String region, long heartbeatTs) {
    getStats(storeName, version).recordLeaderLag(region, heartbeatTs);
  }

  public void recordFollowerLag(String storeName, int version, String region, long heartbeatTs) {
    getStats(storeName, version).recordFollowerLag(region, heartbeatTs);
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
