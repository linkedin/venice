package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.stats.AbstractVeniceAggVersionedStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;


public class HeartbeatVersionedStats extends AbstractVeniceAggVersionedStats<HeartbeatStat, HeartbeatStatReporter> {
  private Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> leaderMonitors;
  private Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> followerMonitors;

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

  public void recordLeaderLag(String storeName, int version, String region, long lag) {
    getStats(storeName, version).recordLeaderLag(region, lag);
  }

  public void recordFollowerLag(String storeName, int version, String region, long lag) {
    getStats(storeName, version).recordFollowerLag(region, lag);
  }

  @Override
  public synchronized void loadAllStats() {
    Set<String> stores = getReportedStores();
    metadataRepository.getAllStores().forEach(store -> {
      if (stores.contains(store.getName())) {
        addStore(store.getName());
        updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
      }
    });
  }

  @Override
  public void handleStoreCreated(Store store) {
    Set<String> stores = getReportedStores();
    if (stores.contains(store.getName())) {
      addStore(store.getName());
    }
  }

  @Override
  public void handleStoreChanged(Store store) {
    Set<String> stores = getReportedStores();
    if (stores.contains(store.getName())) {
      updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
    }
  }

  Set<String> getReportedStores() {
    if (leaderMonitors == null || followerMonitors == null) {
      // TODO: We have to do this because theres a self call in the constructor
      // of the superclass of this class. We shouldn't have to do this
      return Collections.EMPTY_SET;
    }
    Set<String> monitoredStores = leaderMonitors.keySet();
    monitoredStores.addAll(followerMonitors.keySet());
    return monitoredStores;
  }
}
