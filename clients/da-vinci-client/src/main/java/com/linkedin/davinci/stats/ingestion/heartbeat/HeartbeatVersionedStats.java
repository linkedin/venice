package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.stats.AbstractVeniceAggVersionedStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import java.util.Set;
import java.util.function.Supplier;


public class HeartbeatVersionedStats extends AbstractVeniceAggVersionedStats<HeartbeatStat, HeartbeatStatReporter> {
  private HeartbeatMonitoringService monitoringService;

  public HeartbeatVersionedStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      Supplier<HeartbeatStat> statsInitiator,
      StatsSupplier<HeartbeatStatReporter> reporterSupplier,
      HeartbeatMonitoringService monitoringService) {
    super(metricsRepository, metadataRepository, statsInitiator, reporterSupplier, true);
    this.monitoringService = monitoringService;
  }

  public void recordLeaderLag(String storeName, int version, String region, long lag) {
    getStats(storeName, version).recordLeaderLag(region, lag);
  }

  public void recordFollowerLag(String storeName, int version, String region, long lag) {
    getStats(storeName, version).recordFollowerLag(region, lag);
  }

  @Override
  public synchronized void loadAllStats() {
    Set<String> stores = monitoringService.getReportedStores();
    metadataRepository.getAllStores().forEach(store -> {
      if (stores.contains(store.getName())) {
        addStore(store.getName());
        updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
      }
    });
  }

  @Override
  public void handleStoreCreated(Store store) {
    Set<String> stores = monitoringService.getReportedStores();
    if (stores.contains(store.getName())) {
      addStore(store.getName());
    }
  }

  @Override
  public void handleStoreChanged(Store store) {
    Set<String> stores = monitoringService.getReportedStores();
    if (stores.contains(store)) {
      updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
    }
  }
}
