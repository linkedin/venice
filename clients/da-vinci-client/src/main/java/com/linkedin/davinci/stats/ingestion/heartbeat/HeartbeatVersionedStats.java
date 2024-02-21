package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.stats.AbstractVeniceAggVersionedStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import java.util.function.Supplier;


public class HeartbeatVersionedStats extends AbstractVeniceAggVersionedStats<HeartbeatStat, HeartbeatStatReporter> {
  public HeartbeatVersionedStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      Supplier<HeartbeatStat> statsInitiator,
      StatsSupplier<HeartbeatStatReporter> reporterSupplier) {
    super(metricsRepository, metadataRepository, statsInitiator, reporterSupplier, true);
  }

  public void recordLeaderLag(String storeName, int version, String region, Long lag) {
    getStats(storeName, version).recordLeaderLag(region, lag);
  }

  public void recordFollowerLag(String storeName, int version, String region, Long lag) {
    getStats(storeName, version).recordFollowerLag(region, lag);
  }
}
