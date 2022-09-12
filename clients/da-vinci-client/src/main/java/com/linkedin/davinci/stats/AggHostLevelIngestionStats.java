package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.AbstractVeniceAggStoreStats;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;


/**
 * This class provides aggregate initialization support for host level ingestion stats class {@link HostLevelIngestionStats}
 */
public class AggHostLevelIngestionStats extends AbstractVeniceAggStoreStats<HostLevelIngestionStats> {
  public AggHostLevelIngestionStats(
      MetricsRepository metricsRepository,
      VeniceServerConfig serverConfig,
      Map<String, StoreIngestionTask> ingestionTaskMap,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled) {
    super(
        metricsRepository,
        new HostLevelStoreIngestionStatsSupplier(serverConfig, ingestionTaskMap),
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled);
  }

  static class HostLevelStoreIngestionStatsSupplier implements StatsSupplier<HostLevelIngestionStats> {
    private final VeniceServerConfig serverConfig;
    private final Map<String, StoreIngestionTask> ingestionTaskMap;

    HostLevelStoreIngestionStatsSupplier(
        VeniceServerConfig serverConfig,
        Map<String, StoreIngestionTask> ingestionTaskMap) {
      this.serverConfig = serverConfig;
      this.ingestionTaskMap = ingestionTaskMap;
    }

    @Override
    public HostLevelIngestionStats get(MetricsRepository metricsRepository, String storeName) {
      throw new VeniceException("Should not be called.");
    }

    @Override
    public HostLevelIngestionStats get(
        MetricsRepository metricsRepository,
        String storeName,
        HostLevelIngestionStats totalStats) {
      return new HostLevelIngestionStats(metricsRepository, serverConfig, storeName, totalStats, ingestionTaskMap);
    }
  }
}
