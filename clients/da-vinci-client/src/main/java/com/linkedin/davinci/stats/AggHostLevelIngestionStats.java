package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.AbstractVeniceAggStoreStats;
import com.linkedin.venice.stats.StatsSupplier;
import com.linkedin.venice.utils.Time;
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
      boolean unregisterMetricForDeletedStoreEnabled,
      Time time) {
    super(
        serverConfig.getClusterName(),
        metricsRepository,
        new HostLevelStoreIngestionStatsSupplier(serverConfig, ingestionTaskMap, time),
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled,
        false);
  }

  static class HostLevelStoreIngestionStatsSupplier implements StatsSupplier<HostLevelIngestionStats> {
    private final VeniceServerConfig serverConfig;
    private final Map<String, StoreIngestionTask> ingestionTaskMap;
    private final Time time;

    HostLevelStoreIngestionStatsSupplier(
        VeniceServerConfig serverConfig,
        Map<String, StoreIngestionTask> ingestionTaskMap,
        Time time) {
      this.serverConfig = serverConfig;
      this.ingestionTaskMap = ingestionTaskMap;
      this.time = time;
    }

    @Override
    public HostLevelIngestionStats get(MetricsRepository metricsRepository, String storeName, String clusterName) {
      throw new VeniceException("Should not be called.");
    }

    @Override
    public HostLevelIngestionStats get(
        MetricsRepository metricsRepository,
        String storeName,
        String clusterName,
        HostLevelIngestionStats totalStats) {
      return new HostLevelIngestionStats(
          metricsRepository,
          serverConfig,
          storeName,
          totalStats,
          ingestionTaskMap,
          time);
    }
  }
}
