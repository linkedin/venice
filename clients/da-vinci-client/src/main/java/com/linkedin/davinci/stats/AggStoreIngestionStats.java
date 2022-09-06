package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;


//TODO: once we've migrated this stats to multi-version. We might want to consider merge it with DIVStats
public class AggStoreIngestionStats extends AbstractVeniceAggStats<StoreIngestionStats> {
  public AggStoreIngestionStats(
      MetricsRepository metricsRepository,
      VeniceServerConfig serverConfig,
      Map<String, StoreIngestionTask> ingestionTaskMap) {
    super(metricsRepository, new StoreIngestionStatsSupplier(serverConfig, ingestionTaskMap));
  }

  static class StoreIngestionStatsSupplier implements StatsSupplier<StoreIngestionStats> {
    private final VeniceServerConfig serverConfig;
    private final Map<String, StoreIngestionTask> ingestionTaskMap;

    StoreIngestionStatsSupplier(VeniceServerConfig serverConfig, Map<String, StoreIngestionTask> ingestionTaskMap) {
      this.serverConfig = serverConfig;
      this.ingestionTaskMap = ingestionTaskMap;
    }

    @Override
    public StoreIngestionStats get(MetricsRepository metricsRepository, String storeName) {
      throw new VeniceException("Should not be called.");
    }

    @Override
    public StoreIngestionStats get(
        MetricsRepository metricsRepository,
        String storeName,
        StoreIngestionStats totalStats) {
      return new StoreIngestionStats(metricsRepository, serverConfig, storeName, totalStats, ingestionTaskMap);
    }
  }
}
