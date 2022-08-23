package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;


//TODO: once we've migrated this stats to multi-version. We might want to consider merge it with DIVStats
public class AggStoreIngestionStats extends AbstractVeniceAggStats<StoreIngestionStats> {
  public AggStoreIngestionStats(MetricsRepository metricsRepository, VeniceServerConfig serverConfig) {
    super(metricsRepository, new StoreIngestionStatsSupplier(serverConfig));
  }

  static class StoreIngestionStatsSupplier implements StatsSupplier<StoreIngestionStats> {
    private final VeniceServerConfig serverConfig;

    StoreIngestionStatsSupplier(VeniceServerConfig serverConfig) {
      this.serverConfig = serverConfig;
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
      return new StoreIngestionStats(metricsRepository, serverConfig, storeName, totalStats);
    }
  }
}
