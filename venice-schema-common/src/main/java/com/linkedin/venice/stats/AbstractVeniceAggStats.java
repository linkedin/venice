package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public abstract class AbstractVeniceAggStats<T extends AbstractVeniceStats> {
  public static String STORE_NAME_FOR_TOTAL_STAT = "total";
  protected T totalStats;
  protected Map<String, T> storeStats;

  private StatsSupplier<T> statsFactory;
  private final MetricsRepository metricsRepository;

  public AbstractVeniceAggStats(MetricsRepository metricsRepository, StatsSupplier<T> statsSupplier) {
    this.metricsRepository = metricsRepository;
    this.statsFactory = statsSupplier;

    this.totalStats = statsSupplier.get(metricsRepository, STORE_NAME_FOR_TOTAL_STAT);
    storeStats = new ConcurrentHashMap<>();
  }

  public AbstractVeniceAggStats(MetricsRepository metricsRepository) {
    this.metricsRepository = metricsRepository;
    storeStats = new ConcurrentHashMap<>();
  }

  public void setStatsSupplier(StatsSupplier<T> statsSupplier) {
    this.statsFactory = statsSupplier;
    this.totalStats = statsSupplier.get(metricsRepository, STORE_NAME_FOR_TOTAL_STAT);
  }

  public AbstractVeniceAggStats(String clusterName, MetricsRepository metricsRepository, StatsSupplier<T> statsSupplier) {
    this.metricsRepository = metricsRepository;
    this.statsFactory = statsSupplier;

    this.totalStats = statsSupplier.get(metricsRepository, STORE_NAME_FOR_TOTAL_STAT + "." + clusterName);
    storeStats = new ConcurrentHashMap<>();
  }

  protected T getStoreStats(String storeName) {
    return storeStats.computeIfAbsent(storeName,
        k -> statsFactory.get(metricsRepository, storeName));
  }
}
