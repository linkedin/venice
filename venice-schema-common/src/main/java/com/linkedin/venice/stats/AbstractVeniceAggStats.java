package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractVeniceAggStats<T extends AbstractVeniceStats> {
  protected T totalStats;
  protected Map<String, T> storeStats;

  private StatsSupplier<T> statsFactory;
  private final MetricsRepository metricsRepository;

  public AbstractVeniceAggStats(MetricsRepository metricsRepository, StatsSupplier<T> statsSupplier) {
    this.metricsRepository = metricsRepository;
    this.statsFactory = statsSupplier;

    this.totalStats = statsSupplier.get(metricsRepository,"total");
    storeStats = new ConcurrentHashMap<>();
  }

  protected T getStoreStats(String storeName) {
    return storeStats.computeIfAbsent(storeName,
        k -> statsFactory.get(metricsRepository, storeName));
  }
}
