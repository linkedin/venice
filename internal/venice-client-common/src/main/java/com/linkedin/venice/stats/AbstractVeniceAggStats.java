package com.linkedin.venice.stats;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;


public abstract class AbstractVeniceAggStats<T extends AbstractVeniceStats> {
  public final static String STORE_NAME_FOR_TOTAL_STAT = "total";
  protected T totalStats;
  protected final Map<String, T> storeStats = new VeniceConcurrentHashMap<>();

  private StatsSupplier<T> statsFactory;

  private final MetricsRepository metricsRepository;
  private String clusterName = null;

  private AbstractVeniceAggStats(MetricsRepository metricsRepository, StatsSupplier<T> statsSupplier, T totalStats) {
    this.metricsRepository = metricsRepository;
    this.statsFactory = statsSupplier;
    this.totalStats = totalStats;
  }

  private AbstractVeniceAggStats(
      VeniceMetricsRepository metricsRepository,
      StatsSupplier<T> statsSupplier,
      String clusterName,
      T totalStats) {
    this.metricsRepository = metricsRepository;
    this.statsFactory = statsSupplier;
    this.clusterName = clusterName;
    this.totalStats = totalStats;
  }

  public AbstractVeniceAggStats(MetricsRepository metricsRepository, StatsSupplier<T> statsSupplier) {
    this(metricsRepository, statsSupplier, statsSupplier.get(metricsRepository, STORE_NAME_FOR_TOTAL_STAT, null, null));
  }

  public AbstractVeniceAggStats(
      VeniceMetricsRepository metricsRepository,
      StatsSupplier<T> statsSupplier,
      String clusterName) {
    this(
        metricsRepository,
        statsSupplier,
        clusterName,
        statsSupplier.get(metricsRepository, STORE_NAME_FOR_TOTAL_STAT, clusterName, null));
  }

  public AbstractVeniceAggStats(MetricsRepository metricsRepository, String clusterName) {
    this.metricsRepository = metricsRepository;
    this.clusterName = clusterName;
  }

  public void setStatsSupplier(StatsSupplier<T> statsSupplier) {
    this.statsFactory = statsSupplier;
    this.totalStats = statsSupplier.get(metricsRepository, STORE_NAME_FOR_TOTAL_STAT, clusterName, null);
  }

  public AbstractVeniceAggStats(
      MetricsRepository metricsRepository,
      StatsSupplier<T> statsSupplier,
      String clusterName) {
    this(
        metricsRepository,
        statsSupplier,
        statsSupplier.get(metricsRepository, STORE_NAME_FOR_TOTAL_STAT + "." + clusterName, clusterName, null));
    this.clusterName = clusterName;
  }

  public T getStoreStats(String storeName) {
    return storeStats
        .computeIfAbsent(storeName, k -> statsFactory.get(metricsRepository, storeName, clusterName, totalStats));
  }

  public T getNullableStoreStats(String storeName) {
    return storeStats.get(storeName);
  }

  public T getTotalStats() {
    return totalStats;
  }
}
