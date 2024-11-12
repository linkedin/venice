package com.linkedin.venice.stats;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;


public abstract class AbstractVeniceAggStats<T extends AbstractVeniceStats> {
  public final static String STORE_NAME_FOR_TOTAL_STAT = "total";
  protected T totalStats;
  protected final Map<String, T> storeStats = new VeniceConcurrentHashMap<>();

  private StatsSupplierMetricsRepository<T> statsFactoryMetricsRepository;
  private StatsSupplierVeniceMetricsRepository<T> statsFactoryVeniceMetricsRepository;

  private final MetricsRepository metricsRepository;
  private String clusterName = null;

  private AbstractVeniceAggStats(
      MetricsRepository metricsRepository,
      StatsSupplierMetricsRepository<T> statsSupplier,
      T totalStats) {
    this.metricsRepository = metricsRepository;
    this.statsFactoryMetricsRepository = statsSupplier;
    this.totalStats = totalStats;
  }

  private AbstractVeniceAggStats(
      VeniceMetricsRepository metricsRepository,
      StatsSupplierVeniceMetricsRepository<T> statsSupplier,
      String clusterName,
      T totalStats) {
    this.metricsRepository = metricsRepository;
    this.statsFactoryVeniceMetricsRepository = statsSupplier;
    this.clusterName = clusterName;
    this.totalStats = totalStats;
  }

  public AbstractVeniceAggStats(MetricsRepository metricsRepository, StatsSupplierMetricsRepository<T> statsSupplier) {
    this(metricsRepository, statsSupplier, statsSupplier.get(metricsRepository, STORE_NAME_FOR_TOTAL_STAT, null));
  }

  public AbstractVeniceAggStats(
      StatsSupplierVeniceMetricsRepository<T> statsSupplier,
      VeniceMetricsRepository metricsRepository,
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

  public void setStatsSupplier(StatsSupplierVeniceMetricsRepository<T> statsSupplier) {
    this.statsFactoryVeniceMetricsRepository = statsSupplier;
    if (metricsRepository instanceof VeniceMetricsRepository) {
      this.totalStats =
          statsSupplier.get((VeniceMetricsRepository) metricsRepository, STORE_NAME_FOR_TOTAL_STAT, clusterName, null);
    }
  }

  public AbstractVeniceAggStats(
      String clusterName,
      MetricsRepository metricsRepository,
      StatsSupplierMetricsRepository<T> statsSupplier) {
    this(
        metricsRepository,
        statsSupplier,
        statsSupplier.get(metricsRepository, STORE_NAME_FOR_TOTAL_STAT + "." + clusterName, null));
    this.clusterName = clusterName;
  }

  public T getStoreStats(String storeName) {
    if (metricsRepository instanceof VeniceMetricsRepository) {
      return storeStats.computeIfAbsent(
          storeName,
          k -> statsFactoryVeniceMetricsRepository
              .get((VeniceMetricsRepository) metricsRepository, storeName, clusterName, totalStats));
    } else {
      return storeStats
          .computeIfAbsent(storeName, k -> statsFactoryMetricsRepository.get(metricsRepository, storeName, totalStats));
    }
  }

  public T getNullableStoreStats(String storeName) {
    return storeStats.get(storeName);
  }

  public T getTotalStats() {
    return totalStats;
  }
}
