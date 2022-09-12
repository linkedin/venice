package com.linkedin.venice.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.StoreDataChangedListener;
import io.tehuti.metrics.MetricsRepository;


/**
 * This class is an aggregate place that keeps stats objects for multiple stores.
 * {@link AbstractVeniceAggStoreStats#getStoreStats(String)} creates a stats object per store. Sensors are registered
 * during stats object construction. Upon store deletion, if unregister metric for deleted store feature is enabled,
 * {@link AbstractVeniceAggStoreStats#handleStoreDeleted(String)} will retrieve the stats object and unregister sensors.
 */
public class AbstractVeniceAggStoreStats<T extends AbstractVeniceStats> extends AbstractVeniceAggStats<T>
    implements StoreDataChangedListener {
  private final boolean isUnregisterMetricForDeletedStoreEnabled;

  public AbstractVeniceAggStoreStats(
      String clusterName,
      MetricsRepository metricsRepository,
      StatsSupplier<T> statsSupplier,
      ReadOnlyStoreRepository metadataRepository,
      boolean isUnregisterMetricForDeletedStoreEnabled) {
    super(clusterName, metricsRepository, statsSupplier);
    this.isUnregisterMetricForDeletedStoreEnabled = isUnregisterMetricForDeletedStoreEnabled;
    registerStoreDataChangedListenerIfRequired(metadataRepository);
  }

  public AbstractVeniceAggStoreStats(
      MetricsRepository metricsRepository,
      StatsSupplier<T> statsSupplier,
      ReadOnlyStoreRepository metadataRepository,
      boolean isUnregisterMetricForDeletedStoreEnabled) {
    super(metricsRepository, statsSupplier);
    this.isUnregisterMetricForDeletedStoreEnabled = isUnregisterMetricForDeletedStoreEnabled;
    registerStoreDataChangedListenerIfRequired(metadataRepository);
  }

  public AbstractVeniceAggStoreStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean isUnregisterMetricForDeletedStoreEnabled) {
    super(metricsRepository);
    this.isUnregisterMetricForDeletedStoreEnabled = isUnregisterMetricForDeletedStoreEnabled;
    registerStoreDataChangedListenerIfRequired(metadataRepository);
  }

  public T getStoreStats(String storeName) {
    return super.getStoreStats(storeName);
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    if (isUnregisterMetricForDeletedStoreEnabled) {
      T stats = super.storeStats.get(storeName);
      if (stats != null) {
        stats.unregisterAllSensors();
      }
    }
  }

  private void registerStoreDataChangedListenerIfRequired(ReadOnlyStoreRepository metadataRepository) {
    if (isUnregisterMetricForDeletedStoreEnabled) {
      metadataRepository.registerStoreDataChangedListener(this);
    }
  }
}
