package com.linkedin.venice.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.utils.Utils;
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
      boolean isUnregisterMetricForDeletedStoreEnabled,
      boolean perClusterAggregate) {
    super(clusterName, metricsRepository, statsSupplier, perClusterAggregate);
    this.isUnregisterMetricForDeletedStoreEnabled = isUnregisterMetricForDeletedStoreEnabled;
    registerStoreDataChangedListenerIfRequired(metadataRepository);
  }

  public AbstractVeniceAggStoreStats(
      String clusterName,
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean isUnregisterMetricForDeletedStoreEnabled) {
    super(clusterName, metricsRepository);
    this.isUnregisterMetricForDeletedStoreEnabled = isUnregisterMetricForDeletedStoreEnabled;
    registerStoreDataChangedListenerIfRequired(metadataRepository);
  }

  public T getStoreStats(String storeName) {
    return super.getStoreStats(storeName);
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    // Remove first so a subsequent getStoreStats(storeName) does not return a now-closed instance —
    // computeIfAbsent inside the parent will recreate fresh stats if the store is re-created later.
    T stats = super.storeStats.remove(storeName);
    if (stats == null) {
      return;
    }
    if (isUnregisterMetricForDeletedStoreEnabled) {
      stats.unregisterAllSensors();
    }
    // OTel close is unconditional — independent of the Tehuti unregister flag.
    Utils.closeQuietlyWithErrorLogged(stats);
  }

  private void registerStoreDataChangedListenerIfRequired(ReadOnlyStoreRepository metadataRepository) {
    if (metadataRepository != null) {
      metadataRepository.registerStoreDataChangedListener(this);
    }
  }
}
