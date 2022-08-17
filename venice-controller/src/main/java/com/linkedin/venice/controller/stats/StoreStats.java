package com.linkedin.venice.controller.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;


public class StoreStats extends AbstractVeniceStats {
  public StoreStats(String storeName, MetricsRepository metricsRepository, ReadOnlyStoreRepository storeRepository) {
    super(metricsRepository, storeName);

    registerSensorIfAbsent("data_age_ms", new Gauge(() -> {
      try {
        Store store = storeRepository.getStoreOrThrow(storeName);
        long now = System.currentTimeMillis();
        long dataAge = now - store.getVersions().stream().mapToLong(Version::getCreatedTime).min().getAsLong();
        return Math.max(dataAge, 0L);
      } catch (Throwable e) {
        return -1;
      }
    }));
  }
}
