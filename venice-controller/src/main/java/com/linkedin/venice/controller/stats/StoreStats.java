package com.linkedin.venice.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;

import io.tehuti.metrics.MetricsRepository;

import org.apache.log4j.Logger;

public class StoreStats extends AbstractVeniceStats {
  private static final Logger logger = Logger.getLogger(StoreStats.class);

  public StoreStats(String storeName, MetricsRepository metricsRepository, ReadOnlyStoreRepository storeRepository) {
    super(metricsRepository, storeName);

    registerSensor("data_age_ms", new Gauge(() -> {
      try {
        Store store = storeRepository.getStoreOrThrow(storeName);
        long now = System.currentTimeMillis();
        long dataAge = now - store.getVersions().stream().mapToLong(v -> v.getCreatedTime()).min().getAsLong();
        return Math.max(dataAge, 0L);
      } catch (Throwable e) {
        return -1;
      }
    }));
  }
}
