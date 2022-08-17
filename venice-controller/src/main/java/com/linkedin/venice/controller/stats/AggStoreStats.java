package com.linkedin.venice.controller.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import io.tehuti.metrics.MetricsRepository;


public class AggStoreStats {
  public AggStoreStats(MetricsRepository metricsRepository, ReadOnlyStoreRepository storeRepository) {
    for (Store store: storeRepository.getAllStores()) {
      new StoreStats(store.getName(), metricsRepository, storeRepository);
    }

    storeRepository.registerStoreDataChangedListener(new StoreDataChangedListener() {
      @Override
      public void handleStoreCreated(Store store) {
        new StoreStats(store.getName(), metricsRepository, storeRepository);
      }

      @Override
      public void handleStoreDeleted(String storeName) {
      }

      @Override
      public void handleStoreChanged(Store store) {
      }
    });
  }
}
