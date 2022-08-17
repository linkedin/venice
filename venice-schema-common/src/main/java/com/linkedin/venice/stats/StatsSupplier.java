package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;


public interface StatsSupplier<T extends AbstractVeniceStats> {
  T get(MetricsRepository metricsRepository, String storeName);
}
