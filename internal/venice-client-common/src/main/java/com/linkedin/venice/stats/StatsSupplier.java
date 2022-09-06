package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;


public interface StatsSupplier<T extends AbstractVeniceStats> {
  /**
   * Legacy function, for implementations that do not use total stats in their constructor.
   *
   * @see #get(MetricsRepository, String, AbstractVeniceStats) which is the only caller.
   */
  T get(MetricsRepository metricsRepository, String storeName);

  /**
   * This is the function that gets called by {@link AbstractVeniceAggStats}, and concrete classes can
   * optionally implement it in order to be provided with the total stats instance.
   */
  default T get(MetricsRepository metricsRepository, String storeName, T totalStats) {
    return get(metricsRepository, storeName);
  }
}
