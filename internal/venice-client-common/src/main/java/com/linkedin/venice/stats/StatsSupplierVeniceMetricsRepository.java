package com.linkedin.venice.stats;

/** copy of {@link StatsSupplierMetricsRepository} for {@link VeniceMetricsRepository} */
public interface StatsSupplierVeniceMetricsRepository<T extends AbstractVeniceStats> {
  /**
   * Legacy function, for implementations that do not use total stats in their constructor.
   *
   * @see #get(VeniceMetricsRepository, String, AbstractVeniceStats) which is the only caller.
   */
  T get(VeniceMetricsRepository metricsRepository, String storeName);

  /**
   * This is the function that gets called by {@link AbstractVeniceAggStats}, and concrete classes can
   * optionally implement it in order to be provided with the total stats instance.
   */
  default T get(VeniceMetricsRepository metricsRepository, String storeName, T totalStats) {
    return get(metricsRepository, storeName);
  }
}
