package com.linkedin.venice.stats;

import com.sleepycat.je.Environment;
import io.tehuti.metrics.MetricsRepository;


public class AggBdbStorageEngineStats extends AbstractVeniceAggStats<BdbStorageEngineStats> {
  public AggBdbStorageEngineStats(MetricsRepository metricsRepository) {
    super(metricsRepository,
        (metricsRepo, storeName) -> new BdbStorageEngineStats(metricsRepo, storeName));
  }

  /**
   * Since Venice metrics are store-level metric and only emit for the latest version,
   * {@link AggBdbStorageEngineStats} will update bdbEnvironment every time new versions
   * are created.
   */
  public void setBdbEnvironment(String storeName, Environment bdbEnvironment) {
    getStoreStats(storeName).setBdbEnvironment(bdbEnvironment);
  }
}
