package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class D2Stats extends AbstractVeniceStats {
  private static final String METRIC_PREFIX = "d2_store_discovery";
  private final Sensor storeDiscoverySuccessCount;
  private final Sensor storeDiscoveryFailureCount;

  public D2Stats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, METRIC_PREFIX);
    storeDiscoverySuccessCount = registerSensor(storeName + "-success_request_count", new Count());
    storeDiscoveryFailureCount = registerSensor(storeName + "-failure_request_count", new Count());
  }

  public void recordStoreDiscoverySuccess() {
    storeDiscoverySuccessCount.record();
  }

  public void recordStoreDiscoveryFailure() {
    storeDiscoveryFailureCount.record();
  }

}
