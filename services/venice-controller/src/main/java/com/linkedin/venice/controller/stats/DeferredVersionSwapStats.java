package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;


public class DeferredVersionSwapStats extends AbstractVeniceStats {
  private final Sensor deferredVersionSwapErrorSensor;
  private final String DEFERRED_VERSION_SWAP_ERROR = "deferred_version_swap_error";

  public DeferredVersionSwapStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "DeferredVersionSwap");
    deferredVersionSwapErrorSensor = registerSensorIfAbsent(DEFERRED_VERSION_SWAP_ERROR, new Rate());
  }

  public void recordDeferredVersionSwapErrorSensor() {
    deferredVersionSwapErrorSensor.record();
  }
}
