package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class DeferredVersionSwapStats extends AbstractVeniceStats {
  private final Sensor deferredVersionSwapErrorSensor;
  private final Sensor deferredVersionSwapThrowableSensor;
  private final static String DEFERRED_VERSION_SWAP_ERROR = "deferred_version_swap_error";
  private final static String DEFERRED_VERSION_SWAP_THROWABLE = "deferred_version_swap_throwable";

  public DeferredVersionSwapStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "DeferredVersionSwap");
    deferredVersionSwapErrorSensor = registerSensorIfAbsent(DEFERRED_VERSION_SWAP_ERROR, new Count());
    deferredVersionSwapThrowableSensor = registerSensorIfAbsent(DEFERRED_VERSION_SWAP_THROWABLE, new Count());
  }

  public void recordDeferredVersionSwapErrorSensor() {
    deferredVersionSwapErrorSensor.record();
  }

  public void recordDeferreredVersionSwapThrowableSensor() {
    deferredVersionSwapThrowableSensor.record();
  }
}
