package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Gauge;


public class DeferredVersionSwapStats extends AbstractVeniceStats {
  private final Sensor deferredVersionSwapErrorSensor;
  private final Sensor deferredVersionSwapThrowableSensor;
  private final Sensor deferredVersionSwapFailedRollForwardSensor;
  private final Sensor deferredVersionSwapStalledVersionSwapSensor;
  private final Sensor deferredVersionSwapParentChildStatusMismatchSensor;
  private final Sensor deferredVersionSwapChildStatusMismatchSensor;
  private final static String DEFERRED_VERSION_SWAP_ERROR = "deferred_version_swap_error";
  private final static String DEFERRED_VERSION_SWAP_THROWABLE = "deferred_version_swap_throwable";
  private final static String DEFERRED_VERSION_SWAP_FAILED_ROLL_FORWARD = "deferred_version_swap_failed_roll_forward";
  private static final String DEFERRED_VERSION_SWAP_STALLED_VERSION_SWAP = "deferred_version_swap_stalled_version_swap";
  private static final String DEFERRED_VERSION_SWAP_PARENT_CHILD_STATUS_MISMATCH_SENSOR =
      "deferred_version_swap_parent_child_status_mismatch";
  private static final String DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH_SENSOR =
      "deferred_version_swap_child_status_mismatch";

  public DeferredVersionSwapStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "DeferredVersionSwap");
    deferredVersionSwapErrorSensor = registerSensorIfAbsent(DEFERRED_VERSION_SWAP_ERROR, new Count());
    deferredVersionSwapThrowableSensor = registerSensorIfAbsent(DEFERRED_VERSION_SWAP_THROWABLE, new Count());
    deferredVersionSwapFailedRollForwardSensor =
        registerSensorIfAbsent(DEFERRED_VERSION_SWAP_FAILED_ROLL_FORWARD, new Count());
    deferredVersionSwapStalledVersionSwapSensor =
        registerSensorIfAbsent(DEFERRED_VERSION_SWAP_STALLED_VERSION_SWAP, new Gauge());
    deferredVersionSwapParentChildStatusMismatchSensor =
        registerSensorIfAbsent(DEFERRED_VERSION_SWAP_PARENT_CHILD_STATUS_MISMATCH_SENSOR, new Count());
    deferredVersionSwapChildStatusMismatchSensor =
        registerSensorIfAbsent(DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH_SENSOR, new Count());
  }

  public void recordDeferredVersionSwapErrorSensor() {
    deferredVersionSwapErrorSensor.record();
  }

  public void recordDeferredVersionSwapThrowableSensor() {
    deferredVersionSwapThrowableSensor.record();
  }

  public void recordDeferredVersionSwapFailedRollForwardSensor() {
    deferredVersionSwapFailedRollForwardSensor.record();
  }

  public void recordDeferredVersionSwapStalledVersionSwapSensor(double value) {
    deferredVersionSwapStalledVersionSwapSensor.record(value);
  }

  public void recordDeferredVersionSwapParentChildStatusMismatchSensor() {
    deferredVersionSwapParentChildStatusMismatchSensor.record();
  }

  public void recordDeferredVersionSwapChildStatusMismatchSensor() {
    deferredVersionSwapChildStatusMismatchSensor.record();
  }
}
