package com.linkedin.venice.stats;

import io.tehuti.metrics.Measurable;
import io.tehuti.metrics.stats.AsyncGauge;


/**
 * @deprecated, use {@link Gauge} instead.
 *
 * The reason to deprecate {@link LambdaStat} is that {@link Gauge} is a better name when appending the class name
 * as the suffix of metric name here: {@link AbstractVeniceStats#registerSensor(String, Sensor[], MeasurableStat...)}.
 */
@Deprecated
public class LambdaStat extends AsyncGauge {
  public LambdaStat(Measurable measurable, String metricName) {
    super(measurable, metricName);
  }
}
