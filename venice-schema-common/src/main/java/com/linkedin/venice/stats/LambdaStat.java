package com.linkedin.venice.stats;

import io.tehuti.metrics.Measurable;


/**
 * @deprecated, use {@link Gauge} instead.
 *
 * The reason to deprecate {@link LambdaStat} is that {@link Gauge} is a better name when appending the class name
 * as the suffix of metric name here: {@link AbstractVeniceStats#registerSensor}.
 */
@Deprecated
public class LambdaStat extends Gauge {
  public LambdaStat(Measurable measurable) {
    super(measurable);
  }

  public LambdaStat(SimpleMeasurable measurable) {
    super(measurable);
  }
}
