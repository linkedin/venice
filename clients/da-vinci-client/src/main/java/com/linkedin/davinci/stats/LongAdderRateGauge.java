package com.linkedin.davinci.stats;

import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.stats.Gauge;
import java.util.concurrent.atomic.LongAdder;


public class LongAdderRateGauge extends Gauge {
  private final LongAdder adder = new LongAdder();
  private long lastMeasurementTime = System.currentTimeMillis();

  public void record() {
    this.adder.increment();
  }

  public void record(long amount) {
    this.adder.add(amount);
  }

  @Override
  public double measure(MetricConfig config, long now) {
    double elapsedTimeInSeconds = (double) (now - this.lastMeasurementTime) / Time.MS_PER_SECOND;
    this.lastMeasurementTime = now;
    return (double) this.adder.sumThenReset() / elapsedTimeInSeconds;
  }
}
