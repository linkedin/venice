package com.linkedin.venice.stats;

import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.stats.Gauge;
import java.util.concurrent.atomic.LongAdder;


public class LongAdderRateGauge extends Gauge {
  private final Time time;
  private final LongAdder adder = new LongAdder();
  private long lastMeasurementTime;

  public LongAdderRateGauge() {
    this(new SystemTime());
  }

  public LongAdderRateGauge(Time time) {
    this.time = time;
    this.lastMeasurementTime = time.getMilliseconds();
  }

  public void record() {
    this.adder.increment();
  }

  public void record(long amount) {
    this.adder.add(amount);
  }

  @Override
  public double measure(MetricConfig config, long currentTimeMs) {
    return getRate(currentTimeMs);
  }

  public double getRate() {
    return getRate(time.getMilliseconds());
  }

  private double getRate(long currentTimeMs) {
    /**
     * N.B.: We are not looking for great precision at a very short timescale. The intended use case is for this
     * function to be queried by the metric system ~1/minute. So the goal here is just to avoid a division by zero.
     */
    double elapsedTimeInSeconds =
        Math.max((double) (currentTimeMs - this.lastMeasurementTime) / Time.MS_PER_SECOND, 0.001);
    this.lastMeasurementTime = currentTimeMs;
    return (double) this.adder.sumThenReset() / elapsedTimeInSeconds;
  }
}
