package com.linkedin.venice.stats;

import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.stats.Gauge;
import java.util.concurrent.atomic.LongAdder;


/**
 * This metric class is to optimize for high write throughput, low read throughput measurement use case instead of real-time
 * measurement. The smallest measurement interval is 30 seconds.
 */
public class LongAdderRateGauge extends Gauge {
  private final Time time;
  private final LongAdder adder = new LongAdder();
  private long lastMeasurementTime;
  private double lastMeasuredValue = 0.0D;

  public static final int RATE_GAUGE_CACHE_DURATION_IN_SECONDS = 30;

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
  public void record(double value, long now) {
    this.adder.add((long) value);
  }

  @Override
  public double measure(MetricConfig config, long currentTimeMs) {
    return getRate(currentTimeMs);
  }

  public double getRate() {
    return getRate(time.getMilliseconds());
  }

  /**
   * N.B.: We are not looking for great precision at a very short timescale. The intended use case is for this
   * function to be queried by the metric system ~1/minute.
   */
  private double getRate(long currentTimeMs) {
    double elapsedTimeInSeconds = (double) (currentTimeMs - this.lastMeasurementTime) / Time.MS_PER_SECOND;
    if (elapsedTimeInSeconds < RATE_GAUGE_CACHE_DURATION_IN_SECONDS) {
      return lastMeasuredValue;
    }
    this.lastMeasurementTime = currentTimeMs;
    this.lastMeasuredValue = this.adder.sumThenReset() / elapsedTimeInSeconds;
    return lastMeasuredValue;
  }
}
