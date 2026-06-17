package com.linkedin.venice.reliability;

import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.SampledCount;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The LoadController is used to control the load on a server by tracking the request and accept rates, and it
 * borrows the idea from here:
 * https://sre.google/sre-book/handling-overload/
 *
 * High-level idea:
 * 1. The load controller will track every request.
 * 2. For the request, which meets the expectation, it will mark it as accepted.
 * 3. It uses the following formula to calculate the rejection ratio:
 *    max(0, (requestRate - acceptMultiplier * acceptRate) / (requestRate + 1))
 *
 * Implementation:
 * 1. Requests and accepts are tracked over a sliding window (Tehuti path) or since the last
 *    measurement (independent-counter path). See {@link Builder#setUseIndependentCounter}.
 * 2. To reduce the overhead of calculating the rejection ratio, we only calculate it every
 *    {@link #rejectionRatioUpdateIntervalInSec}.
 * 3. This class also limits the maximum rejection ratio to {@link #maxRejectionRatio} to avoid
 *    rejecting every request, otherwise, the backend won't be able to recover automatically.
 * 4. We can tune {@link #acceptMultiplier} to control the rejection ratio.
 *
 * <p><b>Backing counter:</b> two implementations coexist and are selected at construction via
 * {@link Builder#setUseIndependentCounter(boolean)} (default {@code false}):
 * <ul>
 *   <li>{@code false} — legacy Tehuti {@link SampledCount} in a local {@link MetricsRepository}
 *       (the {@code MetricsRepository} is not exported; it serves purely as windowed-count state).</li>
 *   <li>{@code true} — two {@link LongAdder}s reset on each ratio update, so the load-shedding
 *       decision remains functional even when the Tehuti dependency is removed. The effective
 *       window is {@link #rejectionRatioUpdateIntervalInSec}; {@code windowSizeInSec} is
 *       ignored for this path.</li>
 * </ul>
 * Both paths produce equivalent rejection ratios for the same input sequence; {@code true} removes
 * the coupling to Tehuti.
 */
public class LoadController {
  private final static Logger LOGGER = LogManager.getLogger(LoadController.class);
  private final int rejectionRatioUpdateIntervalInSec;
  private final double maxRejectionRatio;
  private final Time time;
  private final double acceptMultiplier;

  private final CountProvider requestCounter;
  private final CountProvider acceptCounter;

  private volatile long nextRejectionRatioUpdateTime = -1;
  private volatile double rejectionRatio = 0;

  private LoadController(Builder builder) {
    this.rejectionRatioUpdateIntervalInSec = builder.rejectionRatioUpdateIntervalInSec;
    this.maxRejectionRatio = builder.maxRejectionRatio;
    this.acceptMultiplier = builder.acceptMultiplier;
    this.time = builder.time;
    if (builder.useIndependentCounter) {
      this.requestCounter = new LongAdderCountProvider();
      this.acceptCounter = new LongAdderCountProvider();
    } else {
      int windowSizeInSec = builder.windowSizeInSec;
      if (this.rejectionRatioUpdateIntervalInSec >= windowSizeInSec) {
        throw new IllegalArgumentException("Rejection ratio update interval should be less than window size");
      }
      MetricsRepository metricsRepository =
          new MetricsRepository(new MetricConfig().timeWindow(windowSizeInSec, TimeUnit.SECONDS));
      Sensor requestSensor = metricsRepository.sensor("request");
      Metric requestMetric = requestSensor.add("request", new SampledCount());
      Sensor acceptSensor = metricsRepository.sensor("accept");
      Metric acceptMetric = acceptSensor.add("accept", new SampledCount());
      this.requestCounter = new TehutiCountProvider(requestSensor, requestMetric);
      this.acceptCounter = new TehutiCountProvider(acceptSensor, acceptMetric);
    }
  }

  public void recordRequest() {
    requestCounter.record();
  }

  public void recordAccept() {
    acceptCounter.record();
  }

  public double getRejectionRatio() {
    if (time.getMilliseconds() < nextRejectionRatioUpdateTime) {
      return rejectionRatio;
    }
    synchronized (this) {
      if (time.getMilliseconds() < nextRejectionRatioUpdateTime) {
        return rejectionRatio;
      }
      double requestCount = requestCounter.count();
      double acceptCount = acceptCounter.count();
      if (requestCount == 0.0d) {
        rejectionRatio = 0;
      } else {
        // TODO: maybe we can remove `+1` since we have already checked whether `requestCount` is 0 or not before.
        rejectionRatio = Math.max(0, (requestCount - acceptMultiplier * acceptCount) / (requestCount + 1));
      }
      nextRejectionRatioUpdateTime = time.getMilliseconds() + rejectionRatioUpdateIntervalInSec * 1000l;
      rejectionRatio = Math.min(rejectionRatio, maxRejectionRatio);

      return rejectionRatio;
    }
  }

  public boolean isOverloaded() {
    return getRejectionRatio() > 0;
  }

  public boolean shouldRejectRequest() {
    double rejectRatio = getRejectionRatio();
    if (rejectRatio == 0) {
      return false;
    }
    if (rejectRatio >= 1) {
      return true;
    }
    return Math.random() < rejectRatio;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private interface CountProvider {
    void record();

    /*
     * Returns the event count for the current measurement window. Semantics differ by
     * implementation: {@link TehutiCountProvider} returns a rolling windowed count (non-destructive,
     * re-readable); {@link LongAdderCountProvider} drains via {@code sumThenReset()} — calling
     * this method a second time in the same measurement cycle returns 0. Callers must invoke
     * {@code count()} exactly once per {@link LoadController#getRejectionRatio()} update cycle.
     */
    double count();
  }

  private static final class TehutiCountProvider implements CountProvider {
    private final Sensor sensor;
    private final Metric metric;

    TehutiCountProvider(Sensor sensor, Metric metric) {
      this.sensor = sensor;
      this.metric = metric;
    }

    @Override
    public void record() {
      sensor.record();
    }

    @Override
    public double count() {
      double value = metric.value();
      return Double.isFinite(value) ? value : 0.0;
    }
  }

  /*
   * Independent counter: a single LongAdder per request/accept stream. count() drains via
   * sumThenReset() so each ratio update sees only events since the last measurement. Safe because
   * getRejectionRatio() calls count() inside a synchronized block — only one thread drains at a time.
   */
  private static final class LongAdderCountProvider implements CountProvider {
    private final LongAdder adder = new LongAdder();

    @Override
    public void record() {
      adder.increment();
    }

    @Override
    public double count() {
      return adder.sumThenReset();
    }
  }

  public static class Builder {
    private int windowSizeInSec;
    private int rejectionRatioUpdateIntervalInSec;
    private double maxRejectionRatio;
    private double acceptMultiplier;
    private Time time = new SystemTime();
    private boolean useIndependentCounter = false;

    public Builder setWindowSizeInSec(int windowSizeInSec) {
      this.windowSizeInSec = windowSizeInSec;
      return this;
    }

    public Builder setMaxRejectionRatio(double maxRejectionRatio) {
      this.maxRejectionRatio = maxRejectionRatio;
      return this;
    }

    public Builder setAcceptMultiplier(double acceptMultiplier) {
      this.acceptMultiplier = acceptMultiplier;
      return this;
    }

    public Builder setRejectionRatioUpdateIntervalInSec(int rejectionRatioUpdateIntervalInSec) {
      this.rejectionRatioUpdateIntervalInSec = rejectionRatioUpdateIntervalInSec;
      return this;
    }

    public Builder setTime(Time time) {
      this.time = time;
      return this;
    }

    public Builder setUseIndependentCounter(boolean useIndependentCounter) {
      this.useIndependentCounter = useIndependentCounter;
      return this;
    }

    public LoadController build() {
      return new LoadController(this);
    }
  }

}
