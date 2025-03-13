package com.linkedin.venice.reliability;

import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.SampledCount;
import java.util.concurrent.TimeUnit;
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
 * Here is how the rejection ratio calculation being implemented here:
 * 1. This class is using a sliding window to calcuate the request rate and accept rate.
 * 2. The window size is controlled by {@link #windowSizeInSec}.
 * 3. To reduce the overhead of calculating the rejection ratio, we only calculate it every
 *    {@link #rejectionRatioUpdateIntervalInSec}.
 * 4. This class also limits the maximum rejection ratio to {@link #maxRejectionRatio} to avoid
 *    rejecting every request, otherwise, the backend won't be able to recover automatically.
 * 5. We can tune {@link #acceptMultiplier} to control the rejection ratio.
 */
public class LoadController {
  private final static Logger LOGGER = LogManager.getLogger(LoadController.class);
  private final int windowSizeInSec;
  private final int rejectionRatioUpdateIntervalInSec;
  private final double maxRejectionRatio;
  private final Time time;
  private final double acceptMultiplier;

  private final Sensor requestSensor;
  private final Metric requestMetric;
  private final Sensor acceptSensor;
  private final Metric acceptMetric;

  private volatile long nextRejectionRatioUpdateTime = -1;
  private volatile double rejectionRatio = 0;

  private LoadController(Builder builder) {
    this.windowSizeInSec = builder.windowSizeInSec;
    this.rejectionRatioUpdateIntervalInSec = builder.rejectionRatioUpdateIntervalInSec;
    if (this.rejectionRatioUpdateIntervalInSec >= this.windowSizeInSec) {
      throw new IllegalArgumentException("Rejection ratio update interval should be less than window size");
    }
    this.maxRejectionRatio = builder.maxRejectionRatio;
    this.acceptMultiplier = builder.acceptMultiplier;
    this.time = builder.time;
    MetricsRepository metricsRepository =
        new MetricsRepository(new MetricConfig().timeWindow(windowSizeInSec, TimeUnit.SECONDS));
    this.requestSensor = metricsRepository.sensor("request");
    this.requestMetric = requestSensor.add("request", new SampledCount());
    this.acceptSensor = metricsRepository.sensor("accept");
    this.acceptMetric = acceptSensor.add("accept", new SampledCount());
  }

  public void recordRequest() {
    requestSensor.record();
  }

  public void recordAccept() {
    acceptSensor.record();
  }

  public double getRejectionRatio() {
    if (time.getMilliseconds() < nextRejectionRatioUpdateTime) {
      return rejectionRatio;
    }
    synchronized (this) {
      if (time.getMilliseconds() < nextRejectionRatioUpdateTime) {
        return rejectionRatio;
      }
      double requestCount = getMetricValue(requestMetric);
      double acceptCount = getMetricValue(acceptMetric);
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

  private double getMetricValue(Metric metric) {
    double value = metric.value();
    return Double.isFinite(value) ? value : 0.0;
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

  public static class Builder {
    private int windowSizeInSec;
    private int rejectionRatioUpdateIntervalInSec;
    private double maxRejectionRatio;
    private double acceptMultiplier;
    private Time time = new SystemTime();

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

    public LoadController build() {
      return new LoadController(this);
    }
  }

}
