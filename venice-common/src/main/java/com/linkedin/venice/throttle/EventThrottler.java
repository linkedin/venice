package com.linkedin.venice.throttle;

import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Quota;
import io.tehuti.metrics.QuotaViolationException;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;

import io.tehuti.utils.Time;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * A class to throttle Events to a certain rate
 *
 * This class takes a maximum rate in events/sec and a minimum interval over
 * which to check the rate. The rate is measured over two rolling windows: one
 * full window, and one in-flight window. Each window is bounded to the provided
 * interval in ms, therefore, the total interval measured over is up to twice
 * the provided interval parameter. If the current event rate exceeds the maximum,
 * the call to {@link #maybeThrottle(int)} will handle this case based on the given
 * throttling strategy.
 *
 * This is a generalized IoThrottler as it existed before, which can be used to
 * throttle Bytes read or written, number of entries scanned, etc.
 */
public class EventThrottler {

  private static final Logger logger = Logger.getLogger(EventThrottler.class);
  private static final long DEFAULT_CHECK_INTERVAL_MS = 1000;
  private static final String THROTTLER_NAME = "event-throttler";
  private static final String UNIT_POSTFIX = " event/sec";

  public static final EventThrottlingStrategy BLOCK_STRATEGY = new BlockEventThrottlingStrategy();
  public static final EventThrottlingStrategy REJECT_STRATEGY = new RejectEventThrottlingStrategy();

  private final long maxRatePerSecond;

  // Tehuti stuff
  private final io.tehuti.utils.Time time;
  private static final MetricsRepository sharedMetricsRepository = new MetricsRepository();
  private final Rate rate;
  private final Sensor rateSensor;
  private final MetricConfig rateConfig;
  private final EventThrottlingStrategy throttlingStrategy;

  /**
   * @param maxRatePerSecond Maximum rate that this throttler should allow (0 is unlimited)
   */
  public EventThrottler(long maxRatePerSecond) {
    this(maxRatePerSecond, DEFAULT_CHECK_INTERVAL_MS, null, false, BLOCK_STRATEGY);
  }

  /**
   * @param maxRatePerSecond Maximum rate that this throttler should allow (0 is unlimited)
   * @param throttlingStrategy the strategy how throttler handle the quota exceeding case.
   * @param checkQuotaBeforeRecording if true throttler will check the quota before recording usage, otherwise throttler will record usage then check the quota.
   */
  public EventThrottler(long maxRatePerSecond, boolean checkQuotaBeforeRecording, EventThrottlingStrategy throttlingStrategy) {
    this(maxRatePerSecond, DEFAULT_CHECK_INTERVAL_MS, null, checkQuotaBeforeRecording, throttlingStrategy);
  }

  /**
   * @param maxRatePerSecond Maximum rate that this throttler should allow (0 is unlimited)
   * @param intervalMs Minimum interval over which the rate is measured (maximum is twice that)
   * @param throttlerName if specified, the throttler will share its limit with others named the same
   *                      if null, the throttler will be independent of the others
   * @param checkQuotaBeforeRecording if true throttler will check the quota before recording usage, otherwise throttler will record usage then check the quota.
   * @param throttlingStrategy the strategy how throttler handle the quota exceeding case.
   */
  public EventThrottler(long maxRatePerSecond, long intervalMs, String throttlerName, boolean checkQuotaBeforeRecording, EventThrottlingStrategy throttlingStrategy) {
    this(new io.tehuti.utils.SystemTime(), maxRatePerSecond, intervalMs, throttlerName, checkQuotaBeforeRecording, throttlingStrategy);
  }

  /**
   * @param time Used to inject a {@link io.tehuti.utils.Time} in tests
   * @param maxRatePerSecond Maximum rate that this throttler should allow (0 is unlimited)
   * @param intervalMs Minimum interval over which the rate is measured (maximum is twice that)
   * @param throttlerName if specified, the throttler will share its limit with others named the same
   *                      if null, the throttler will be independent of the others
   * @param checkQuotaBeforeRecording if true throttler will check the quota before recording usage, otherwise throttler will record usage then check the quota.
   * @param throttlingStrategy the strategy how throttler handle the quota exceeding case.
   */
  public EventThrottler(io.tehuti.utils.Time time,
      long maxRatePerSecond,
      long intervalMs,
      String throttlerName,
      boolean checkQuotaBeforeRecording,
      EventThrottlingStrategy throttlingStrategy) {
    this.maxRatePerSecond = maxRatePerSecond;
    this.throttlingStrategy = Utils.notNull(throttlingStrategy);
    if (maxRatePerSecond > 0) {
      this.time = Utils.notNull(time);
      if (intervalMs <= 0) {
        throw new IllegalArgumentException("intervalMs must be a positive number.");
      }
      this.rateConfig = new MetricConfig()
          .timeWindow(intervalMs, TimeUnit.MILLISECONDS)
          // TODO create quota that check it before recording the usage.
          .quota(Quota.lessThan(maxRatePerSecond));
      this.rate = new Rate(TimeUnit.SECONDS);
      if (throttlerName == null) {
        // Then we want this EventThrottler to be independent.
        MetricsRepository metricsRepository = new MetricsRepository(time);
        this.rateSensor = metricsRepository.sensor(THROTTLER_NAME, rateConfig);
        rateSensor.add(THROTTLER_NAME + ".rate", rate, rateConfig);
      } else {
        // Then we want to share the EventThrottler's limit with other instances having the same name.
        synchronized (sharedMetricsRepository) {
          Sensor existingSensor = sharedMetricsRepository.getSensor(throttlerName);
          if (existingSensor != null) {
            this.rateSensor = existingSensor;
          } else {
            // Create it once for all EventThrottlers sharing that name
            this.rateSensor = sharedMetricsRepository.sensor(throttlerName);
            this.rateSensor.add(throttlerName + ".rate", rate, rateConfig);
          }
        }
      }
    } else {
      // DISABLED, no point in allocating anything...
      this.time = null;
      this.rate = null;
      this.rateSensor = null;
      this.rateConfig = null;
    }

    if(logger.isDebugEnabled())
      logger.debug("EventThrottler constructed with maxRatePerSecond = " + maxRatePerSecond);

  }

  /**
   * Sleeps if necessary to slow down the caller.
   *
   * @param eventsSeen Number of events seen since last invocation. Basis for
   *        determining whether its necessary to sleep.
   */
  public synchronized void maybeThrottle(int eventsSeen) {
    if (maxRatePerSecond > 0) {
      long now = time.milliseconds();
      try {
        rateSensor.record(eventsSeen, now);
      } catch (QuotaViolationException e) {
        throttlingStrategy.onExceedQuota(time, rateSensor.name(), (long) e.getValue(), maxRatePerSecond,
            rateConfig.timeWindowMs());
      }
    }
  }

  private static class BlockEventThrottlingStrategy implements EventThrottlingStrategy{
    @Override
    public void onExceedQuota(Time time, String throttlerName, long currentRate, long quota, long timeWindowMS) {
      // If we're over quota, we calculate how long to sleep to compensate.
      double excessRate = currentRate - quota;
      long sleepTimeMs = Math.round(excessRate / quota * Time.MS_PER_SECOND);
      if (logger.isDebugEnabled()) {
        logger.debug("Throttler: " + throttlerName + " quota exceeded:\n" +
            "currentRate \t= " + currentRate + UNIT_POSTFIX + "\n" +
            "maxRatePerSecond \t= " + quota + UNIT_POSTFIX + "\n" +
            "excessRate \t= " + excessRate + UNIT_POSTFIX + "\n" +
            "sleeping for \t" + sleepTimeMs + " ms to compensate.\n" +
            "rateConfig.timeWindowMs() = " + timeWindowMS);
      }
      if (sleepTimeMs > timeWindowMS) {
        logger.warn("Throttler: " + throttlerName + " sleep time(" + sleepTimeMs + "ms) exceeds " +
            "window size (" + timeWindowMS + " ms). This will likely " +
            "result in not being able to honor the rate limit accurately.");
      }
      time.sleep(sleepTimeMs);
    }
  }

  /**
   * The strategy used by event throttler which will thrown an exception to reject the event request.
   */
  private static class RejectEventThrottlingStrategy implements EventThrottlingStrategy {

    @Override
    public void onExceedQuota(Time time, String throttlerName, long currentRate, long quota, long timeWindowMS) {
      throw new QuotaExceededException(throttlerName, currentRate + UNIT_POSTFIX, quota + UNIT_POSTFIX);
    }
  }
}
