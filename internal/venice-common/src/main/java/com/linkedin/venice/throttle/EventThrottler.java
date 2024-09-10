package com.linkedin.venice.throttle;

import com.linkedin.venice.exceptions.QuotaExceededException;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Quota;
import io.tehuti.metrics.QuotaViolationException;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.utils.Time;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A class to throttle Events to a certain rate
 *
 * This class takes a maximum rate in events/sec and a minimum interval over
 * which to check the rate. The rate is measured over two rolling windows: one
 * full window, and one in-flight window. Each window is bounded to the provided
 * interval in ms, therefore, the total interval measured over is up to twice
 * the provided interval parameter. If the current event rate exceeds the maximum,
 * the call to {@link #maybeThrottle(double)} will handle this case based on the given
 * throttling strategy.
 *
 * This is a generalized IoThrottler as it existed before, which can be used to
 * throttle Bytes read or written, number of entries scanned, etc.
 */
public class EventThrottler implements VeniceRateLimiter {
  private static final Logger LOGGER = LogManager.getLogger(EventThrottler.class);
  private static final long DEFAULT_CHECK_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
  private static final String THROTTLER_NAME = "event-throttler";
  private static final String UNIT_POSTFIX = " event/sec";

  public static final EventThrottlingStrategy BLOCK_STRATEGY = new BlockEventThrottlingStrategy();
  public static final EventThrottlingStrategy REJECT_STRATEGY = new RejectEventThrottlingStrategy();

  private final LongSupplier maxRatePerSecondProvider;
  private final long enforcementIntervalMs;
  private final String throttlerName;
  private final boolean checkQuotaBeforeRecording;

  // Tehuti stuff
  private long configuredMaxRatePerSecond = -1;
  private final io.tehuti.utils.Time time;
  private Sensor rateSensor = null;
  private MetricConfig rateConfig = null;
  private final EventThrottlingStrategy throttlingStrategy;

  // Used only to compare if the new quota requests are different from the existing quota.
  private long quota = -1;

  /**
   * @param maxRatePerSecond Maximum rate that this throttler should allow (-1 is unlimited)
   */
  public EventThrottler(long maxRatePerSecond) {
    this(maxRatePerSecond, DEFAULT_CHECK_INTERVAL_MS, null, false, BLOCK_STRATEGY);
  }

  /**
   * @param maxRatePerSecond          Maximum rate that this throttler should allow (-1 is unlimited)
   * @param throttlerName             if specified, the throttler will share its limit with others named the same
   *                                  if null, the throttler will be independent of the others
   * @param throttlingStrategy        the strategy how throttler handle the quota exceeding case.
   * @param checkQuotaBeforeRecording if true throttler will check the quota before recording usage, otherwise throttler
   *                                  will record usage then check the quota.
   */
  public EventThrottler(
      long maxRatePerSecond,
      String throttlerName,
      boolean checkQuotaBeforeRecording,
      EventThrottlingStrategy throttlingStrategy) {
    this(maxRatePerSecond, DEFAULT_CHECK_INTERVAL_MS, throttlerName, checkQuotaBeforeRecording, throttlingStrategy);
  }

  /**
   * @param maxRatePerSecond Maximum rate that this throttler should allow (-1 is unlimited)
   * @param intervalMs Minimum interval over which the rate is measured (maximum is twice that)
   * @param throttlerName if specified, the throttler will share its limit with others named the same
   *                      if null, the throttler will be independent of the others
   * @param checkQuotaBeforeRecording if true throttler will check the quota before recording usage, otherwise throttler will record usage then check the quota.
   * @param throttlingStrategy the strategy how throttler handle the quota exceeding case.
   */
  public EventThrottler(
      long maxRatePerSecond,
      long intervalMs,
      String throttlerName,
      boolean checkQuotaBeforeRecording,
      EventThrottlingStrategy throttlingStrategy) {
    this(
        new io.tehuti.utils.SystemTime(),
        maxRatePerSecond,
        intervalMs,
        throttlerName,
        checkQuotaBeforeRecording,
        throttlingStrategy);
  }

  /**
   * @param time Used to inject a {@link io.tehuti.utils.Time} in tests
   * @param maxRatePerSecond Maximum rate that this throttler should allow (-1 is unlimited)
   * @param intervalMs Minimum interval over which the rate is measured (maximum is twice that)
   * @param throttlerName if specified, the throttler will share its limit with others named the same
   *                      if null, the throttler will be independent of the others
   * @param checkQuotaBeforeRecording if true throttler will check the quota before recording usage, otherwise throttler will record usage then check the quota.
   * @param throttlingStrategy the strategy how throttler handle the quota exceeding case.
   */
  public EventThrottler(
      io.tehuti.utils.Time time,
      long maxRatePerSecond,
      long intervalMs,
      String throttlerName,
      boolean checkQuotaBeforeRecording,
      EventThrottlingStrategy throttlingStrategy) {
    this(time, () -> maxRatePerSecond, intervalMs, throttlerName, checkQuotaBeforeRecording, throttlingStrategy);
  }

  /**
   * @param maxRatePerSecondProvider Provider for maximum rate that this throttler should allow (-1 is unlimited)
   * @param intervalMs Minimum interval over which the rate is measured (maximum is twice that)
   * @param throttlerName if specified, the throttler will share its limit with others named the same
   *                      if null, the throttler will be independent of the others
   * @param checkQuotaBeforeRecording if true throttler will check the quota before recording usage, otherwise throttler will record usage then check the quota.
   * @param throttlingStrategy the strategy how throttler handle the quota exceeding case.
   */
  public EventThrottler(
      LongSupplier maxRatePerSecondProvider,
      long intervalMs,
      String throttlerName,
      boolean checkQuotaBeforeRecording,
      EventThrottlingStrategy throttlingStrategy) {
    this(
        new io.tehuti.utils.SystemTime(),
        maxRatePerSecondProvider,
        intervalMs,
        throttlerName,
        checkQuotaBeforeRecording,
        throttlingStrategy);
  }

  /**
   * @param time Used to inject a {@link io.tehuti.utils.Time} in tests
   * @param maxRatePerSecondProvider Provider for maximum rate that this throttler should allow (-1 is unlimited)
   * @param intervalMs Minimum interval over which the rate is measured (maximum is twice that)
   * @param throttlerName if specified, the throttler will share its limit with others named the same
   *                      if null, the throttler will be independent of the others
   * @param checkQuotaBeforeRecording if true throttler will check the quota before recording usage, otherwise throttler will record usage then check the quota.
   * @param throttlingStrategy the strategy how throttler handle the quota exceeding case.
   */
  public EventThrottler(
      @Nonnull io.tehuti.utils.Time time,
      LongSupplier maxRatePerSecondProvider,
      long intervalMs,
      String throttlerName,
      boolean checkQuotaBeforeRecording,
      @Nonnull EventThrottlingStrategy throttlingStrategy) {
    Validate.notNull(time);
    Validate.notNull(throttlingStrategy);
    this.time = time;
    this.maxRatePerSecondProvider = maxRatePerSecondProvider;
    this.throttlingStrategy = throttlingStrategy;
    this.enforcementIntervalMs = intervalMs;
    this.throttlerName = throttlerName != null ? throttlerName : THROTTLER_NAME;
    this.checkQuotaBeforeRecording = checkQuotaBeforeRecording;

    long maxRatePerSecond = maxRatePerSecondProvider.getAsLong();
    if (maxRatePerSecond >= 0) {
      initialize(maxRatePerSecond);
    }
    LOGGER.debug("EventThrottler constructed with maxRatePerSecond: {}", getMaxRatePerSecond());
  }

  /**
   * Sleeps if necessary to slow down the caller.
   *
   * @param eventsSeen Number of events seen since last invocation. Basis for
   *        determining whether its necessary to sleep.
   */
  public void maybeThrottle(double eventsSeen) {
    if (getMaxRatePerSecond() >= 0) {
      long now = time.milliseconds();
      try {
        rateSensor.record(eventsSeen, now);
      } catch (QuotaViolationException e) {
        throttlingStrategy.onExceedQuota(
            time,
            rateSensor.name(),
            (long) e.getValue(),
            getMaxRatePerSecond(),
            rateConfig.timeWindowMs());
      }
    }
  }

  private static class BlockEventThrottlingStrategy implements EventThrottlingStrategy {
    @Override
    public void onExceedQuota(Time time, String throttlerName, long currentRate, long quota, long timeWindowMS) {
      // If we're over quota, we calculate how long to sleep to compensate.
      double excessRate = currentRate - quota;
      final long sleepTimeMs;
      if (quota == 0) {
        sleepTimeMs = timeWindowMS;
      } else {
        sleepTimeMs = Math.round(excessRate / quota * Time.MS_PER_SECOND);
      }
      LOGGER.debug(
          "Throttler: {} quota exceeded:\ncurrentRate \t={}{}\nmaxRatePerSecond \t= {}{}\nexcessRate \t= {}{}\nsleeping for \t {} ms to compensate.\nrateConfig.timeWindowMs = {}",
          throttlerName,
          currentRate,
          UNIT_POSTFIX,
          quota,
          UNIT_POSTFIX,
          excessRate,
          UNIT_POSTFIX,
          sleepTimeMs,
          timeWindowMS);
      if (sleepTimeMs > timeWindowMS) {
        LOGGER.warn(
            "Throttler: {} sleep time ({} ms) exceeds window size ({} ms). This will likely result in not being able to honor the rate limit accurately.",
            throttlerName,
            sleepTimeMs,
            timeWindowMS);
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

  public final long getMaxRatePerSecond() {
    long maxRatePerSecond = maxRatePerSecondProvider.getAsLong();
    // If quota was disabled but enabled at run time, we need to initialize the required structures.
    initialize(maxRatePerSecond);
    return maxRatePerSecond;
  }

  private void initialize(long maxRatePerSecond) {
    // If the maxRatePerSecond is negative or if the value has not changed since the last time the throttler was
    // configured, then no need to modify the throttlers.
    if (maxRatePerSecond < 0 || maxRatePerSecond == configuredMaxRatePerSecond) {
      return;
    }

    if (enforcementIntervalMs <= 0) {
      throw new IllegalArgumentException("intervalMs must be a positive number.");
    }
    this.rateConfig = new MetricConfig().timeWindow(enforcementIntervalMs, TimeUnit.MILLISECONDS)
        .quota(Quota.lessThan(maxRatePerSecond, checkQuotaBeforeRecording));
    Rate rate = new Rate(TimeUnit.SECONDS);

    // Then we want this EventThrottler to be independent.
    MetricsRepository metricsRepository = new MetricsRepository(time);
    this.rateSensor = metricsRepository.sensor(throttlerName, rateConfig);
    rateSensor.add(THROTTLER_NAME + ".rate", rate, rateConfig);
    this.configuredMaxRatePerSecond = maxRatePerSecond;
  }

  protected io.tehuti.utils.Time getTime() {
    return time;
  }

  protected MetricConfig getRateConfig() {
    return rateConfig;
  }

  protected long getConfiguredMaxRatePerSecond() {
    return configuredMaxRatePerSecond;
  }

  protected boolean isCheckQuotaBeforeRecording() {
    return checkQuotaBeforeRecording;
  }

  @Override
  public boolean tryAcquirePermit(int units) {
    if (getMaxRatePerSecond() < 0) {
      return true;
    }
    long now = time.milliseconds();
    try {
      rateSensor.record(units, now);
      return true;
    } catch (QuotaViolationException e) {
      return false;
    }
  }

  @Override
  public void setQuota(long quota) {
    this.quota = quota;
  }

  @Override
  public long getQuota() {
    return quota;
  }

  @Override
  public String toString() {
    return "EventThrottler{" + "maxRatePerSecondProvider=" + maxRatePerSecondProvider + ", enforcementIntervalMs="
        + enforcementIntervalMs + ", throttlerName='" + throttlerName + ", checkQuotaBeforeRecording="
        + checkQuotaBeforeRecording + ", configuredMaxRatePerSecond=" + configuredMaxRatePerSecond + ", time=" + time
        + ", throttlingStrategy=" + throttlingStrategy + '}';
  }
}
