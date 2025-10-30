package com.linkedin.venice.meta;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.RetryManagerStats;
import com.linkedin.venice.throttle.TokenBucket;
import io.tehuti.metrics.MetricsRepository;
import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RetryManager {
  private static final Logger LOGGER = LogManager.getLogger(RetryManager.class);
  private static final int TOKEN_BUCKET_REFILL_INTERVAL_IN_SECONDS = 1;
  /**
   * Capacity multiple of 5 to accommodate retry burst. For example, long tail retry budget is 10% of request.
   * A multiple of 5 will allow 50% of the requests trigger long tail retry for a short period of time.
   */
  private static final int TOKEN_BUCKET_CAPACITY_MULTIPLE = 5;
  private final AtomicBoolean retryBudgetEnabled = new AtomicBoolean();
  private final long enforcementWindowInMs;
  private final double retryBudgetInPercentDecimal;
  private final AtomicLong requestCount = new AtomicLong();
  private final AtomicReference<TokenBucket> retryTokenBucket = new AtomicReference<>(null);
  private final ScheduledExecutorService scheduler;
  private final Clock clock;
  private RetryManagerStats retryManagerStats;
  private long lastUpdateTimestamp;
  private long previousQPS = 0;

  public RetryManager(
      MetricsRepository metricsRepository,
      String metricNamePrefix,
      String storeName,
      RequestType requestType,
      long enforcementWindowInMs,
      double retryBudgetInPercentDecimal,
      Clock clock,
      ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
    if (enforcementWindowInMs <= 0 || retryBudgetInPercentDecimal <= 0) {
      retryBudgetEnabled.set(false);
    } else {
      retryBudgetEnabled.set(true);
      lastUpdateTimestamp = clock.millis();
      retryManagerStats = new RetryManagerStats(metricsRepository, metricNamePrefix, storeName, requestType, this);
      this.scheduler.schedule(this::updateRetryTokenBucket, enforcementWindowInMs, TimeUnit.MILLISECONDS);
    }
    this.enforcementWindowInMs = enforcementWindowInMs;
    this.retryBudgetInPercentDecimal = retryBudgetInPercentDecimal;
    this.clock = clock;
  }

  public RetryManager(
      MetricsRepository metricsRepository,
      String metricNamePrefix,
      long enforcementWindowInMs,
      double retryBudgetInPercentDecimal,
      ScheduledExecutorService scheduler,
      String storeName,
      RequestType requestType) {
    this(
        metricsRepository,
        metricNamePrefix,
        storeName,
        requestType,
        enforcementWindowInMs,
        retryBudgetInPercentDecimal,
        Clock.systemUTC(),
        scheduler);
  }

  public void recordRequest() {
    if (retryBudgetEnabled.get()) {
      requestCount.incrementAndGet();
    }
  }

  public void recordRequests(int requests) {
    if (retryBudgetEnabled.get()) {
      requestCount.getAndAdd(requests);
    }
  }

  public boolean isRetryAllowed() {
    return this.isRetryAllowed(1);
  }

  public boolean isRetryAllowed(int numberOfRetries) {
    TokenBucket tokenBucket = retryTokenBucket.get();
    if (!retryBudgetEnabled.get() || tokenBucket == null) {
      // All retries are allowed when the feature is disabled or during the very first enforcement window when we
      // haven't collected enough data points yet
      return true;
    }
    boolean retryAllowed = retryTokenBucket.get().tryConsume(numberOfRetries);
    if (!retryAllowed) {
      retryManagerStats.recordRejectedRetry(numberOfRetries);
    }
    return retryAllowed;
  }

  private void updateRetryTokenBucket() {
    if (retryBudgetEnabled.get()) {
      try {
        /**
         * Always schedule the next update of retry token bucket, even if the request count is 0 as the traffic
         * can come back later, and we want to make sure that the retry token bucket is updated according to the
         * new traffic volume.
         */
        scheduler.schedule(this::updateRetryTokenBucket, enforcementWindowInMs, TimeUnit.MILLISECONDS);
        long elapsedTimeInMs = clock.millis() - lastUpdateTimestamp;
        lastUpdateTimestamp = clock.millis();
        if (requestCount.get() > 0) {
          long requestCountSinceLastUpdate = requestCount.getAndSet(0);
          // Minimum user request per second will be 1
          long newQPS = (long) Math
              .ceil((double) requestCountSinceLastUpdate / (double) TimeUnit.MILLISECONDS.toSeconds(elapsedTimeInMs));
          if (previousQPS > 0) {
            long difference = Math.abs(previousQPS - newQPS);
            double differenceInPercentDecimal = (double) difference / (double) previousQPS;
            if (differenceInPercentDecimal > 0.1) {
              // Only update the retry token bucket if the change in request per seconds is more than 10 percent
              previousQPS = newQPS;
              updateTokenBucket(newQPS);
            }
          } else {
            previousQPS = newQPS;
            updateTokenBucket(newQPS);
          }
        }
      } catch (Throwable e) {
        LOGGER.warn("Caught exception when trying to update retry budget, retry budget will be disabled", e);
        // Once disabled it will not be re-enabled until client is restarted
        retryBudgetEnabled.set(false);
      }
    }
  }

  private void updateTokenBucket(long newQPS) {
    // Retry budget QPS should be greater than or equal to 1
    if (newQPS > 0) {
      long newRetryBudgetQPS = (long) Math.ceil((double) newQPS * retryBudgetInPercentDecimal);
      long refillAmount = newRetryBudgetQPS * TOKEN_BUCKET_REFILL_INTERVAL_IN_SECONDS;
      long capacity = TOKEN_BUCKET_CAPACITY_MULTIPLE * refillAmount;
      TokenBucket newTokenBucket =
          new TokenBucket(capacity, refillAmount, TOKEN_BUCKET_REFILL_INTERVAL_IN_SECONDS, TimeUnit.SECONDS, clock);
      retryTokenBucket.set(newTokenBucket);
    }
  }

  public TokenBucket getRetryTokenBucket() {
    return retryTokenBucket.get();
  }
}
