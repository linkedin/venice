package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.fastclient.stats.RetryManagerStats;
import com.linkedin.venice.throttle.TokenBucket;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class offers advanced client retry behaviors. Specifically enforcing a retry budget and relevant monitoring to
 * avoid retry storm and alert users when the retry threshold is misconfigured or service is degrading.
 */
public class RetryManager implements Closeable {
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
  RetryManagerStats retryManagerStats;
  private long lastUpdateTimestamp;
  private long previousQPS = 0;

  public RetryManager(
      MetricsRepository metricsRepository,
      String metricNamePrefix,
      long enforcementWindowInMs,
      double retryBudgetInPercentDecimal,
      Clock clock) {
    if (enforcementWindowInMs <= 0 || retryBudgetInPercentDecimal <= 0) {
      scheduler = null;
      retryBudgetEnabled.set(false);
    } else {
      scheduler = Executors.newScheduledThreadPool(1);
      retryBudgetEnabled.set(true);
      lastUpdateTimestamp = clock.millis();
      retryManagerStats = new RetryManagerStats(metricsRepository, metricNamePrefix, this);
      scheduler.schedule(this::updateRetryTokenBucket, enforcementWindowInMs, TimeUnit.MILLISECONDS);
    }
    this.enforcementWindowInMs = enforcementWindowInMs;
    this.retryBudgetInPercentDecimal = retryBudgetInPercentDecimal;
    this.clock = clock;
  }

  public RetryManager(
      MetricsRepository metricsRepository,
      String metricNamePrefix,
      long enforcementWindowInMs,
      double retryBudgetInPercentDecimal) {
    this(metricsRepository, metricNamePrefix, enforcementWindowInMs, retryBudgetInPercentDecimal, Clock.systemUTC());
  }

  public void recordRequest() {
    if (retryBudgetEnabled.get()) {
      requestCount.incrementAndGet();
    }
  }

  public boolean isRetryAllowed() {
    TokenBucket tokenBucket = retryTokenBucket.get();
    if (!retryBudgetEnabled.get() || tokenBucket == null) {
      // All retries are allowed when the feature is disabled or during the very first enforcement window when we
      // haven't collected enough data points yet
      return true;
    }
    boolean retryAllowed = retryTokenBucket.get().tryConsume(1);
    if (!retryAllowed) {
      retryManagerStats.recordRejectedRetry();
    }
    return retryAllowed;
  }

  private void updateRetryTokenBucket() {
    if (retryBudgetEnabled.get() && requestCount.get() > 0) {
      try {
        long elapsedTimeInMs = clock.millis() - lastUpdateTimestamp;
        long requestCountSinceLastUpdate = requestCount.getAndSet(0);
        lastUpdateTimestamp = clock.millis();
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
        scheduler.schedule(this::updateRetryTokenBucket, enforcementWindowInMs, TimeUnit.MILLISECONDS);
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

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }
}
