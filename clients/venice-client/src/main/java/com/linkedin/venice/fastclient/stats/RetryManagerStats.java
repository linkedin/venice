package com.linkedin.venice.fastclient.stats;

import com.linkedin.venice.fastclient.meta.RetryManager;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.throttle.TokenBucket;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;


public class RetryManagerStats extends AbstractVeniceStats {
  private final Sensor retryLimitPerSeconds;
  private final Sensor retriesRemaining;

  public RetryManagerStats(MetricsRepository metricsRepository, String name, RetryManager retryManager) {
    super(metricsRepository, name);
    retryLimitPerSeconds = registerSensor(new AsyncGauge((ignored, ignored2) -> {
      TokenBucket bucket = retryManager.getRetryTokenBucket();
      if (bucket == null) {
        return -1;
      } else {
        return bucket.getAmortizedRefillPerSecond();
      }
    }, "retry_limit_per_seconds"));
    retriesRemaining = registerSensor(new AsyncGauge((ignored, ignored2) -> {
      TokenBucket bucket = retryManager.getRetryTokenBucket();
      if (bucket == null) {
        return -1;
      } else {
        return bucket.getStaleTokenCount();
      }
    }, "retries_remaining"));
  }
}
