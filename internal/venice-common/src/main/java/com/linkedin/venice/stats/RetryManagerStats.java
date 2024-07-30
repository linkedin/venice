package com.linkedin.venice.stats;

import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.throttle.TokenBucket;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.OccurrenceRate;


public class RetryManagerStats extends AbstractVeniceStats {
  private final Sensor retryLimitPerSeconds;
  private final Sensor retriesRemaining;
  private final Sensor rejectedRetrySensor;

  public RetryManagerStats(MetricsRepository metricsRepository, String name, RetryManager retryManager) {
    super(metricsRepository, name);
    retryLimitPerSeconds = registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> {
      TokenBucket bucket = retryManager.getRetryTokenBucket();
      if (bucket == null) {
        return -1;
      } else {
        return bucket.getAmortizedRefillPerSecond();
      }
    }, "retry_limit_per_seconds"));
    retriesRemaining = registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> {
      TokenBucket bucket = retryManager.getRetryTokenBucket();
      if (bucket == null) {
        return -1;
      } else {
        return bucket.getStaleTokenCount();
      }
    }, "retries_remaining"));
    rejectedRetrySensor = registerSensorIfAbsent("rejected_retry", new OccurrenceRate());
  }

  public void recordRejectedRetry(int count) {
    rejectedRetrySensor.record(count);
  }
}
