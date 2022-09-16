package com.linkedin.venice.stats;

import com.linkedin.venice.throttle.TokenBucket;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.function.Supplier;


/**
 * Quota-related metrics that are extracted from the TokenBuckets
 */
public class ServerQuotaTokenBucketStats extends AbstractVeniceStats {
  private final Sensor quota;
  private final Sensor tokensAvailable;

  public ServerQuotaTokenBucketStats(
      MetricsRepository metricsRepository,
      String name,
      Supplier<TokenBucket> tokenBucketSupplier) {
    super(metricsRepository, name);
    quota = registerSensor("QuotaRcuPerSecondAllowed", new Gauge(() -> {
      TokenBucket bucket = tokenBucketSupplier.get();
      if (bucket == null) {
        return 0;
      } else {
        return bucket.getAmortizedRefillPerSecond();
      }
    }));
    tokensAvailable = registerSensor("QuotaRcuTokensRemaining", new Gauge(() -> {
      TokenBucket bucket = tokenBucketSupplier.get();
      if (bucket == null) {
        return 0;
      } else {
        return bucket.getStaleTokenCount();
      }
    }));
  }

}
