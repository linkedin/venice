package com.linkedin.venice.stats;

import com.linkedin.venice.throttle.TokenBucket;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.function.Supplier;


/**
 * Quota-related metrics that are extracted from the TokenBuckets
 */
public class ServerQuotaTokenBucketStats extends AbstractVeniceStats {

  private final Sensor quota;
  private final Sensor tokensAvailable;

  public ServerQuotaTokenBucketStats(MetricsRepository metricsRepository, String name, Supplier<TokenBucket> tokenBucketSupplier) {
    super(metricsRepository, name);
    quota = registerSensor("QuotaRcuPerSecondAllowed",
        new Gauge(() -> tokenBucketSupplier.get().getAmortizedRefillPerSecond()));
    tokensAvailable = registerSensor("QuotaRcuTokensRemaining",
        new Gauge(() -> tokenBucketSupplier.get().getStaleTokenCount()));
  }

}
