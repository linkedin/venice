package com.linkedin.venice.router.stats;

import com.linkedin.venice.router.cache.RouterCache;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.LambdaStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;


public class RouterCacheStats extends AbstractVeniceStats {
  private final RouterCache routerCache;
  private final Sensor cacheSizeSensor;
  private final Sensor cacheEntryNumSensor;
  private final Sensor cacheSizeMaxDiffBetweenBucketsSensor;
  private final Sensor cacheEntryNumMaxDiffBetweenBucketsSensor;

  public RouterCacheStats(MetricsRepository metricsRepository, String name, RouterCache routerCache) {
    super(metricsRepository, name);

    this.routerCache = routerCache;
    this.cacheSizeSensor = registerSensor("cache_size", new LambdaStat(() -> routerCache.getCacheSize()));
    this.cacheEntryNumSensor = registerSensor("cache_entry_num", new LambdaStat(() -> routerCache.getEntryNum()));
    this.cacheSizeMaxDiffBetweenBucketsSensor = registerSensor("cache_size_max_diff_between_buckets",
        new LambdaStat(() -> routerCache.getCacheSizeMaxDiffBetweenBuckets()));
    this.cacheEntryNumMaxDiffBetweenBucketsSensor = registerSensor("cache_entry_num_max_diff_between_buckets",
        new LambdaStat(() -> routerCache.getEntryNumMaxDiffBetweenBuckets()));
  }
}
