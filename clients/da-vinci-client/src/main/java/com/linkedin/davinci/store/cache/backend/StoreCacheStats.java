package com.linkedin.davinci.store.cache.backend;

import com.linkedin.davinci.store.cache.VeniceStoreCache;
import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;


public class StoreCacheStats extends AbstractVeniceStats {
  private final Sensor cacheHitRate;
  private final Sensor cacheMissCount;
  private final Sensor cacheHitCount;
  private VeniceStoreCache servingCache;

  public StoreCacheStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    cacheHitCount = registerSensor(new AsyncGauge((c, t) -> this.getHitCount(), "cache_hit"));
    cacheMissCount = registerSensor(new AsyncGauge((c, t) -> this.getMissCount(), "cache_miss"));
    cacheHitRate = registerSensor(new AsyncGauge((c, t) -> this.getHitRate(), "cache_hit_rate"));
  }

  public synchronized void registerServingCache(VeniceStoreCache cache) {
    servingCache = cache;
  }

  public final synchronized long getHitCount() {
    return servingCache == null ? 0 : servingCache.hitCount();
  }

  public final synchronized long getMissCount() {
    return servingCache == null ? 0 : servingCache.missCount();
  }

  public final synchronized double getHitRate() {
    return servingCache == null ? 0 : servingCache.hitRate();
  }
}
