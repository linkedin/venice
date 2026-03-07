package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.time.Clock;
import java.util.Map;


public class NativeMetadataRepositoryStats extends AbstractVeniceStats {
  private final Sensor storeMetadataStalenessSensor;
  private final Map<String, Long> metadataCacheTimestampMapInMs = new VeniceConcurrentHashMap<>();
  private final Clock clock;

  public NativeMetadataRepositoryStats(MetricsRepository metricsRepository, String name, Clock clock) {
    super(metricsRepository, name);
    this.clock = clock;
    this.storeMetadataStalenessSensor = registerSensor(
        new AsyncGauge(
            (ignored1, ignored2) -> getMetadataStalenessHighWatermarkMs(),
            "store_metadata_staleness_high_watermark_ms"));
  }

  public final double getMetadataStalenessHighWatermarkMs() {
    // Use a single atomic operation to avoid TOCTOU race: a concurrent removeCacheTimestamp()
    // could empty the map between an isEmpty() check and the stream().min().get() call.
    return metadataCacheTimestampMapInMs.values()
        .stream()
        .min(Long::compareTo)
        .map(oldest -> (double) (clock.millis() - oldest))
        .orElse(Double.NaN);
  }

  public void updateCacheTimestamp(String storeName, long cacheTimeStampInMs) {
    metadataCacheTimestampMapInMs.put(storeName, cacheTimeStampInMs);
  }

  public void removeCacheTimestamp(String storeName) {
    metadataCacheTimestampMapInMs.remove(storeName);
  }
}
