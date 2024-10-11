package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.time.Clock;
import java.util.Iterator;
import java.util.Map;


public class NativeMetadataRepositoryStats extends AbstractVeniceStats {
  private final Sensor storeMetadataStalenessSensor;
  private final Map<String, Long> metadataCacheTimestampMapInMs = new VeniceConcurrentHashMap<>();
  private final Clock clock;

  public NativeMetadataRepositoryStats(MetricsRepository metricsRepository, String name, Clock clock) {
    super(metricsRepository, name);
    this.clock = clock;
    this.storeMetadataStalenessSensor = registerSensor(new AsyncGauge((ignored1, ignored2) -> {
      if (this.metadataCacheTimestampMapInMs.isEmpty()) {
        return Double.NaN;
      } else {
        Iterator<Long> iterator = metadataCacheTimestampMapInMs.values().iterator();
        long oldest = iterator.next();
        while (iterator.hasNext()) {
          oldest = Math.min(oldest, iterator.next());
        }
        return clock.millis() - oldest;
      }
    }, "store_metadata_staleness_high_watermark_ms"));
  }

  public void updateCacheTimestamp(String storeName, long cacheTimeStampInMs) {
    metadataCacheTimestampMapInMs.put(storeName, cacheTimeStampInMs);
  }

  public void removeCacheTimestamp(String storeName) {
    metadataCacheTimestampMapInMs.remove(storeName);
  }
}
