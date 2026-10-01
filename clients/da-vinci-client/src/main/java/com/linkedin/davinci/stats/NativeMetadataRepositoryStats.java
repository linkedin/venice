package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.NativeMetadataRepositoryOtelMetricEntity.METADATA_CACHE_STALENESS;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricScope;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;


/**
 * Tracks metadata cache staleness for {@link com.linkedin.davinci.repository.NativeMetadataRepository}.
 *
 * <p>Tehuti emits a single high-watermark gauge (oldest store's staleness across all stores).
 * OTel emits per-store ASYNC_DOUBLE_GAUGE with STORE_NAME dimension — backends can compute the
 * high watermark at query time via max aggregation.
 *
 * <p>Per-store OTel gauges are registered on a store's first {@link #updateCacheTimestamp} call and read from the
 * shared {@link #metadataCacheTimestampMapInMs}; {@link #removeCacheTimestamp} closes the store's gauge.
 */
public class NativeMetadataRepositoryStats extends AbstractVeniceStats {
  private final Map<String, Long> metadataCacheTimestampMapInMs = new VeniceConcurrentHashMap<>();
  private final Clock clock;

  // OTel: per-store ASYNC_DOUBLE_GAUGE for staleness, each in its own scope so removing the store closes it.
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final Map<String, MetricScope> otelPerStore = new VeniceConcurrentHashMap<>();

  public NativeMetadataRepositoryStats(MetricsRepository metricsRepository, String name, Clock clock) {
    super(metricsRepository, name);
    this.clock = clock;

    // Tehuti: single high-watermark gauge across all stores
    registerSensor(
        new AsyncGauge(
            (ignored1, ignored2) -> getMetadataStalenessHighWatermarkMs(),
            "store_metadata_staleness_high_watermark_ms"));

    // OTel setup
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).build();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
  }

  public final double getMetadataStalenessHighWatermarkMs() {
    // Iterate without streams to avoid allocation overhead on the hot metrics path.
    // Using a local variable for min also avoids the TOCTOU race where a concurrent
    // removeCacheTimestamp() could empty the map between an isEmpty() check and get().
    long oldest = Long.MAX_VALUE;
    for (long ts: metadataCacheTimestampMapInMs.values()) {
      if (ts < oldest) {
        oldest = ts;
      }
    }
    return oldest == Long.MAX_VALUE ? Double.NaN : (double) (clock.millis() - oldest);
  }

  /**
   * Updates the cache timestamp for a store and registers its OTel gauge if it has none. Synchronized with
   * {@link #removeCacheTimestamp}, so a store's timestamp and gauge are added and removed together.
   *
   * @param clusterName sets the CLUSTER_NAME OTel dimension when the store's gauge is registered. Later calls don't
   *                    change it, so a store that migrates clusters while tracked keeps reporting under its original
   *                    cluster until it is removed and added again.
   */
  public synchronized void updateCacheTimestamp(String storeName, String clusterName, long cacheTimeStampInMs) {
    metadataCacheTimestampMapInMs.put(storeName, cacheTimeStampInMs);
    registerOtelGaugeIfAbsent(storeName, clusterName);
  }

  public synchronized void removeCacheTimestamp(String storeName) {
    metadataCacheTimestampMapInMs.remove(storeName);
    MetricScope storeScope = otelPerStore.remove(storeName);
    if (storeScope != null) {
      getMetricScope().retire(storeScope);
    }
  }

  private void registerOtelGaugeIfAbsent(String storeName, String clusterName) {
    if (otelRepository == null) {
      return;
    }
    otelPerStore.computeIfAbsent(storeName, k -> {
      Map<VeniceMetricsDimensions, String> dims = new HashMap<>(baseDimensionsMap);
      dims.put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME, clusterName);
      dims.put(VeniceMetricsDimensions.VENICE_STORE_NAME, OpenTelemetryMetricsSetup.sanitizeStoreName(k));
      Attributes attrs = otelRepository.createAttributes(METADATA_CACHE_STALENESS.getMetricEntity(), dims);
      MetricScope storeScope = getMetricScope().register(new MetricScope());
      AsyncMetricEntityStateBase.createWithState(
          METADATA_CACHE_STALENESS.getMetricEntity(),
          otelRepository,
          dims,
          attrs,
          storeScope,
          () -> metadataCacheTimestampMapInMs.get(k),
          cacheTimestampMs -> clock.millis() - cacheTimestampMs);
      return storeScope;
    });
  }
}
