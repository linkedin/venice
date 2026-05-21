package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.NativeMetadataRepositoryOtelMetricEntity.METADATA_CACHE_STALENESS;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AbstractStatsCloseable;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.time.Clock;
import java.util.Map;
import java.util.function.DoubleSupplier;


/**
 * Tracks metadata cache staleness for {@link com.linkedin.davinci.repository.NativeMetadataRepository}.
 *
 * <p>Tehuti emits a single high-watermark gauge (oldest store's staleness across all stores).
 * OTel emits per-store ASYNC_DOUBLE_GAUGE with STORE_NAME dimension — backends can compute the
 * high watermark at query time via max aggregation.
 *
 * <p>Per-store OTel callbacks are registered lazily on first {@link #updateCacheTimestamp} call
 * and read from the shared {@link #metadataCacheTimestampMapInMs}. When a store is removed via
 * {@link #handleStoreDeleted(String)} the OTel callback is deregistered (closed) and the per-store map entry is
 * dropped so the SDK stops polling the gauge.
 */
public class NativeMetadataRepositoryStats extends AbstractVeniceStats {
  private final Map<String, Long> metadataCacheTimestampMapInMs = new VeniceConcurrentHashMap<>();
  private final Clock clock;

  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  /** Per-store ASYNC_DOUBLE_GAUGE; entries are closed in {@link #handleStoreDeleted} so the SDK stops polling. */
  private final Map<String, PerStoreEntry> perStoreEntryMap = new VeniceConcurrentHashMap<>();

  /** Per-store state held by {@link #perStoreEntryMap}. */
  private static final class PerStoreEntry extends AbstractStatsCloseable {
    final AsyncMetricEntityStateBase gauge;

    PerStoreEntry(
        VeniceOpenTelemetryMetricsRepository otelRepository,
        Map<VeniceMetricsDimensions, String> dims,
        Attributes attrs,
        DoubleSupplier callback) {
      this.gauge = AsyncMetricEntityStateBase
          .create(METADATA_CACHE_STALENESS.getMetricEntity(), otelRepository, dims, attrs, callback, statsCloseables);
    }
  }

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
   * Updates the cache timestamp for a store and lazily registers an OTel gauge on first call per store.
   *
   * @param clusterName used only on the first call per store to set the CLUSTER_NAME OTel dimension.
   *                    Subsequent calls for the same store ignore this parameter — the OTel gauge is
   *                    already registered under the first-seen cluster name and OTel callbacks cannot
   *                    be deregistered. Known limitation: if a store migrates clusters during the
   *                    lifetime of this process, the gauge will continue to emit under the original
   *                    cluster name dimension rather than the post-migration cluster.
   */
  public void updateCacheTimestamp(String storeName, String clusterName, long cacheTimeStampInMs) {
    metadataCacheTimestampMapInMs.put(storeName, cacheTimeStampInMs);
    registerOtelGaugeIfAbsent(storeName, clusterName);
  }

  /** Removes the Tehuti cache-timestamp entry and closes the per-store OTel async-gauge wrapper. */
  public void handleStoreDeleted(String storeName) {
    metadataCacheTimestampMapInMs.remove(storeName);
    MetricEntityStateUtils.closeQuietly(perStoreEntryMap.remove(storeName));
  }

  @Override
  public void close() {
    MetricEntityStateUtils.closeAndClear(perStoreEntryMap);
    super.close();
  }

  private void registerOtelGaugeIfAbsent(String storeName, String clusterName) {
    if (otelRepository == null) {
      return;
    }
    perStoreEntryMap.computeIfAbsent(storeName, k -> {
      Map<VeniceMetricsDimensions, String> dims =
          OpenTelemetryMetricsSetup.buildStoreDimensionsMap(baseDimensionsMap, k);
      dims.put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME, clusterName);
      Attributes attrs = otelRepository.createAttributes(METADATA_CACHE_STALENESS.getMetricEntity(), dims);
      // DoubleSupplier callback returns NaN when store is removed (no timestamp in map),
      // consistent with the Tehuti high-watermark gauge behavior.
      return new PerStoreEntry(otelRepository, dims, attrs, () -> {
        Long ts = metadataCacheTimestampMapInMs.get(k);
        return ts == null ? Double.NaN : (double) (clock.millis() - ts);
      });
    });
  }
}
