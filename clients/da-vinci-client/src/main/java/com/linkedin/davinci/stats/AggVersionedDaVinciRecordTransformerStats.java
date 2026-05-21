package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.DaVinciRecordTransformerOtelMetricEntity.RECORD_TRANSFORMER_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerOtelMetricEntity.RECORD_TRANSFORMER_LATENCY;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceRecordTransformerOperation;
import com.linkedin.venice.stats.metrics.AbstractStatsCloseable;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;


/**
 * The store level stats for {@link com.linkedin.davinci.client.DaVinciRecordTransformer}.
 * OTel metrics are recorded directly here (separate API) because Tehuti uses the Reporter
 * layer ({@link DaVinciRecordTransformerStatsReporter}) with AsyncGauge polling, while OTel
 * records at the point of the call.
 */
public class AggVersionedDaVinciRecordTransformerStats
    extends AbstractVeniceAggVersionedStats<DaVinciRecordTransformerStats, DaVinciRecordTransformerStatsReporter> {
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final boolean emitOtelMetrics;

  /**
   * Per-store entry map. Each entry bundles the per-store {@link AbstractStatsCloseable#resources}
   * with the latency and error-count wrappers. A single {@code remove()} in
   * {@link #handleStoreDeleted(String)} closes both wrappers atomically — there is no
   * cross-map race window where a concurrent record could resurrect parallel entries.
   * Bounded by the number of stores on this host.
   */
  private final Map<String, PerStoreEntry> perStore = new VeniceConcurrentHashMap<>();

  /** Per-store state held by {@link #perStore}. */
  private static final class PerStoreEntry extends AbstractStatsCloseable {
    final MetricEntityStateOneEnum<VeniceRecordTransformerOperation> latency;
    final MetricEntityStateOneEnum<VeniceRecordTransformerOperation> errorCount;

    PerStoreEntry(VeniceOpenTelemetryMetricsRepository otelRepository, Map<VeniceMetricsDimensions, String> dims) {
      this.latency = MetricEntityStateOneEnum.create(
          RECORD_TRANSFORMER_LATENCY.getMetricEntity(),
          otelRepository,
          dims,
          VeniceRecordTransformerOperation.class,
          statsCloseables);
      this.errorCount = MetricEntityStateOneEnum.create(
          RECORD_TRANSFORMER_ERROR_COUNT.getMetricEntity(),
          otelRepository,
          dims,
          VeniceRecordTransformerOperation.class,
          statsCloseables);
    }
  }

  public AggVersionedDaVinciRecordTransformerStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      VeniceServerConfig serverConfig) {
    super(
        metricsRepository,
        metadataRepository,
        DaVinciRecordTransformerStats::new,
        DaVinciRecordTransformerStatsReporter::new,
        serverConfig.isUnregisterMetricForDeletedStoreEnabled());

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(serverConfig.getClusterName()).build();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
    this.emitOtelMetrics = otelData.emitOpenTelemetryMetrics();
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    try {
      super.handleStoreDeleted(storeName);
    } finally {
      MetricEntityStateUtils.closeQuietly(perStore.remove(storeName));
    }
  }

  public void recordPutLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordPutLatency(value, timestamp));
    recordOtelLatency(storeName, value, VeniceRecordTransformerOperation.PUT);
  }

  public void recordDeleteLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordDeleteLatency(value, timestamp));
    recordOtelLatency(storeName, value, VeniceRecordTransformerOperation.DELETE);
  }

  public void recordPutError(String storeName, int version, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordPutError(timestamp));
    recordOtelErrorCount(storeName, VeniceRecordTransformerOperation.PUT);
  }

  public void recordDeleteError(String storeName, int version, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordDeleteError(timestamp));
    recordOtelErrorCount(storeName, VeniceRecordTransformerOperation.DELETE);
  }

  private void recordOtelLatency(String storeName, double value, VeniceRecordTransformerOperation operation) {
    if (!emitOtelMetrics) {
      return;
    }
    getOrCreateEntry(storeName).latency.record(value, operation);
  }

  private void recordOtelErrorCount(String storeName, VeniceRecordTransformerOperation operation) {
    if (!emitOtelMetrics) {
      return;
    }
    getOrCreateEntry(storeName).errorCount.record(1, operation);
  }

  private PerStoreEntry getOrCreateEntry(String storeName) {
    return perStore.computeIfAbsent(
        storeName,
        k -> new PerStoreEntry(
            otelRepository,
            OpenTelemetryMetricsSetup.buildStoreDimensionsMap(baseDimensionsMap, k)));
  }

  @VisibleForTesting
  boolean hasMetricsFor(String storeName) {
    return perStore.get(storeName) != null;
  }

  @VisibleForTesting
  int storeCount() {
    return perStore.size();
  }

  @Override
  public void close() {
    // Unregister metadata listener first so handleStore* can't re-populate the map while we drain.
    super.close();
    MetricEntityStateUtils.closeAndClear(perStore);
  }
}
