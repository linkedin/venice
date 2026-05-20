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
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
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
   * Per-store OTel metric state for latency. Bounded by the number of stores on this host.
   * Entries created lazily inside {@link #recordOtelLatency}, removed in
   * {@link #handleStoreDeleted(String)}.
   */
  private final Map<String, MetricEntityStateOneEnum<VeniceRecordTransformerOperation>> latencyPerStore =
      new VeniceConcurrentHashMap<>();

  /**
   * Per-store OTel metric state for error count. Same bounding and lifecycle as latencyPerStore.
   */
  private final Map<String, MetricEntityStateOneEnum<VeniceRecordTransformerOperation>> errorCountPerStore =
      new VeniceConcurrentHashMap<>();

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
      latencyPerStore.remove(storeName);
      errorCountPerStore.remove(storeName);
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
    latencyPerStore.computeIfAbsent(storeName, k -> createPerStoreMetric(k, RECORD_TRANSFORMER_LATENCY))
        .record(value, operation);
  }

  private void recordOtelErrorCount(String storeName, VeniceRecordTransformerOperation operation) {
    if (!emitOtelMetrics) {
      return;
    }
    errorCountPerStore.computeIfAbsent(storeName, k -> createPerStoreMetric(k, RECORD_TRANSFORMER_ERROR_COUNT))
        .record(1, operation);
  }

  @VisibleForTesting
  boolean hasLatencyMetricFor(String storeName) {
    return latencyPerStore.containsKey(storeName);
  }

  @VisibleForTesting
  boolean hasErrorCountMetricFor(String storeName) {
    return errorCountPerStore.containsKey(storeName);
  }

  @VisibleForTesting
  int latencyStoreCount() {
    return latencyPerStore.size();
  }

  @VisibleForTesting
  int errorCountStoreCount() {
    return errorCountPerStore.size();
  }

  private MetricEntityStateOneEnum<VeniceRecordTransformerOperation> createPerStoreMetric(
      String storeName,
      DaVinciRecordTransformerOtelMetricEntity metricEntity) {
    Map<VeniceMetricsDimensions, String> storeDimensionsMap = new HashMap<>(baseDimensionsMap);
    storeDimensionsMap
        .put(VeniceMetricsDimensions.VENICE_STORE_NAME, OpenTelemetryMetricsSetup.sanitizeStoreName(storeName));
    return MetricEntityStateOneEnum.create(
        metricEntity.getMetricEntity(),
        otelRepository,
        storeDimensionsMap,
        VeniceRecordTransformerOperation.class);
  }
}
