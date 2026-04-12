package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.DaVinciRecordTransformerOtelMetricEntity.RECORD_TRANSFORMER_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerOtelMetricEntity.RECORD_TRANSFORMER_LATENCY;

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

  /**
   * Per-store OTel metric state for latency. Bounded by the number of stores on this host.
   * Entries created lazily via {@link #getOrCreateLatencyMetric}, removed in
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
    getOrCreateLatencyMetric(storeName).record(value, VeniceRecordTransformerOperation.PUT);
  }

  public void recordDeleteLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordDeleteLatency(value, timestamp));
    getOrCreateLatencyMetric(storeName).record(value, VeniceRecordTransformerOperation.DELETE);
  }

  public void recordPutError(String storeName, int version, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordPutError(timestamp));
    getOrCreateErrorCountMetric(storeName).record(1, VeniceRecordTransformerOperation.PUT);
  }

  public void recordDeleteError(String storeName, int version, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordDeleteError(timestamp));
    getOrCreateErrorCountMetric(storeName).record(1, VeniceRecordTransformerOperation.DELETE);
  }

  private MetricEntityStateOneEnum<VeniceRecordTransformerOperation> getOrCreateLatencyMetric(String storeName) {
    return latencyPerStore.computeIfAbsent(storeName, k -> createPerStoreMetric(k, RECORD_TRANSFORMER_LATENCY));
  }

  private MetricEntityStateOneEnum<VeniceRecordTransformerOperation> getOrCreateErrorCountMetric(String storeName) {
    return errorCountPerStore.computeIfAbsent(storeName, k -> createPerStoreMetric(k, RECORD_TRANSFORMER_ERROR_COUNT));
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
