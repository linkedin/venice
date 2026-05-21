package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDrainerType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;


public class StoreBufferServiceStats extends AbstractVeniceStats {
  enum TehutiMetricName implements TehutiMetricNameEnum {
    TOTAL_MEMORY_USAGE, TOTAL_REMAINING_MEMORY, MAX_MEMORY_USAGE_PER_WRITER, MIN_MEMORY_USAGE_PER_WRITER,
    INTERNAL_PROCESSING_LATENCY, INTERNAL_PROCESSING_ERROR;
  }

  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final Attributes baseAttributes;

  private final AsyncMetricEntityStateBase memoryUsedMetric;
  private final AsyncMetricEntityStateBase memoryRemainingMetric;
  private final AsyncMetricEntityStateBase memoryUsedPerWriterMaxMetric;
  private final AsyncMetricEntityStateBase memoryUsedPerWriterMinMetric;

  /**
   * Per-store latency metric states. Bounded by the number of active stores on this server (typically < 100).
   * All stores share a single Tehuti sensor (registered once via {@code registerSensorIfAbsent}); per-store
   * structure exists for OTel dimensions only. Entries are never evicted; bounded cardinality makes this safe.
   */
  private final VeniceConcurrentHashMap<String, MetricEntityStateBase> latencyPerStore =
      new VeniceConcurrentHashMap<>();

  /**
   * Per-store error metric states. Same bounding and lifecycle as {@link #latencyPerStore}.
   */
  private final VeniceConcurrentHashMap<String, MetricEntityStateBase> errorPerStore = new VeniceConcurrentHashMap<>();

  public StoreBufferServiceStats(
      MetricsRepository metricsRepository,
      String metricNamePrefix,
      String clusterName,
      boolean sorted,
      LongSupplier totalMemoryUsageSupplier,
      LongSupplier totalRemainingMemorySupplier,
      LongSupplier maxMemoryUsagePerDrainerSupplier,
      LongSupplier minMemoryUsagePerDrainerSupplier) {
    super(metricsRepository, metricNamePrefix);

    VeniceDrainerType bufferType = sorted ? VeniceDrainerType.SORTED : VeniceDrainerType.UNSORTED;
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setClusterName(clusterName)
            .addCustomDimension(bufferType)
            .build();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
    // All 4 memory metrics share the same dimension set {CLUSTER_NAME, STORE_BUFFER_SERVICE_TYPE},
    // so baseAttributes built from any one of them is valid for all four.
    this.baseAttributes = otelData.getBaseAttributes();

    // Memory metrics (#1-4): joint Tehuti+OTel AsyncGauge.
    memoryUsedMetric = registerMemoryGauge(
        StoreBufferServiceOtelMetricEntity.MEMORY_USED,
        TehutiMetricName.TOTAL_MEMORY_USAGE,
        totalMemoryUsageSupplier);
    memoryRemainingMetric = registerMemoryGauge(
        StoreBufferServiceOtelMetricEntity.MEMORY_REMAINING,
        TehutiMetricName.TOTAL_REMAINING_MEMORY,
        totalRemainingMemorySupplier);
    memoryUsedPerWriterMaxMetric = registerMemoryGauge(
        StoreBufferServiceOtelMetricEntity.MEMORY_USED_PER_WRITER_MAX,
        TehutiMetricName.MAX_MEMORY_USAGE_PER_WRITER,
        maxMemoryUsagePerDrainerSupplier);
    memoryUsedPerWriterMinMetric = registerMemoryGauge(
        StoreBufferServiceOtelMetricEntity.MEMORY_USED_PER_WRITER_MIN,
        TehutiMetricName.MIN_MEMORY_USAGE_PER_WRITER,
        minMemoryUsagePerDrainerSupplier);
  }

  private AsyncMetricEntityStateBase registerMemoryGauge(
      StoreBufferServiceOtelMetricEntity metricEntity,
      TehutiMetricName tehutiName,
      LongSupplier supplier) {
    return AsyncMetricEntityStateBase.create(
        metricEntity.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        tehutiName,
        Collections.singletonList(new AsyncGauge((ig, ig2) -> supplier.getAsLong(), tehutiName.getMetricName())),
        baseDimensionsMap,
        baseAttributes,
        supplier,
        resources);
  }

  private MetricEntityStateBase createPerStoreState(
      String storeName,
      MetricEntity metricEntity,
      TehutiMetricNameEnum tehutiName,
      List<MeasurableStat> tehutiStats) {
    Map<VeniceMetricsDimensions, String> dims =
        OpenTelemetryMetricsSetup.buildStoreDimensionsMap(baseDimensionsMap, storeName);
    Attributes attrs = otelRepository != null ? otelRepository.createAttributes(metricEntity, dims) : null;
    return MetricEntityStateBase.create(
        metricEntity,
        otelRepository,
        this::registerSensorIfAbsent,
        tehutiName,
        tehutiStats,
        dims,
        attrs,
        resources);
  }

  private MetricEntityStateBase getOrCreateLatencyState(String storeName) {
    return latencyPerStore.computeIfAbsent(
        storeName,
        k -> createPerStoreState(
            k,
            StoreBufferServiceOtelMetricEntity.PROCESSING_TIME.getMetricEntity(),
            TehutiMetricName.INTERNAL_PROCESSING_LATENCY,
            Arrays.asList(new Avg(), new Max())));
  }

  private MetricEntityStateBase getOrCreateErrorState(String storeName) {
    return errorPerStore.computeIfAbsent(
        storeName,
        k -> createPerStoreState(
            k,
            StoreBufferServiceOtelMetricEntity.PROCESSING_ERROR_COUNT.getMetricEntity(),
            TehutiMetricName.INTERNAL_PROCESSING_ERROR,
            Collections.singletonList(new OccurrenceRate())));
  }

  public void recordInternalProcessingLatency(long latency, String storeName) {
    getOrCreateLatencyState(storeName).record(latency);
  }

  public void recordInternalProcessingError(String storeName) {
    getOrCreateErrorState(storeName).record(1);
  }

  /** Closes all OTel metric wrappers held by this stats instance, including per-store entries. */
  @Override
  public void close() {
    super.close();
    latencyPerStore.clear();
    errorPerStore.clear();
  }
}
