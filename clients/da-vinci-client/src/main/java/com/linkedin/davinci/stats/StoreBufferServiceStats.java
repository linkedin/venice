package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceStoreBufferServiceType;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;


public class StoreBufferServiceStats extends AbstractVeniceStats {
  enum TehutiMetricName implements TehutiMetricNameEnum {
    TOTAL_MEMORY_USAGE, TOTAL_REMAINING_MEMORY, MAX_MEMORY_USAGE_PER_WRITER, MIN_MEMORY_USAGE_PER_WRITER,
    INTERNAL_PROCESSING_LATENCY, INTERNAL_PROCESSING_ERROR;
  }

  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

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

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    this.otelRepository = otelData.getOtelRepository();
    if (otelRepository != null && (clusterName == null || clusterName.isEmpty())) {
      throw new IllegalArgumentException("clusterName must be non-null and non-empty when OTel metrics are enabled");
    }

    // Base dimensions: cluster + buffer type (shared by all metrics; per-store metrics add VENICE_STORE_NAME)
    Map<VeniceMetricsDimensions, String> dimMap = new HashMap<>(otelData.getBaseDimensionsMap());
    VeniceStoreBufferServiceType bufferType =
        sorted ? VeniceStoreBufferServiceType.SORTED : VeniceStoreBufferServiceType.UNSORTED;
    dimMap.put(VeniceMetricsDimensions.VENICE_STORE_BUFFER_SERVICE_TYPE, bufferType.getDimensionValue());
    this.baseDimensionsMap = Collections.unmodifiableMap(dimMap);
    // All 4 memory metrics share the same dimension set {CLUSTER_NAME, STORE_BUFFER_SERVICE_TYPE},
    // so baseAttributes built from any one of them is valid for all four.
    Attributes baseAttributes = otelRepository != null
        ? otelRepository
            .createAttributes(StoreBufferServiceOtelMetricEntity.MEMORY_USED.getMetricEntity(), baseDimensionsMap)
        : null;

    // Memory metrics (#1-4): joint Tehuti+OTel AsyncGauge
    AsyncMetricEntityStateBase.create(
        StoreBufferServiceOtelMetricEntity.MEMORY_USED.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.TOTAL_MEMORY_USAGE,
        Collections
            .singletonList(new AsyncGauge((ig, ig2) -> totalMemoryUsageSupplier.getAsLong(), "total_memory_usage")),
        baseDimensionsMap,
        baseAttributes,
        totalMemoryUsageSupplier);

    AsyncMetricEntityStateBase.create(
        StoreBufferServiceOtelMetricEntity.MEMORY_REMAINING.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.TOTAL_REMAINING_MEMORY,
        Collections.singletonList(
            new AsyncGauge((ig, ig2) -> totalRemainingMemorySupplier.getAsLong(), "total_remaining_memory")),
        baseDimensionsMap,
        baseAttributes,
        totalRemainingMemorySupplier);

    AsyncMetricEntityStateBase.create(
        StoreBufferServiceOtelMetricEntity.MEMORY_USED_PER_WRITER_MAX.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.MAX_MEMORY_USAGE_PER_WRITER,
        Collections.singletonList(
            new AsyncGauge((ig, ig2) -> maxMemoryUsagePerDrainerSupplier.getAsLong(), "max_memory_usage_per_writer")),
        baseDimensionsMap,
        baseAttributes,
        maxMemoryUsagePerDrainerSupplier);

    AsyncMetricEntityStateBase.create(
        StoreBufferServiceOtelMetricEntity.MEMORY_USED_PER_WRITER_MIN.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.MIN_MEMORY_USAGE_PER_WRITER,
        Collections.singletonList(
            new AsyncGauge((ig, ig2) -> minMemoryUsagePerDrainerSupplier.getAsLong(), "min_memory_usage_per_writer")),
        baseDimensionsMap,
        baseAttributes,
        minMemoryUsagePerDrainerSupplier);
  }

  private MetricEntityStateBase getOrCreateLatencyState(String storeName) {
    return latencyPerStore.computeIfAbsent(storeName, k -> {
      Map<VeniceMetricsDimensions, String> dims = new HashMap<>(baseDimensionsMap);
      dims.put(VeniceMetricsDimensions.VENICE_STORE_NAME, k);
      dims = Collections.unmodifiableMap(dims);
      Attributes attrs = otelRepository != null
          ? otelRepository.createAttributes(StoreBufferServiceOtelMetricEntity.PROCESSING_TIME.getMetricEntity(), dims)
          : null;
      return MetricEntityStateBase.create(
          StoreBufferServiceOtelMetricEntity.PROCESSING_TIME.getMetricEntity(),
          otelRepository,
          this::registerSensorIfAbsent,
          TehutiMetricName.INTERNAL_PROCESSING_LATENCY,
          Arrays.asList(new Avg(), new Max()),
          dims,
          attrs);
    });
  }

  private MetricEntityStateBase getOrCreateErrorState(String storeName) {
    return errorPerStore.computeIfAbsent(storeName, k -> {
      Map<VeniceMetricsDimensions, String> dims = new HashMap<>(baseDimensionsMap);
      dims.put(VeniceMetricsDimensions.VENICE_STORE_NAME, k);
      dims = Collections.unmodifiableMap(dims);
      Attributes attrs = otelRepository != null
          ? otelRepository
              .createAttributes(StoreBufferServiceOtelMetricEntity.PROCESSING_ERROR_COUNT.getMetricEntity(), dims)
          : null;
      return MetricEntityStateBase.create(
          StoreBufferServiceOtelMetricEntity.PROCESSING_ERROR_COUNT.getMetricEntity(),
          otelRepository,
          this::registerSensorIfAbsent,
          TehutiMetricName.INTERNAL_PROCESSING_ERROR,
          Collections.singletonList(new OccurrenceRate()),
          dims,
          attrs);
    });
  }

  public void recordInternalProcessingLatency(long latency, String storeName) {
    getOrCreateLatencyState(OpenTelemetryMetricsSetup.sanitizeStoreName(storeName)).record(latency);
  }

  public void recordInternalProcessingError(String storeName) {
    getOrCreateErrorState(OpenTelemetryMetricsSetup.sanitizeStoreName(storeName)).record(1);
  }

  /** Convenience overload for callers without store context; records under {@code "unknown_store"}. */
  public void recordInternalProcessingLatency(long latency) {
    recordInternalProcessingLatency(latency, OpenTelemetryMetricsSetup.UNKNOWN_STORE_NAME);
  }

  /** Convenience overload for callers without store context; records under {@code "unknown_store"}. */
  public void recordInternalProcessingError() {
    recordInternalProcessingError(OpenTelemetryMetricsSetup.UNKNOWN_STORE_NAME);
  }
}
