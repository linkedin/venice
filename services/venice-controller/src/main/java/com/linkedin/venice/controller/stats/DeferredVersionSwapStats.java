package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Gauge;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


public class DeferredVersionSwapStats extends AbstractVeniceStats {
  private final MetricEntityStateGeneric deferredVersionSwapThrowableMetric;
  private final MetricEntityStateGeneric deferredVersionSwapExceptionMetric;

  private final MetricEntityStateGeneric deferredVersionSwapFailedRollForwardMetric;
  private final MetricEntityStateBase deferredVersionSwapStalledVersionSwapMetric;
  private final MetricEntityStateGeneric deferredVersionSwapParentChildStatusMismatchMetric;
  private final MetricEntityStateGeneric deferredVersionSwapChildStatusMismatchMetric;

  public DeferredVersionSwapStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "DeferredVersionSwap");

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    deferredVersionSwapExceptionMetric = MetricEntityStateGeneric.create(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_PROCESSING_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_ERROR,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    deferredVersionSwapThrowableMetric = MetricEntityStateGeneric.create(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_PROCESSING_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_THROWABLE,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    deferredVersionSwapFailedRollForwardMetric = MetricEntityStateGeneric.create(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_ROLL_FORWARD_FAILURE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_FAILED_ROLL_FORWARD,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    deferredVersionSwapStalledVersionSwapMetric = MetricEntityStateBase.create(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_STALLED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_STALLED_VERSION_SWAP,
        Collections.singletonList(new Gauge()),
        baseDimensionsMap,
        baseAttributes);

    deferredVersionSwapParentChildStatusMismatchMetric = MetricEntityStateGeneric.create(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_PARENT_STATUS_MISMATCH_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_PARENT_CHILD_STATUS_MISMATCH,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    deferredVersionSwapChildStatusMismatchMetric = MetricEntityStateGeneric.create(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH,
        Collections.singletonList(new Count()),
        baseDimensionsMap);
  }

  public void recordDeferredVersionSwapExceptionMetric(String clusterName) {
    deferredVersionSwapExceptionMetric.record(1, clusterDimensions(clusterName));
  }

  public void recordDeferredVersionSwapThrowableMetric(String clusterName) {
    deferredVersionSwapThrowableMetric.record(1, clusterDimensions(clusterName));
  }

  public void recordDeferredVersionSwapFailedRollForwardMetric(String clusterName, String storeName) {
    deferredVersionSwapFailedRollForwardMetric.record(1, clusterAndStoreDimensions(clusterName, storeName));
  }

  public void recordDeferredVersionSwapStalledVersionSwapMetric(double value) {
    deferredVersionSwapStalledVersionSwapMetric.record(value);
  }

  public void recordDeferredVersionSwapParentChildStatusMismatchMetric(String clusterName, String storeName) {
    deferredVersionSwapParentChildStatusMismatchMetric.record(1, clusterAndStoreDimensions(clusterName, storeName));
  }

  public void recordDeferredVersionSwapChildStatusMismatchMetric(String clusterName, String storeName) {
    deferredVersionSwapChildStatusMismatchMetric.record(1, clusterAndStoreDimensions(clusterName, storeName));
  }

  private static Map<VeniceMetricsDimensions, String> clusterDimensions(String clusterName) {
    return Collections.singletonMap(VENICE_CLUSTER_NAME, clusterName);
  }

  private static Map<VeniceMetricsDimensions, String> clusterAndStoreDimensions(String clusterName, String storeName) {
    return ImmutableMap.of(VENICE_CLUSTER_NAME, clusterName, VENICE_STORE_NAME, storeName);
  }

  enum DeferredVersionSwapTehutiMetricNameEnum implements TehutiMetricNameEnum {
    DEFERRED_VERSION_SWAP_ERROR, DEFERRED_VERSION_SWAP_THROWABLE, DEFERRED_VERSION_SWAP_FAILED_ROLL_FORWARD,
    DEFERRED_VERSION_SWAP_STALLED_VERSION_SWAP, DEFERRED_VERSION_SWAP_PARENT_CHILD_STATUS_MISMATCH,
    DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH
  }

  public enum DeferredVersionSwapOtelMetricEntity implements ModuleMetricEntityInterface {
    /** Count of unexpected failures (both {@link Exception} and {@link Throwable}) in the per-cluster processing loop */
    DEFERRED_VERSION_SWAP_PROCESSING_ERROR_COUNT(
        "deferred_version_swap.processing_error_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of unexpected failures in the deferred version swap processing loop", setOf(VENICE_CLUSTER_NAME)
    ),
    /** Count of deferred version swap roll forward failures */
    DEFERRED_VERSION_SWAP_ROLL_FORWARD_FAILURE_COUNT(
        "deferred_version_swap.roll_forward.failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of deferred version swap roll forward failures", setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    ),
    /** Gauge of stalled deferred version swaps (global — stalledVersionSwapSet is shared across all clusters) */
    DEFERRED_VERSION_SWAP_STALLED_COUNT(
        MetricEntity.createWithNoDimensions(
            "deferred_version_swap.stalled_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Count of stalled deferred version swaps across all clusters")
    ),
    /** Count of deferred version swap parent-child status mismatches */
    DEFERRED_VERSION_SWAP_PARENT_STATUS_MISMATCH_COUNT(
        "deferred_version_swap.parent_status_mismatch_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of deferred version swap parent-child status mismatches", setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    ),
    /** Count of deferred version swap child status mismatches */
    DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH_COUNT(
        "deferred_version_swap.child_status_mismatch_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of deferred version swap child status mismatches", setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    );

    private final MetricEntity metricEntity;

    DeferredVersionSwapOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    DeferredVersionSwapOtelMetricEntity(MetricEntity metricEntity) {
      this.metricEntity = metricEntity;
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
