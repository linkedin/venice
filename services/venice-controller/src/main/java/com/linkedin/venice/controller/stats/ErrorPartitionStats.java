package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.stats.ControllerStatsDimensionUtils.dimensionMapBuilder;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

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
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Total;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;


public class ErrorPartitionStats extends AbstractVeniceStats {
  // Store-scoped metrics (cluster + store dimensions)
  private final MetricEntityStateGeneric resetAttemptMetric;
  private final MetricEntityStateGeneric resetErrorMetric;
  private final MetricEntityStateGeneric recoveredMetric;
  private final MetricEntityStateGeneric unrecoverableMetric;

  // Cluster-only metrics
  private final MetricEntityStateBase processingErrorMetric;
  private final MetricEntityStateBase processingTimeMetric;

  public ErrorPartitionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(name).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    resetAttemptMetric = MetricEntityStateGeneric.create(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_ATTEMPT_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT,
        Collections.singletonList(new Total()),
        baseDimensionsMap);

    resetErrorMetric = MetricEntityStateGeneric.create(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT_ERRORED,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    recoveredMetric = MetricEntityStateGeneric.create(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_RECOVERED_PARTITION_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RECOVERED_FROM_RESET,
        Collections.singletonList(new Total()),
        baseDimensionsMap);

    unrecoverableMetric = MetricEntityStateGeneric.create(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_UNRECOVERABLE_PARTITION_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_UNRECOVERABLE_FROM_RESET,
        Collections.singletonList(new Total()),
        baseDimensionsMap);

    processingErrorMetric = MetricEntityStateBase.create(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_PROCESSING_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        ErrorPartitionTehutiMetricNameEnum.ERROR_PARTITION_PROCESSING_ERROR,
        Collections.singletonList(new Count()),
        baseDimensionsMap,
        baseAttributes);

    processingTimeMetric = MetricEntityStateBase.create(
        ErrorPartitionOtelMetricEntity.ERROR_PARTITION_PROCESSING_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        ErrorPartitionTehutiMetricNameEnum.ERROR_PARTITION_PROCESSING_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);
  }

  public void recordErrorPartitionResetAttempt(double value, String storeName) {
    resetAttemptMetric.record(value, dimensionMapBuilder().store(storeName).build());
  }

  public void recordErrorPartitionResetAttemptErrored(String storeName) {
    resetErrorMetric.record(1, dimensionMapBuilder().store(storeName).build());
  }

  public void recordErrorPartitionProcessingError() {
    processingErrorMetric.record(1);
  }

  public void recordErrorPartitionRecoveredFromReset(String storeName) {
    recoveredMetric.record(1, dimensionMapBuilder().store(storeName).build());
  }

  public void recordErrorPartitionUnrecoverableFromReset(String storeName) {
    unrecoverableMetric.record(1, dimensionMapBuilder().store(storeName).build());
  }

  public void recordErrorPartitionProcessingTime(double value) {
    processingTimeMetric.record(value);
  }

  enum ErrorPartitionTehutiMetricNameEnum implements TehutiMetricNameEnum {
    CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT, CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT_ERRORED,
    CURRENT_VERSION_ERROR_PARTITION_RECOVERED_FROM_RESET, CURRENT_VERSION_ERROR_PARTITION_UNRECOVERABLE_FROM_RESET,
    ERROR_PARTITION_PROCESSING_ERROR, ERROR_PARTITION_PROCESSING_TIME
  }

  public enum ErrorPartitionOtelMetricEntity implements ModuleMetricEntityInterface {
    ERROR_PARTITION_RESET_ATTEMPT_COUNT(
        new MetricEntity(
            "partition.error_partition.reset.attempt_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Total partitions reset across all operations",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME))
    ),
    ERROR_PARTITION_RESET_ERROR_COUNT(
        new MetricEntity(
            "partition.error_partition.reset.error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Failed reset operations for a store",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME))
    ),
    ERROR_PARTITION_RESET_RECOVERED_PARTITION_COUNT(
        new MetricEntity(
            "partition.error_partition.reset.recovered_partition_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Partitions recovered after reset",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME))
    ),
    ERROR_PARTITION_RESET_UNRECOVERABLE_PARTITION_COUNT(
        new MetricEntity(
            "partition.error_partition.reset.unrecoverable_partition_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Partitions declared unrecoverable after hitting reset limit",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME))
    ),
    ERROR_PARTITION_PROCESSING_ERROR_COUNT(
        new MetricEntity(
            "partition.error_partition.processing.error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Unexpected failures in the error partition processing cycle",
            setOf(VENICE_CLUSTER_NAME))
    ),
    ERROR_PARTITION_PROCESSING_TIME(
        new MetricEntity(
            "partition.error_partition.processing.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time for each complete error partition processing cycle",
            setOf(VENICE_CLUSTER_NAME))
    );

    private final MetricEntity metricEntity;

    ErrorPartitionOtelMetricEntity(MetricEntity metricEntity) {
      this.metricEntity = metricEntity;
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
