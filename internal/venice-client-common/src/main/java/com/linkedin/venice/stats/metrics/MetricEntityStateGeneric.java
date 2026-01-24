package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Provides a flexible, generic, and non-caching implementation of {@link MetricEntityState} for one or more
 * dynamic dimensions, where the dimensions can be enums or arbitrary strings and do not need to be cached: This
 * implementation should be used only in certain control-path components (such as controllers) where:
 * 1. Performance constraints are lenient, and metric recording is infrequent.
 * 2. The metric entity requires an arbitrary number of dynamic dimensions without predefined enums: For example,
 *    store name, cluster name, etc. Do not use this for cases with 0 dynamic dimensions and use
 *    {@link MetricEntityStateBase} instead.
 * This approach also helps reduce code complexity and avoids the proliferation of specialized subclasses (like
 * MetricEntityStateOneEnum) for different dynamic dimension combinations when attribute caching is not necessary.
 *
 * Compared to enum-based subclasses, this class provides less compile-time type safety, as it does not enforce
 * dynamic dimensions to be enums and does not require explicit dimension types during instantiation.
 */
public class MetricEntityStateGeneric extends MetricEntityState {
  private MetricEntityStateGeneric(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    this(metricEntity, otelRepository, null, null, Collections.EMPTY_LIST, baseDimensionsMap);
  }

  private MetricEntityStateGeneric(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats);
    validateMetricType(metricEntity);
    validateRequiredDimensions(metricEntity, baseDimensionsMap);
  }

  /**
   * MetricEntityStateGeneric does not support async counter types because it doesn't cache
   * MetricAttributesData and cannot provide the getAllMetricAttributesData() iteration required for
   * observable counter reporting. Use one of the enum-based MetricEntityState subclasses for
   * ASYNC_COUNTER_FOR_HIGH_PERF_CASES or ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES metrics.
   */
  private void validateMetricType(MetricEntity metricEntity) {
    MetricType metricType = metricEntity.getMetricType();
    if (metricType.isObservableCounterType()) {
      throw new IllegalArgumentException(
          "MetricEntityStateGeneric does not support " + metricType + " metric type. "
              + "Use MetricEntityStateOneEnum, MetricEntityStateTwoEnums, etc. for metric: "
              + metricEntity.getMetricName());
    }
  }

  /** Factory method to keep the API consistent with other subclasses like {@link MetricEntityStateOneEnum} */
  public static MetricEntityStateGeneric create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    return new MetricEntityStateGeneric(metricEntity, otelRepository, baseDimensionsMap);
  }

  /** Overloaded Factory method for constructor with Tehuti parameters */
  public static MetricEntityStateGeneric create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    return new MetricEntityStateGeneric(
        metricEntity,
        otelRepository,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        baseDimensionsMap);
  }

  /**
   * Validates only the base dimensions upfront, while validation of additional dimensions
   * occurs later during {@link #getAttributes}.
   */
  private void validateRequiredDimensions(
      MetricEntity metricEntity,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    if (baseDimensionsMap != null) {
      Set<VeniceMetricsDimensions> requiredDimensionsList = metricEntity.getDimensionsList();
      // check if all required dimensions are present in baseDimensionsMap itself
      if (baseDimensionsMap.keySet().size() >= requiredDimensionsList.size()) {
        // if the baseDimensionsMap has all dimensions, MetricEntityStateBase should be used instead
        throw new IllegalArgumentException(
            "baseDimensionsMap " + baseDimensionsMap.keySet() + " contains all or more dimensions than required "
                + requiredDimensionsList + " for metric: " + metricEntity.getMetricName());
      }

      for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
        VeniceMetricsDimensions dimension = entry.getKey();
        String value = entry.getValue();

        if (value == null || value.isEmpty()) {
          throw new IllegalArgumentException(
              "baseDimensionsMap " + baseDimensionsMap.keySet() + " contains a null or empty value for dimension "
                  + dimension + " for metric: " + metricEntity.getMetricName());
        }

        // check if all dimensions in baseDimensionsMap are valid dimensions
        if (!requiredDimensionsList.contains(dimension)) {
          throw new IllegalArgumentException(
              "baseDimensionsMap " + baseDimensionsMap.keySet() + " contains invalid dimension " + dimension
                  + " for metric: " + metricEntity.getMetricName());
        }
      }
    }
  }

  /**
   * Validates whether the provided dimension list matches the dimensions defined by the MetricEntity.
   * This method performs additional runtime checks compared to other {@link MetricEntityState} subclasses
   * to ensure that the supplied dimensions are appropriate for the metric entity.
   */
  Attributes getAttributes(Map<VeniceMetricsDimensions, String> dimensions) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    // validate whether the input dimension list + baseDimensionsMap is complete
    Set<VeniceMetricsDimensions> inputDimensions = new HashSet<>(dimensions.keySet());
    inputDimensions.addAll(getBaseDimensionsMap().keySet());
    Set<VeniceMetricsDimensions> requiredDimensions = getMetricEntity().getDimensionsList();
    if (!requiredDimensions.equals(inputDimensions)) {
      throw new IllegalArgumentException(
          "Input dimensions " + inputDimensions + " doesn't match with the required dimensions " + requiredDimensions
              + " for metric: " + getMetricEntity().getMetricName());
    }
    Attributes attributes = createAttributes(dimensions);

    if (attributes == null) {
      throw new IllegalArgumentException(
          "No Attributes created for dimensions: " + dimensions + " for metric Entity: "
              + getMetricEntity().getMetricName());
    }
    return attributes;
  }

  public void record(long value, @Nonnull Map<VeniceMetricsDimensions, String> dimensions) {
    record((double) value, dimensions);
  }

  public void record(double value, @Nonnull Map<VeniceMetricsDimensions, String> dimensions) {
    try {
      super.record(value, getAttributes(dimensions));
    } catch (IllegalArgumentException e) {
      getOtelRepository().recordFailureMetric(getMetricEntity(), e);
    }
  }

  @Override
  /**
   * MetricEntityStateGeneric does not support ASYNC_COUNTER_FOR_HIGH_PERF_CASES because it doesn't cache
   * MetricAttributesData and cannot provide the getAllMetricAttributesData() iteration required for
   * observable counter reporting. Use one of the enum-based MetricEntityState subclasses for
   * ASYNC_COUNTER_FOR_HIGH_PERF_CASES metrics.
   */
  protected Iterable<MetricAttributesData> getAllMetricAttributesData() {
    return null;
  }
}
