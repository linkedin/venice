package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.REDUNDANT_LOG_FILTER;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Provides a flexible, generic, and non-caching implementation of {@link MetricEntityState} for zero or more
 * dynamic dimensions, where the dimensions can be enums or arbitrary strings
 *
 * This implementation should be used only in certain control-path components (such as controllers) where:
 * 1. Performance constraints are lenient, and metric recording is infrequent.
 * 2. The metric entity requires an arbitrary number of dynamic dimensions without predefined enums.
 * This approach also helps reduce code complexity and avoids the proliferation of specialized subclasses (like
 * MetricEntityStateOneEnum or MetricEntityStateTwoEnums) for different dynamic dimension combinations when
 * attribute caching is not necessary.
 *
 * Compared to enum-based subclasses, this class provides less compile-time type safety, as it does not enforce
 * dynamic dimensions to be enums and does not require explicit dimension types during instantiation.
 */
public class MetricEntityStateGeneric extends MetricEntityState {
  public MetricEntityStateGeneric(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    this(metricEntity, otelRepository, null, null, Collections.EMPTY_LIST, baseDimensionsMap);
  }

  public MetricEntityStateGeneric(
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
    validateRequiredDimensions(metricEntity, null, baseDimensionsMap);
  }

  /**
   * Overrides {@link MetricEntityState#validateRequiredDimensions} with simplified logic suitable
   * for generic dimensions, which can be enums or arbitrary strings. Validates only the base dimensions
   * upfront, while validation of additional dimensions occurs later during {@link #getAttributes}.
   */
  @Override
  void validateRequiredDimensions(
      MetricEntity metricEntity,
      Attributes baseAttributes,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<?>... enumTypes) {
    if (baseDimensionsMap != null) {
      // check of all dimensions in baseDimensionsMap are valid dimensions
      Set<VeniceMetricsDimensions> dimensionsList = metricEntity.getDimensionsList();
      for (VeniceMetricsDimensions dimension: baseDimensionsMap.keySet()) {
        if (!dimensionsList.contains(dimension)) {
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
   * to ensure that the supplied dimensions are appropriate,
   * .
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

  public void record(long value, Map<VeniceMetricsDimensions, String> dimensions) {
    try {
      super.record(value, getAttributes(dimensions));
    } catch (IllegalArgumentException e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }

  public void record(double value, Map<VeniceMetricsDimensions, String> dimensions) {
    try {
      super.record(value, getAttributes(dimensions));
    } catch (IllegalArgumentException e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }
}
