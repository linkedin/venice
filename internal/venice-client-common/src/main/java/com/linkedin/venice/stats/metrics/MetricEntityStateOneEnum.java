package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.REDUNDANT_LOG_FILTER;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MetricEntityStateOneEnum<E extends Enum<E> & VeniceDimensionInterface> extends MetricEntityState {
  private final EnumMap<E, Attributes> attributesEnumMap;
  private final Class<E> enumTypeClass;

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateOneEnum(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass) {
    super(metricEntity, otelRepository);
    validateRequiredDimensions(metricEntity, baseDimensionsMap, enumTypeClass);
    this.enumTypeClass = enumTypeClass;
    this.attributesEnumMap = new EnumMap<>(enumTypeClass);
    if (emitOpenTelemetryMetrics()) {
      createAttributesEnumMap(metricEntity, otelRepository, baseDimensionsMap);
    }
  }

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateOneEnum(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass) {
    super(metricEntity, otelRepository, registerTehutiSensorFn, tehutiMetricNameEnum, tehutiMetricStats);
    validateRequiredDimensions(metricEntity, baseDimensionsMap, enumTypeClass);
    this.enumTypeClass = enumTypeClass;
    this.attributesEnumMap = new EnumMap<>(enumTypeClass);
    if (otelRepository != null) {
      createAttributesEnumMap(metricEntity, otelRepository, baseDimensionsMap);
    }
  }

  /** Factory method with named parameters to ensure the passed in enumTypeClass are in the same order as E */
  public static <E extends Enum<E> & VeniceDimensionInterface> MetricEntityStateOneEnum<E> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass) {
    return new MetricEntityStateOneEnum<>(metricEntity, otelRepository, baseDimensionsMap, enumTypeClass);
  }

  /** Overloaded Factory method for constructor with Tehuti parameters */
  public static <E extends Enum<E> & VeniceDimensionInterface> MetricEntityStateOneEnum<E> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass) {
    return new MetricEntityStateOneEnum<>(
        metricEntity,
        otelRepository,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        baseDimensionsMap,
        enumTypeClass);
  }

  private void createAttributesEnumMap(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    Map<VeniceMetricsDimensions, String> additionalDimensionsMap = new HashMap<>();
    for (E enumValue: enumTypeClass.getEnumConstants()) {
      additionalDimensionsMap.put(enumValue.getDimensionName(), enumValue.getDimensionValue());
      attributesEnumMap
          .put(enumValue, otelRepository.createAttributes(metricEntity, baseDimensionsMap, additionalDimensionsMap));
    }
    if (attributesEnumMap.isEmpty()) {
      throw new VeniceException(
          "The dimensions map is empty. Please check the enum types and ensure they are properly defined.");
    }
  }

  Attributes getAttributes(E key) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    if (key == null) {
      throw new IllegalArgumentException(
          "The key for otel dimension cannot be null for metric Entity: " + getMetricEntity().getMetricName());
    }
    if (!enumTypeClass.isInstance(key)) {
      // defensive check: This only happens if the instance is declared without the explicit types
      throw new IllegalArgumentException(
          "The key for otel dimension is not of the correct type: " + key.getClass() + " for metric Entity: "
              + getMetricEntity().getMetricName());
    }
    return attributesEnumMap.get(key);
  }

  public void record(long value, E key) {
    try {
      super.record(value, getAttributes(key));
    } catch (IllegalArgumentException e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }

  public void record(double value, E key) {
    try {
      super.record(value, getAttributes(key));
    } catch (IllegalArgumentException e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }

  /** visible for testing */
  public EnumMap<E, Attributes> getAttributesEnumMap() {
    return attributesEnumMap;
  }

}
