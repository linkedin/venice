package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.REDUNDANT_LOG_FILTER;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This version of {@link MetricEntityState} is used when the metric entity has one dynamic dimension
 * which is an {@link Enum} implementing {@link VeniceDimensionInterface}.
 * The base dimensions that are common for all invocation of this instance are passed in the constructor
 * which is used along with all possible values for the dynamic dimensions to create an EnumMap of
 * {@link Attributes} for each possible value of the dynamic dimension. These attributes are used during
 * every record() call, the key to the EnumMap being the value of the dynamic dimension.
 */
public class MetricEntityStateOneEnum<E extends Enum<E> & VeniceDimensionInterface> extends MetricEntityState {
  private final EnumMap<E, Attributes> attributesEnumMap;
  private final Class<E> enumTypeClass;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateOneEnum(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass) {
    this(metricEntity, otelRepository, null, null, Collections.EMPTY_LIST, baseDimensionsMap, enumTypeClass);
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
    validateRequiredDimensions(metricEntity, null, baseDimensionsMap, enumTypeClass);
    this.enumTypeClass = enumTypeClass;
    this.baseDimensionsMap = baseDimensionsMap;
    this.attributesEnumMap = createAttributesEnumMap();
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

  private Map<VeniceMetricsDimensions, String> createAdditionalDimensionsMap(E key) {
    Map<VeniceMetricsDimensions, String> additionalDimensionsMap = new HashMap<>();
    additionalDimensionsMap.put(key.getDimensionName(), key.getDimensionValue());
    return additionalDimensionsMap;
  }

  private Attributes createAttributes(E key) {
    Map<VeniceMetricsDimensions, String> additionalDimensionsMap = createAdditionalDimensionsMap(key);
    return getOtelRepository().createAttributes(getMetricEntity(), baseDimensionsMap, additionalDimensionsMap);
  }

  /**
   * Creates an EnumMap of {@link Attributes} for each possible value of the dynamic dimension {@link #enumTypeClass}
   */
  private EnumMap<E, Attributes> createAttributesEnumMap() {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    EnumMap<E, Attributes> attributesEnumMap = new EnumMap<>(enumTypeClass);
    for (E enumValue: enumTypeClass.getEnumConstants()) {
      attributesEnumMap.put(enumValue, createAttributes(enumValue));
    }
    return attributesEnumMap;
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
      // defensive check: This can only happen if the instance is declared without the explicit types
      // and passed in wrong args
      throw new IllegalArgumentException(
          "The key for otel dimension is not of the correct type: " + key.getClass() + " for metric Entity: "
              + getMetricEntity().getMetricName());
    }

    Attributes attributes = attributesEnumMap.get(key);

    if (attributes == null) {
      // defensive check: attributes for all entries of bounded enums should be pre created
      throw new IllegalArgumentException(
          "No dimensions found for key: " + key + " for metric Entity: " + getMetricEntity().getMetricName());
    }
    return attributes;
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
