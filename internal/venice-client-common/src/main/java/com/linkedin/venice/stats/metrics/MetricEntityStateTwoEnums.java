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
 * Similar to {@link MetricEntityStateOneEnum} but with two dynamic dimensions and 2 level EnumMap
 */
public class MetricEntityStateTwoEnums<E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface>
    extends MetricEntityState {
  private final EnumMap<E1, EnumMap<E2, Attributes>> attributesEnumMap;
  private final Class<E1> enumTypeClass1;
  private final Class<E2> enumTypeClass2;

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateTwoEnums(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2) {
    this(
        metricEntity,
        otelRepository,
        null,
        null,
        Collections.EMPTY_LIST,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2);
  }

  /** should not be called directly, call {@link #create} instead */
  public MetricEntityStateTwoEnums(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2) {
    super(metricEntity, otelRepository, registerTehutiSensorFn, tehutiMetricNameEnum, tehutiMetricStats);
    validateRequiredDimensions(metricEntity, null, baseDimensionsMap, enumTypeClass1, enumTypeClass2);
    this.enumTypeClass1 = enumTypeClass1;
    this.enumTypeClass2 = enumTypeClass2;
    this.attributesEnumMap = createAttributesEnumMap(metricEntity, otelRepository, baseDimensionsMap);
  }

  /** Factory method with named parameters to ensure the passed in enumTypeClass are in the same order as E */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface> MetricEntityStateTwoEnums<E1, E2> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2) {
    return new MetricEntityStateTwoEnums<>(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2);
  }

  /** Overloaded Factory method for constructor with Tehuti parameters */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface> MetricEntityStateTwoEnums<E1, E2> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2) {
    return new MetricEntityStateTwoEnums<>(
        metricEntity,
        otelRepository,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2);
  }

  /**
    * Creates an EnumMap of {@link Attributes} for each possible value of the dynamic dimensions
   * {@link #enumTypeClass1} and {@link #enumTypeClass2}
   */
  private EnumMap<E1, EnumMap<E2, Attributes>> createAttributesEnumMap(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    EnumMap<E1, EnumMap<E2, Attributes>> attributesEnumMap = new EnumMap<>(enumTypeClass1);
    Map<VeniceMetricsDimensions, String> additionalDimensionsMap = new HashMap<>();
    for (E1 enumConst1: enumTypeClass1.getEnumConstants()) {
      additionalDimensionsMap.put(enumConst1.getDimensionName(), enumConst1.getDimensionValue());
      EnumMap<E2, Attributes> mapE2 = new EnumMap<>(enumTypeClass2);
      attributesEnumMap.put(enumConst1, mapE2);
      for (E2 enumConst2: enumTypeClass2.getEnumConstants()) {
        additionalDimensionsMap.put(enumConst2.getDimensionName(), enumConst2.getDimensionValue());
        mapE2
            .put(enumConst2, otelRepository.createAttributes(metricEntity, baseDimensionsMap, additionalDimensionsMap));
      }
    }
    return attributesEnumMap;
  }

  Attributes getAttributes(E1 key1, E2 key2) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    if (key1 == null || key2 == null) {
      throw new IllegalArgumentException(
          "The key for otel dimension cannot be null for metric Entity: " + getMetricEntity().getMetricName());
    }
    if (!enumTypeClass1.isInstance(key1) || !enumTypeClass2.isInstance(key2)) {
      // defensive check: This can only happen if the instance is declared without the explicit types
      // and passed in wrong args
      throw new IllegalArgumentException(
          "The key for otel dimension is not of the correct type: " + key1.getClass() + "," + key2.getClass()
              + " for metric Entity: " + getMetricEntity().getMetricName());
    }

    Attributes attributes = null;
    EnumMap<E2, Attributes> mapE2 = attributesEnumMap.get(key1);
    if (mapE2 != null) {
      attributes = mapE2.get(key2);
    }

    if (attributes == null) {
      // defensive check: attributes for all entries of bounded enums should be pre created
      throw new IllegalArgumentException(
          "No dimensions found for keys: " + key1 + "," + key2 + " for metric Entity: "
              + getMetricEntity().getMetricName());
    }
    return attributes;
  }

  public void record(long value, E1 key1, E2 key2) {
    try {
      super.record(value, getAttributes(key1, key2));
    } catch (Exception e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }

  public void record(double value, E1 key1, E2 key2) {
    try {
      super.record(value, getAttributes(key1, key2));
    } catch (Exception e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }

  /** visible for testing */
  public EnumMap<E1, EnumMap<E2, Attributes>> getAttributesEnumMap() {
    return attributesEnumMap;
  }
}
