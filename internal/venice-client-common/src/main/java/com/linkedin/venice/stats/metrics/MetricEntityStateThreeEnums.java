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
 * Similar to {@link MetricEntityStateOneEnum} but with three dynamic dimensions and 3 level EnumMap
 */
public class MetricEntityStateThreeEnums<E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, E3 extends Enum<E3> & VeniceDimensionInterface>
    extends MetricEntityState {
  private final EnumMap<E1, EnumMap<E2, EnumMap<E3, Attributes>>> attributesEnumMap;

  private final Class<E1> enumTypeClass1;
  private final Class<E2> enumTypeClass2;
  private final Class<E3> enumTypeClass3;

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateThreeEnums(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3) {
    this(
        metricEntity,
        otelRepository,
        null,
        null,
        Collections.EMPTY_LIST,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2,
        enumTypeClass3);
  }

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateThreeEnums(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3) {
    super(metricEntity, otelRepository, registerTehutiSensorFn, tehutiMetricNameEnum, tehutiMetricStats);
    validateRequiredDimensions(metricEntity, baseDimensionsMap, enumTypeClass1, enumTypeClass2, enumTypeClass3);
    this.enumTypeClass1 = enumTypeClass1;
    this.enumTypeClass2 = enumTypeClass2;
    this.enumTypeClass3 = enumTypeClass3;
    this.attributesEnumMap = createAttributesEnumMap(metricEntity, otelRepository, baseDimensionsMap);
  }

  /** Factory method with named parameters to ensure the passed in enumTypeClass are in the same order as E */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, E3 extends Enum<E3> & VeniceDimensionInterface> MetricEntityStateThreeEnums<E1, E2, E3> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3) {
    return new MetricEntityStateThreeEnums<>(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2,
        enumTypeClass3);
  }

  /** Overloaded Factory method for constructor with Tehuti parameters */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, E3 extends Enum<E3> & VeniceDimensionInterface> MetricEntityStateThreeEnums<E1, E2, E3> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3) {
    return new MetricEntityStateThreeEnums<>(
        metricEntity,
        otelRepository,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2,
        enumTypeClass3);
  }

  /**
   * Creates an EnumMap of {@link Attributes} for each possible value of the dynamic dimensions {@link #enumTypeClass1},
   * {@link #enumTypeClass2} and {@link #enumTypeClass3}
   */
  private EnumMap<E1, EnumMap<E2, EnumMap<E3, Attributes>>> createAttributesEnumMap(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    EnumMap<E1, EnumMap<E2, EnumMap<E3, Attributes>>> attributesEnumMap = new EnumMap<>(enumTypeClass1);
    Map<VeniceMetricsDimensions, String> additionalDimensionsMap = new HashMap<>();
    for (E1 enumConst1: enumTypeClass1.getEnumConstants()) {
      additionalDimensionsMap.put(enumConst1.getDimensionName(), enumConst1.getDimensionValue());
      EnumMap<E2, EnumMap<E3, Attributes>> mapE2 = new EnumMap<>(enumTypeClass2);
      attributesEnumMap.put(enumConst1, mapE2);
      for (E2 enumConst2: enumTypeClass2.getEnumConstants()) {
        additionalDimensionsMap.put(enumConst2.getDimensionName(), enumConst2.getDimensionValue());
        EnumMap<E3, Attributes> mapE3 = new EnumMap<>(enumTypeClass3);
        mapE2.put(enumConst2, mapE3);
        for (E3 enumConst3: enumTypeClass3.getEnumConstants()) {
          additionalDimensionsMap.put(enumConst3.getDimensionName(), enumConst3.getDimensionValue());
          mapE3.put(
              enumConst3,
              otelRepository.createAttributes(metricEntity, baseDimensionsMap, additionalDimensionsMap));
        }
      }
    }
    return attributesEnumMap;
  }

  public Attributes getAttributes(E1 key1, E2 key2, E3 key3) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    if (key1 == null || key2 == null || key3 == null) {
      throw new IllegalArgumentException(
          "The key for otel dimension cannot be null for metric Entity: " + getMetricEntity().getMetricName());
    }
    if (!enumTypeClass1.isInstance(key1) || !enumTypeClass2.isInstance(key2) || !enumTypeClass3.isInstance(key3)) {
      // defensive check: This can only happen if the instance is declared without the explicit types
      // and passed in wrong args
      throw new IllegalArgumentException(
          "The key for otel dimension is not of the correct type: " + key1.getClass() + "," + key2.getClass() + ","
              + key3.getClass() + " for metric Entity: " + getMetricEntity().getMetricName());
    }

    Attributes attributes = null;
    EnumMap<E2, EnumMap<E3, Attributes>> mapE2 = attributesEnumMap.get(key1);
    if (mapE2 != null) {
      EnumMap<E3, Attributes> mapE3 = mapE2.get(key2);
      if (mapE3 != null) {
        attributes = mapE3.get(key3);
      }
    }

    if (attributes == null) {
      // defensive check: attributes for all entries of bounded enums should be pre created
      throw new IllegalArgumentException(
          "No dimensions found for keys: " + key1 + "," + key2 + "," + key3 + " for metric Entity: "
              + getMetricEntity().getMetricName());
    }
    return attributes;
  }

  public void record(long value, E1 key1, E2 key2, E3 key3) {
    try {
      super.record(value, getAttributes(key1, key2, key3));
    } catch (Exception e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }

  public void record(double value, E1 key1, E2 key2, E3 key3) {
    try {
      super.record(value, getAttributes(key1, key2, key3));
    } catch (Exception e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }

  /** visible for testing */
  public EnumMap<E1, EnumMap<E2, EnumMap<E3, Attributes>>> getAttributesEnumMap() {
    return attributesEnumMap;
  }
}
