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
    super(metricEntity, otelRepository);
    validateRequiredDimensions(metricEntity, baseDimensionsMap, enumTypeClass1, enumTypeClass2, enumTypeClass3);
    this.enumTypeClass1 = enumTypeClass1;
    this.enumTypeClass2 = enumTypeClass2;
    this.enumTypeClass3 = enumTypeClass3;
    this.attributesEnumMap = new EnumMap<>(enumTypeClass1);
    if (emitOpenTelemetryMetrics()) {
      createAttributesEnumMap(metricEntity, otelRepository, baseDimensionsMap);
    }
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
    this.attributesEnumMap = new EnumMap<>(enumTypeClass1);
    if (otelRepository != null) {
      createAttributesEnumMap(metricEntity, otelRepository, baseDimensionsMap);
    }
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

  private void createAttributesEnumMap(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {

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
    if (attributesEnumMap.isEmpty()) {
      throw new VeniceException(
          "The dimensions map is empty. Please check the enum types and ensure they are properly defined.");
    }
  }

  public Attributes getAttributes(E1 key1, E2 key2, E3 key3) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    Attributes attributes = null;
    if (key1 == null || key2 == null || key3 == null) {
      throw new IllegalArgumentException(
          "The key for otel dimension cannot be null for metric Entity: " + getMetricEntity().getMetricName());
    }
    if (!enumTypeClass1.isInstance(key1) || !enumTypeClass2.isInstance(key2) || !enumTypeClass3.isInstance(key3)) {
      // defensive check: This only happens if the instance is declared without the explicit types
      throw new IllegalArgumentException(
          "The key for otel dimension is not of the correct type: " + key1.getClass() + "," + key2.getClass() + ","
              + key3.getClass() + " for metric Entity: " + getMetricEntity().getMetricName());
    }
    EnumMap<E2, EnumMap<E3, Attributes>> mapE2 = attributesEnumMap.get(key1);
    if (mapE2 != null) {
      EnumMap<E3, Attributes> mapE3 = mapE2.get(key2);
      if (mapE3 != null) {
        attributes = mapE3.get(key3);
      }
    }

    if (attributes == null) {
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
