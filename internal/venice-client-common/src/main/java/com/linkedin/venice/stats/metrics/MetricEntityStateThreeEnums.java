package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.REDUNDANT_LOG_FILTER;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.Collections;
import java.util.EnumMap;
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
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

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
    validateRequiredDimensions(metricEntity, null, baseDimensionsMap, enumTypeClass1, enumTypeClass2, enumTypeClass3);
    this.enumTypeClass1 = enumTypeClass1;
    this.enumTypeClass2 = enumTypeClass2;
    this.enumTypeClass3 = enumTypeClass3;
    this.baseDimensionsMap = baseDimensionsMap;
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

    if (!preCreateAttributes() && !lazyInitializeAttributes()) {
      // will be created on demand everytime
      return null;
    }

    EnumMap<E1, EnumMap<E2, EnumMap<E3, Attributes>>> attributesEnumMap = new EnumMap<>(enumTypeClass1);
    if (!preCreateAttributes()) {
      // will be created on demand once and cached
      return attributesEnumMap;
    }
    for (E1 enumConst1: enumTypeClass1.getEnumConstants()) {
      EnumMap<E2, EnumMap<E3, Attributes>> mapE2 = new EnumMap<>(enumTypeClass2);
      attributesEnumMap.put(enumConst1, mapE2);
      for (E2 enumConst2: enumTypeClass2.getEnumConstants()) {
        EnumMap<E3, Attributes> mapE3 = new EnumMap<>(enumTypeClass3);
        mapE2.put(enumConst2, mapE3);
        for (E3 enumConst3: enumTypeClass3.getEnumConstants()) {
          mapE3.put(
              enumConst3,
              otelRepository.createAttributes(metricEntity, baseDimensionsMap, enumConst1, enumConst2, enumConst3));
        }
      }
    }
    return attributesEnumMap;
  }

  /**
   * Validates whether the input dimensions passed is not null or whether it
   * is of right class type
   */
  private void validateInputDimensions(E1 key1, E2 key2, E3 key3) {
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
  }

  private Attributes createAttributes(E1 key1, E2 key2, E3 key3) {
    validateInputDimensions(key1, key2, key3);
    return getOtelRepository().createAttributes(getMetricEntity(), baseDimensionsMap, key1, key2, key3);
  }

  private Attributes getAttributesFromEnumMap(E1 key1, E2 key2, E3 key3) {
    Attributes attributes = null;
    EnumMap<E2, EnumMap<E3, Attributes>> mapE2 = attributesEnumMap.get(key1);
    if (mapE2 != null) {
      EnumMap<E3, Attributes> mapE3 = mapE2.get(key2);
      if (mapE3 != null) {
        attributes = mapE3.get(key3);
      }
    }
    return attributes;
  }

  public Attributes getAttributes(E1 key1, E2 key2, E3 key3) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    Attributes attributes;
    if (preCreateAttributes()) {
      // If preCreateAttributes is enabled, then the attributes should have been created during the constructor
      attributes = getAttributesFromEnumMap(key1, key2, key3);
    } else if (lazyInitializeAttributes()) {
      // If lazyInitializeAttributes is enabled, then the attributes should be created on demand for first time and
      // cache it
      attributes = getAttributesFromEnumMap(key1, key2, key3);
      if (attributes != null) {
        return attributes; // Return from cache if found
      }
      attributes = attributesEnumMap.computeIfAbsent(key1, k -> new EnumMap<>(enumTypeClass2))
          .computeIfAbsent(key2, k -> new EnumMap<>(enumTypeClass3))
          .computeIfAbsent(key3, k -> createAttributes(key1, key2, key3));
    } else {
      // create on demand everytime
      attributes = createAttributes(key1, key2, key3);
    }

    if (attributes == null) {
      // check for any specific errors
      validateInputDimensions(key1, key2, key3);
      // throw a generic error if not
      throw new IllegalArgumentException(
          "No dimensions found for keys: " + key1 + "," + key2 + "," + key3 + " for metric Entity: "
              + getMetricEntity().getMetricName());
    }
    return attributes;
  }

  public void record(long value, E1 key1, E2 key2, E3 key3) {
    try {
      super.record(value, getAttributes(key1, key2, key3));
    } catch (IllegalArgumentException e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }

  public void record(double value, E1 key1, E2 key2, E3 key3) {
    try {
      super.record(value, getAttributes(key1, key2, key3));
    } catch (IllegalArgumentException e) {
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
