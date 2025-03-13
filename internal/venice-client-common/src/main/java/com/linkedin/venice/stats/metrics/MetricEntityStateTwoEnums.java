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
 * Similar to {@link MetricEntityStateOneEnum} but with two dynamic dimensions and 2 level EnumMap
 */
public class MetricEntityStateTwoEnums<E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface>
    extends MetricEntityState {
  private final EnumMap<E1, EnumMap<E2, Attributes>> attributesEnumMap;
  private final Class<E1> enumTypeClass1;
  private final Class<E2> enumTypeClass2;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

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
    this.baseDimensionsMap = baseDimensionsMap;
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

    if (!preCreateAttributes() && !lazyInitializeAttributes()) {
      // will be created on demand everytime
      return null;
    }

    EnumMap<E1, EnumMap<E2, Attributes>> attributesEnumMap = new EnumMap<>(enumTypeClass1);
    if (!preCreateAttributes()) {
      // will be created on demand once and cached
      return attributesEnumMap;
    }
    for (E1 enumConst1: enumTypeClass1.getEnumConstants()) {
      EnumMap<E2, Attributes> mapE2 = new EnumMap<>(enumTypeClass2);
      attributesEnumMap.put(enumConst1, mapE2);
      for (E2 enumConst2: enumTypeClass2.getEnumConstants()) {
        mapE2.put(enumConst2, otelRepository.createAttributes(metricEntity, baseDimensionsMap, enumConst1, enumConst2));
      }
    }
    return attributesEnumMap;
  }

  /**
   * Validates whether the input dimensions passed is not null or whether it
   * is of right class type
   */
  private void validateInputDimensions(E1 key1, E2 key2) {
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
  }

  private Attributes createAttributes(E1 key1, E2 key2) {
    validateInputDimensions(key1, key2);
    return getOtelRepository().createAttributes(getMetricEntity(), baseDimensionsMap, key1, key2);
  }

  private Attributes getAttributesFromEnumMap(E1 key1, E2 key2) {
    Attributes attributes = null;
    EnumMap<E2, Attributes> mapE2 = attributesEnumMap.get(key1);
    if (mapE2 != null) {
      attributes = mapE2.get(key2);
    }
    return attributes;
  }

  Attributes getAttributes(E1 key1, E2 key2) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    Attributes attributes;
    if (preCreateAttributes()) {
      // If preCreateAttributes is enabled, then the attributes should have been created during the constructor
      attributes = getAttributesFromEnumMap(key1, key2);
    } else if (lazyInitializeAttributes()) {
      // If lazyInitializeAttributes is enabled, then the attributes should be created on demand for first time and
      // cache it
      attributes = getAttributesFromEnumMap(key1, key2);
      if (attributes != null) {
        return attributes; // Return from cache if found
      }
      attributes = attributesEnumMap.computeIfAbsent(key1, k -> new EnumMap<>(enumTypeClass2))
          .computeIfAbsent(key2, k -> createAttributes(key1, key2));
    } else {
      // create on demand everytime
      attributes = createAttributes(key1, key2);
    }

    if (attributes == null) {
      // check for any specific errors
      validateInputDimensions(key1, key2);
      // throw a generic error if not
      throw new IllegalArgumentException(
          "No dimensions found for keys: " + key1 + "," + key2 + " for metric Entity: "
              + getMetricEntity().getMetricName());
    }
    return attributes;
  }

  public void record(long value, E1 key1, E2 key2) {
    try {
      super.record(value, getAttributes(key1, key2));
    } catch (IllegalArgumentException e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }

  public void record(double value, E1 key1, E2 key2) {
    try {
      super.record(value, getAttributes(key1, key2));
    } catch (IllegalArgumentException e) {
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
