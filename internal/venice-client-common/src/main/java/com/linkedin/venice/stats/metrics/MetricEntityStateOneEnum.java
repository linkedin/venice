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
 * This version of {@link MetricEntityState} is used when the metric entity has one dynamic dimension
 * which is an {@link Enum} implementing {@link VeniceDimensionInterface}.
 * The base dimensions that are common for all invocation of this instance are passed in the constructor
 * which is used along with all possible values for the dynamic dimensions to create an EnumMap of
 * {@link Attributes} for each possible value of the dynamic dimension. These attributes are used during
 * every record() call, the key to the EnumMap being the value of the dynamic dimension.
 *
 * {@link EnumMap} is used here as it is more efficient than HashMap as it is backed by an array and does
 * not require hashing of the keys resulting in constant time complexity for get() and put() operations.
 *
 */
public class MetricEntityStateOneEnum<E extends Enum<E> & VeniceDimensionInterface> extends MetricEntityState {
  private final EnumMap<E, Attributes> attributesEnumMap;
  private final Class<E> enumTypeClass;

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
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats);
    validateRequiredDimensions(metricEntity, null, baseDimensionsMap, enumTypeClass);
    this.enumTypeClass = enumTypeClass;
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

  /**
   * Creates an EnumMap of {@link Attributes} which will be used to lazy initialize the Attributes
   */
  private EnumMap<E, Attributes> createAttributesEnumMap() {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    return new EnumMap<>(enumTypeClass);
  }

  /**
   * Manages the EnumMap structure for lazy initialization of Attributes.
   * This allows efficient retrieval of Attributes based on one enum dimension. <br>
   * <br>
   * Thread Safety Considerations: <br>
   * While {@link EnumMap} itself is not inherently thread-safe, it is practically thread-safe for this
   * specific use case, without explicit synchronization. This relies on several key properties: <br>
   * 1. No Resizing: EnumMap is backed by an array sized to the number of enum constants at initialization time.
   *    It never resizes this internal array. This eliminates the possibility of ConcurrentModificationException
   *    that might occur with resizable maps during concurrent modification. <br>
   * 2. Idempotent Value Computation: {@link EnumMap#computeIfAbsent} method is used for lazy initialization.
   *    While it is not thread safe, the {@link #createAttributes} method is. This means that multiple calls for
   *    the same keys will result in the creation of Attributes object with similar content. <br>
   * 3. while multiple thread can simultaneously try to write to the same key by calling {@link EnumMap#put}
   *    inside {@link EnumMap#computeIfAbsent}, put does {@code vals[index] = maskNull(value)} which is an
   *    atomic operation by itself, though the returned old value or the size of the Map is not guaranteed to be
   *    correct. But since we are not using the returned value of put or the size anywhere, and as the input value
   *    is idempotent, we can use EnumMap without any synchronization here.<br>
   */
  Attributes getAttributes(E dimension) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    Attributes attributes = attributesEnumMap.computeIfAbsent(dimension, k -> {
      validateInputDimension(k);
      return createAttributes(k);
    });

    if (attributes == null) {
      throw new IllegalArgumentException(
          "No Attributes found for dimension: " + dimension + " for metric Entity: "
              + getMetricEntity().getMetricName());
    }
    return attributes;
  }

  public void record(long value, E dimension) {
    try {
      super.record(value, getAttributes(dimension));
    } catch (IllegalArgumentException e) {
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
        LOGGER.error("Error recording metric: ", e);
      }
    }
  }

  public void record(double value, E dimension) {
    try {
      super.record(value, getAttributes(dimension));
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
