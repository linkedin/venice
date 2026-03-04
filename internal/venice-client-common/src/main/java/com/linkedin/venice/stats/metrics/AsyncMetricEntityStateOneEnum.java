package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;


/**
 * This class is used when the async metric (ASYNC_GAUGE) has one dynamic dimension which is an
 * {@link Enum} implementing {@link VeniceDimensionInterface}.
 *
 * For each enum value, a separate {@link AsyncMetricEntityStateBase} is created with its own callback.
 * This allows polling different values based on the dimension (e.g., different VersionRole values).
 *
 * {@link EnumMap} is used here as it is more efficient than HashMap as it is backed by an array and does
 * not require hashing of the keys resulting in constant time complexity for get() and put() operations.
 */
public class AsyncMetricEntityStateOneEnum<E extends Enum<E> & VeniceDimensionInterface> {
  private final EnumMap<E, AsyncMetricEntityStateBase> metricStatesByEnum;
  private final boolean emitOpenTelemetryMetrics;

  /** should not be called directly, call {@link #create} instead */
  private AsyncMetricEntityStateOneEnum(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass,
      Function<E, LongSupplier> callbackProvider) {
    this.emitOpenTelemetryMetrics = otelRepository != null && otelRepository.emitOpenTelemetryMetrics();
    this.metricStatesByEnum = createMetricStatesForAllEnumValues(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        enumTypeClass,
        callbackProvider);
  }

  /**
   * Factory method to create an AsyncMetricEntityStateOneEnum.
   *
   * @param metricEntity The metric entity definition
   * @param otelRepository The OTel repository for metric registration
   * @param baseDimensionsMap Base dimensions common to all enum values
   * @param enumTypeClass The enum class for the dynamic dimension
   * @param callbackProvider Function that provides a LongSupplier callback for each enum value
   * @return A new AsyncMetricEntityStateOneEnum instance
   */
  public static <E extends Enum<E> & VeniceDimensionInterface> AsyncMetricEntityStateOneEnum<E> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass,
      Function<E, LongSupplier> callbackProvider) {
    return new AsyncMetricEntityStateOneEnum<>(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        enumTypeClass,
        callbackProvider);
  }

  /**
   * Creates AsyncMetricEntityStateBase instances for all enum values.
   * Each enum value gets its own ASYNC_GAUGE with dimensions including that enum value.
   */
  private EnumMap<E, AsyncMetricEntityStateBase> createMetricStatesForAllEnumValues(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass,
      Function<E, LongSupplier> callbackProvider) {
    if (!emitOpenTelemetryMetrics) {
      return null;
    }

    EnumMap<E, AsyncMetricEntityStateBase> states = new EnumMap<>(enumTypeClass);

    for (E enumValue: enumTypeClass.getEnumConstants()) {
      // Create dimensions map including the enum dimension
      Map<VeniceMetricsDimensions, String> dimensionsWithEnum = new HashMap<>(baseDimensionsMap);
      dimensionsWithEnum.put(enumValue.getDimensionName(), enumValue.getDimensionValue());

      // Create attributes for this enum value
      Attributes attributes = otelRepository.createAttributes(metricEntity, baseDimensionsMap, enumValue);

      // Get the callback for this enum value
      LongSupplier callback = callbackProvider.apply(enumValue);

      // Create the async metric state
      AsyncMetricEntityStateBase state =
          AsyncMetricEntityStateBase.create(metricEntity, otelRepository, dimensionsWithEnum, attributes, callback);

      states.put(enumValue, state);
    }

    return states;
  }

  /**
   * Returns whether OTel metrics are being emitted.
   */
  public boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  /**
   * Gets the underlying AsyncMetricEntityStateBase for a specific enum value.
   * Useful for testing or advanced use cases.
   */
  public AsyncMetricEntityStateBase getMetricState(E enumValue) {
    if (metricStatesByEnum == null) {
      return null;
    }
    return metricStatesByEnum.get(enumValue);
  }

  /** Visible for testing */
  public EnumMap<E, AsyncMetricEntityStateBase> getMetricStatesByEnum() {
    return metricStatesByEnum;
  }
}
