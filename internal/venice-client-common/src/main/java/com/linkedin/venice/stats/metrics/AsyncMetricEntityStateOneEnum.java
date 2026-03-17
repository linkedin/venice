package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.DoubleSupplier;
import java.util.function.Function;


/**
 * This class is used when the async metric ({@link MetricType#ASYNC_GAUGE} or
 * {@link MetricType#ASYNC_DOUBLE_GAUGE}) has one dynamic dimension which is an
 * {@link Enum} implementing {@link VeniceDimensionInterface}.
 *
 * <p>For each enum value, a separate {@link AsyncMetricEntityStateBase} is created with its own callback.
 * This allows polling different values based on the dimension (e.g., different VersionRole values).
 *
 * <p>Callers always provide a {@link DoubleSupplier} callback. The factory dispatches to the correct
 * OTel instrument type based on the {@link MetricEntity}'s {@link MetricType}:
 * <ul>
 *   <li>{@code ASYNC_GAUGE} creates an {@code ObservableLongGauge} (value cast to long)</li>
 *   <li>{@code ASYNC_DOUBLE_GAUGE} creates an {@code ObservableDoubleGauge} (value used as-is)</li>
 * </ul>
 *
 * <p>{@link EnumMap} is used here as it is more efficient than HashMap as it is backed by an array and does
 * not require hashing of the keys resulting in constant time complexity for get() and put() operations.
 */
public class AsyncMetricEntityStateOneEnum<E extends Enum<E> & VeniceDimensionInterface> {
  private final EnumMap<E, AsyncMetricEntityStateBase> metricStatesByEnum;
  private final boolean emitOpenTelemetryMetrics;

  private AsyncMetricEntityStateOneEnum(
      boolean emitOpenTelemetryMetrics,
      EnumMap<E, AsyncMetricEntityStateBase> metricStatesByEnum) {
    this.emitOpenTelemetryMetrics = emitOpenTelemetryMetrics;
    this.metricStatesByEnum = metricStatesByEnum;
  }

  /**
   * Factory method that accepts a {@link DoubleSupplier} callback for each enum value.
   * Dispatches to the correct OTel instrument type based on {@link MetricEntity#getMetricType()}:
   * {@code ASYNC_GAUGE} wraps as long (no rounding — simple truncation cast), and
   * {@code ASYNC_DOUBLE_GAUGE} passes the double directly.
   *
   * <p>Callers with {@code long} values can pass them directly in the lambda — Java auto-widens
   * {@code long} to {@code double} losslessly.
   */
  public static <E extends Enum<E> & VeniceDimensionInterface> AsyncMetricEntityStateOneEnum<E> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass,
      Function<E, DoubleSupplier> callbackProvider) {
    boolean isDoubleGauge = metricEntity.getMetricType() == MetricType.ASYNC_DOUBLE_GAUGE;
    return createInternal(metricEntity, otelRepository, baseDimensionsMap, enumTypeClass, enumValue -> {
      DoubleSupplier callback = callbackProvider.apply(enumValue);
      if (isDoubleGauge) {
        return (dims, attrs) -> AsyncMetricEntityStateBase.create(metricEntity, otelRepository, dims, attrs, callback);
      } else {
        return (dims, attrs) -> AsyncMetricEntityStateBase
            .create(metricEntity, otelRepository, dims, attrs, () -> (long) callback.getAsDouble());
      }
    });
  }

  private static <E extends Enum<E> & VeniceDimensionInterface> AsyncMetricEntityStateOneEnum<E> createInternal(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass,
      Function<E, StateFactory> stateFactory) {
    boolean emitOtel = otelRepository != null && otelRepository.emitOpenTelemetryMetrics();
    if (!emitOtel) {
      return new AsyncMetricEntityStateOneEnum<>(false, null);
    }

    EnumMap<E, AsyncMetricEntityStateBase> states = new EnumMap<>(enumTypeClass);
    for (E enumValue: enumTypeClass.getEnumConstants()) {
      Map<VeniceMetricsDimensions, String> dimensionsWithEnum = new HashMap<>(baseDimensionsMap);
      dimensionsWithEnum.put(enumValue.getDimensionName(), enumValue.getDimensionValue());
      Attributes attributes = otelRepository.createAttributes(metricEntity, baseDimensionsMap, enumValue);

      states.put(enumValue, stateFactory.apply(enumValue).create(dimensionsWithEnum, attributes));
    }
    return new AsyncMetricEntityStateOneEnum<>(true, states);
  }

  @FunctionalInterface
  private interface StateFactory {
    AsyncMetricEntityStateBase create(Map<VeniceMetricsDimensions, String> dimensionsMap, Attributes attributes);
  }

  public boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

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
