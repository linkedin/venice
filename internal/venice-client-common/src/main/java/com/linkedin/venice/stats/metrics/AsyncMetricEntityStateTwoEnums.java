package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.DoubleSupplier;


/**
 * This class is used when the async metric ({@link MetricType#ASYNC_GAUGE} or
 * {@link MetricType#ASYNC_DOUBLE_GAUGE}) has two dynamic dimensions, each of which is an
 * {@link Enum} implementing {@link VeniceDimensionInterface}.
 *
 * <p>For each combination of (E1, E2) enum values, a separate {@link AsyncMetricEntityStateBase}
 * is created with its own callback. The callbacks are pre-registered at construction time.
 *
 * <p>Callers always provide a {@link BiFunction}{@code <E1, E2, DoubleSupplier>} callback. The
 * factory dispatches to the correct OTel instrument type based on the {@link MetricEntity}'s
 * {@link MetricType}:
 * <ul>
 *   <li>{@code ASYNC_GAUGE} creates an {@code ObservableLongGauge} (value cast to long)</li>
 *   <li>{@code ASYNC_DOUBLE_GAUGE} creates an {@code ObservableDoubleGauge} (value used as-is)</li>
 * </ul>
 *
 * <p>A two-level {@link EnumMap} is used for O(1) lookup by (E1, E2) pair, backed by arrays
 * with no hashing overhead.
 */
public class AsyncMetricEntityStateTwoEnums<E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface> {
  private final EnumMap<E1, EnumMap<E2, AsyncMetricEntityStateBase>> metricStatesByEnum;
  private final boolean emitOpenTelemetryMetrics;

  private AsyncMetricEntityStateTwoEnums(
      boolean emitOpenTelemetryMetrics,
      EnumMap<E1, EnumMap<E2, AsyncMetricEntityStateBase>> metricStatesByEnum) {
    this.emitOpenTelemetryMetrics = emitOpenTelemetryMetrics;
    this.metricStatesByEnum = metricStatesByEnum;
  }

  /**
   * Factory method that accepts a {@link BiFunction}{@code <E1, E2, DoubleSupplier>} callback for
   * each (E1, E2) combination. Dispatches to the correct OTel instrument type based on
   * {@link MetricEntity#getMetricType()}: {@code ASYNC_GAUGE} wraps as long (truncation cast),
   * and {@code ASYNC_DOUBLE_GAUGE} passes the double directly.
   */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface> AsyncMetricEntityStateTwoEnums<E1, E2> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      BiFunction<E1, E2, DoubleSupplier> callbackProvider) {
    boolean isDoubleGauge = metricEntity.getMetricType() == MetricType.ASYNC_DOUBLE_GAUGE;
    return createInternal(metricEntity, otelRepository, baseDimensionsMap, enumTypeClass1, enumTypeClass2, (e1, e2) -> {
      DoubleSupplier callback = callbackProvider.apply(e1, e2);
      if (isDoubleGauge) {
        return (dims, attrs) -> AsyncMetricEntityStateBase.create(metricEntity, otelRepository, dims, attrs, callback);
      } else {
        return (dims, attrs) -> AsyncMetricEntityStateBase
            .create(metricEntity, otelRepository, dims, attrs, () -> (long) callback.getAsDouble());
      }
    });
  }

  private static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface> AsyncMetricEntityStateTwoEnums<E1, E2> createInternal(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      BiFunction<E1, E2, StateFactory> stateFactory) {
    boolean emitOtel = otelRepository != null && otelRepository.emitOpenTelemetryMetrics();
    if (!emitOtel) {
      return new AsyncMetricEntityStateTwoEnums<>(false, null);
    }

    EnumMap<E1, EnumMap<E2, AsyncMetricEntityStateBase>> states = new EnumMap<>(enumTypeClass1);
    for (E1 e1: enumTypeClass1.getEnumConstants()) {
      EnumMap<E2, AsyncMetricEntityStateBase> innerMap = new EnumMap<>(enumTypeClass2);
      for (E2 e2: enumTypeClass2.getEnumConstants()) {
        Map<VeniceMetricsDimensions, String> dimensionsWithEnums = new HashMap<>(baseDimensionsMap);
        dimensionsWithEnums.put(e1.getDimensionName(), e1.getDimensionValue());
        dimensionsWithEnums.put(e2.getDimensionName(), e2.getDimensionValue());
        Attributes attributes = otelRepository.createAttributes(metricEntity, baseDimensionsMap, e1, e2);
        innerMap.put(e2, stateFactory.apply(e1, e2).create(dimensionsWithEnums, attributes));
      }
      states.put(e1, innerMap);
    }
    return new AsyncMetricEntityStateTwoEnums<>(true, states);
  }

  @FunctionalInterface
  private interface StateFactory {
    AsyncMetricEntityStateBase create(Map<VeniceMetricsDimensions, String> dimensionsMap, Attributes attributes);
  }

  public boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  public AsyncMetricEntityStateBase getMetricState(E1 e1, E2 e2) {
    if (metricStatesByEnum == null) {
      return null;
    }
    // Construction guarantees all E1 keys are populated; innerMap is always non-null here
    return metricStatesByEnum.get(e1).get(e2);
  }

  /** Visible for testing */
  public EnumMap<E1, EnumMap<E2, AsyncMetricEntityStateBase>> getMetricStatesByEnum() {
    return metricStatesByEnum;
  }
}
