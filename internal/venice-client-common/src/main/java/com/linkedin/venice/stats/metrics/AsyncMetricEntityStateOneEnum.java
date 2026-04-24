package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleBiFunction;


/**
 * Async state wrapper for a metric with one enum dimension ({@link MetricType#ASYNC_GAUGE} or
 * {@link MetricType#ASYNC_DOUBLE_GAUGE}).
 *
 * <h2>Two-callback contract (enforces cardinality control)</h2>
 *
 * This class registers exactly ONE OTel observable gauge per metric entity. The caller provides:
 *
 * <ol>
 *   <li><b>{@code liveStateResolver}</b> — maps an enum value to its backing state, or {@code null}
 *       when the combo is dormant. The {@code null} return is the liveness signal: the SDK
 *       never sees an attribute set for a dormant combo, so the cardinality cap is only charged for
 *       combos that actually have data.</li>
 *   <li><b>{@code valueResolver}</b> — reads the numeric value from the resolved state. Only invoked
 *       when {@code liveStateResolver} returned non-null.</li>
 * </ol>
 *
 * Splitting the two phases forces the caller to think about liveness: there is no path from
 * "combo" to "value" that skips the state resolution, so it is impossible to accidentally emit a
 * dormant attribute set.
 *
 * <p>Attribute sets are precomputed once per enum value at construction and cached. Per-collection
 * cost is {@code O(|E|)} {@code liveStateResolver} calls plus one {@code measurement.record(...)}
 * per emitted combo.
 */
public class AsyncMetricEntityStateOneEnum<E extends Enum<E> & VeniceDimensionInterface> {
  private final boolean emitOpenTelemetryMetrics;
  /** Precomputed per-enum attributes; {@code null} when OTel is disabled. */
  private final EnumMap<E, Attributes> attributesByEnum;
  /** The single SDK instrument; retained so the SDK keeps the callback referenced. */
  private final Object instrument;

  private AsyncMetricEntityStateOneEnum(
      boolean emitOpenTelemetryMetrics,
      EnumMap<E, Attributes> attributesByEnum,
      Object instrument) {
    this.emitOpenTelemetryMetrics = emitOpenTelemetryMetrics;
    this.attributesByEnum = attributesByEnum;
    this.instrument = instrument;
  }

  /**
   * Creates a state wrapper and registers a single multi-emit observable gauge. On every
   * collection the SDK invokes the callback, which for each enum value:
   * <ul>
   *   <li>calls {@code liveStateResolver.apply(enumValue)} — if {@code null}, skips this combo
   *       for this cycle;</li>
   *   <li>otherwise calls {@code valueResolver.applyAsDouble(state, enumValue)} and emits a data
   *       point with the precomputed attributes.</li>
   * </ul>
   *
   * <p>When OTel is disabled, no registration happens and neither callback is invoked.
   *
   * @param <S> the state type returned by {@code liveStateResolver}. Can be any reference type
   *            (wrapper, task, counter, etc.) — the infra never inspects it beyond null-check.
   */
  public static <E extends Enum<E> & VeniceDimensionInterface, S> AsyncMetricEntityStateOneEnum<E> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass,
      Function<E, S> liveStateResolver,
      ToDoubleBiFunction<S, E> valueResolver) {
    MetricType metricType = metricEntity.getMetricType();
    if (metricType != MetricType.ASYNC_GAUGE && metricType != MetricType.ASYNC_DOUBLE_GAUGE) {
      throw new IllegalArgumentException(
          "AsyncMetricEntityStateOneEnum requires ASYNC_GAUGE or ASYNC_DOUBLE_GAUGE, got: " + metricType
              + " for metric: " + metricEntity.getMetricName());
    }

    // If OTel is disabled (or no repo supplied), short-circuit
    boolean emitOtel = otelRepository != null && otelRepository.emitOpenTelemetryMetrics();
    if (!emitOtel) {
      return new AsyncMetricEntityStateOneEnum<>(false, null, null);
    }

    // Cache the enum constants array once. Class#getEnumConstants() clones its internal array on
    // every call; the callback below runs on every OTel collection cycle, so caching avoids
    // per-cycle allocation.
    E[] enumConstants = enumTypeClass.getEnumConstants();

    // Precompute the Attributes once per enum value at construction time.
    EnumMap<E, Attributes> attributesByEnum = new EnumMap<>(enumTypeClass);
    for (E enumValue: enumConstants) {
      attributesByEnum.put(enumValue, otelRepository.createAttributes(metricEntity, baseDimensionsMap, enumValue));
    }

    // Register exactly ONE SDK observable gauge whose callback, on every collection,
    // walks each enum value and calls liveStateResolver and if the result is not null, calls
    // valueResolver in sequence
    //
    // Per-combo try/catch isolates failures: a throw from liveStateResolver or valueResolver for
    // one enum value does NOT prevent emission for the remaining enum values in the same cycle.
    Object instrument;
    if (metricType == MetricType.ASYNC_DOUBLE_GAUGE) {
      instrument = otelRepository.registerObservableDoubleGauge(metricEntity, measurement -> {
        for (E enumValue: enumConstants) {
          try {
            S state = liveStateResolver.apply(enumValue);
            if (state != null) {
              measurement.record(valueResolver.applyAsDouble(state, enumValue), attributesByEnum.get(enumValue));
            }
          } catch (Exception e) {
            otelRepository.recordFailureMetric(metricEntity, e);
          }
        }
      });
    } else { // ASYNC_GAUGE — SDK instrument is a long gauge, so the value is truncated to long.
      instrument = otelRepository.registerObservableLongGauge(metricEntity, measurement -> {
        for (E enumValue: enumConstants) {
          try {
            S state = liveStateResolver.apply(enumValue);
            if (state != null) {
              measurement.record((long) valueResolver.applyAsDouble(state, enumValue), attributesByEnum.get(enumValue));
            }
          } catch (Exception e) {
            otelRepository.recordFailureMetric(metricEntity, e);
          }
        }
      });
    }

    return new AsyncMetricEntityStateOneEnum<>(true, attributesByEnum, instrument);
  }

  public boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  /** Visible for testing — the cached attributes per enum value, or {@code null} if OTel disabled. */
  public EnumMap<E, Attributes> getAttributesByEnum() {
    return attributesByEnum;
  }

  /** Visible for testing — the underlying SDK instrument handle, or {@code null} if OTel disabled. */
  public Object getInstrument() {
    return instrument;
  }
}
