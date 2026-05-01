package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.LiveStateResolverOneEnum;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.ValueResolverOneEnum;
import io.opentelemetry.api.common.Attributes;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.ObjDoubleConsumer;


/**
 * Async state wrapper for a metric with one enum dimension ({@link MetricType#ASYNC_GAUGE} or
 * {@link MetricType#ASYNC_DOUBLE_GAUGE}).
 *
 * <h2>Two-callback contract (enforces cardinality control)</h2>
 *
 * This class registers exactly ONE OTel observable gauge per metric entity. The caller provides:
 *
 * <ol>
 *   <li><b>{@link LiveStateResolverOneEnum}</b> — maps an enum value to its backing state, or
 *       {@code null} when the combo is dormant. The {@code null} return is the liveness signal:
 *       the SDK never sees an attribute set for a dormant combo, so the cardinality cap is only
 *       charged for combos that actually have data.</li>
 *   <li><b>{@link ValueResolverOneEnum}</b> — reads the numeric value from the resolved state.
 *       Only invoked when {@link LiveStateResolverOneEnum#resolve} returned non-null.</li>
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
   *   <li>calls {@code liveStateResolver.resolve(enumValue)} — if {@code null}, skips this combo
   *       for this cycle;</li>
   *   <li>otherwise calls {@code valueResolver.extractValue(state, enumValue)} and emits a data
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
      LiveStateResolverOneEnum<E, S> liveStateResolver,
      ValueResolverOneEnum<S, E> valueResolver) {
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

    /*
     * Cache the enum constants array once. Class#getEnumConstants() clones its internal array on
     * every call; the callback below runs on every OTel collection cycle, so caching avoids
     * per-cycle allocation.
     */
    E[] enumConstants = enumTypeClass.getEnumConstants();

    // Precompute the Attributes once per enum value at construction time.
    EnumMap<E, Attributes> attributesByEnum = new EnumMap<>(enumTypeClass);
    for (E enumValue: enumConstants) {
      attributesByEnum.put(enumValue, otelRepository.createAttributes(metricEntity, baseDimensionsMap, enumValue));
    }

    /*
     * Register exactly ONE SDK observable gauge. The callback walks every enum value, calls
     * liveStateResolver, and (when non-null) records via valueResolver. Per-combo try/catch
     * isolates failures so one bad combo doesn't poison the rest of the cycle.
     * {@link VeniceOpenTelemetryMetricsRepository#recordFailureMetric} is internally best-effort
     * (no Exception escapes it), so callers don't need a secondary catch.
     *
     * ASYNC_GAUGE casts double to long; use ASYNC_DOUBLE_GAUGE for ratios / NaN-capable values.
     */
    final Object instrument;
    if (metricType == MetricType.ASYNC_DOUBLE_GAUGE) {
      instrument = otelRepository.registerObservableDoubleGauge(
          metricEntity,
          measurement -> emitAll(
              enumConstants,
              attributesByEnum,
              liveStateResolver,
              valueResolver,
              metricEntity,
              otelRepository,
              (attrs, value) -> measurement.record(value, attrs)));
    } else {
      instrument = otelRepository.registerObservableLongGauge(
          metricEntity,
          measurement -> emitAll(
              enumConstants,
              attributesByEnum,
              liveStateResolver,
              valueResolver,
              metricEntity,
              otelRepository,
              (attrs, value) -> measurement.record((long) value, attrs)));
    }

    return new AsyncMetricEntityStateOneEnum<>(true, attributesByEnum, instrument);
  }

  /**
   * Walks each enum value, resolves liveness + value, and forwards to {@code recorder} when live.
   * Per-combo try/catch isolates failures so one bad combo doesn't poison the rest of the cycle.
   * Only {@link Exception} is caught — {@link Error} (e.g. {@code OutOfMemoryError}) propagates
   * so JVM-level failures still surface.
   */
  private static <E extends Enum<E> & VeniceDimensionInterface, S> void emitAll(
      E[] enumConstants,
      EnumMap<E, Attributes> attributesByEnum,
      LiveStateResolverOneEnum<E, S> liveStateResolver,
      ValueResolverOneEnum<S, E> valueResolver,
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      ObjDoubleConsumer<Attributes> recorder) {
    for (E enumValue: enumConstants) {
      try {
        S state = liveStateResolver.resolve(enumValue);
        if (state != null) {
          recorder.accept(attributesByEnum.get(enumValue), valueResolver.extractValue(state, enumValue));
        }
      } catch (Exception e) {
        // recordFailureMetric handles its own best-effort try/catch internally.
        otelRepository.recordFailureMetric(metricEntity, e);
      }
    }
  }

  public boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  /** Visible for testing — the cached attributes per enum value, or {@code null} if OTel is disabled. */
  public EnumMap<E, Attributes> getAttributesByEnum() {
    return attributesByEnum;
  }

  /** Visible for testing — the underlying SDK instrument handle, or {@code null} if OTel disabled. */
  public Object getInstrument() {
    return instrument;
  }
}
