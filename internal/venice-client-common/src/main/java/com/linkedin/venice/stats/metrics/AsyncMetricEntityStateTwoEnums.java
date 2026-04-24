package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.BiFunction;


/**
 * Async state wrapper for a metric with two enum dimensions ({@link MetricType#ASYNC_GAUGE} or
 * {@link MetricType#ASYNC_DOUBLE_GAUGE}).
 *
 * <h2>Two-callback contract (enforces cardinality control)</h2>
 *
 * This class registers exactly ONE OTel observable gauge per metric entity. The caller provides:
 *
 * <ol>
 *   <li><b>{@code liveStateResolver}</b> — maps an {@code (e1, e2)} pair to its backing state, or
 *       {@code null} when the pair is dormant. The {@code null} return is the liveness signal:
 *       the SDK never sees an attribute set for a dormant pair, so the cardinality cap is only
 *       charged for pairs that actually have data.</li>
 *   <li><b>{@code valueResolver}</b> — reads the numeric value from the resolved state plus both
 *       enum values (useful when the value logic branches on the enums). Only invoked when
 *       {@code liveStateResolver} returned non-null.</li>
 * </ol>
 *
 * Splitting the two phases forces the caller to think about liveness: there is no path from
 * "pair" to "value" that skips the state resolution, so it is impossible to accidentally emit a
 * dormant attribute set.
 *
 * <p>Attribute sets are precomputed once per pair at construction and cached. Per-collection cost
 * is {@code O(|E1| × |E2|)} {@code liveStateResolver} calls plus one
 * {@code measurement.record(...)} per emitted pair.
 */
public class AsyncMetricEntityStateTwoEnums<E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface> {
  private final boolean emitOpenTelemetryMetrics;
  /** Precomputed per-pair attributes; {@code null} when OTel is disabled. */
  private final EnumMap<E1, EnumMap<E2, Attributes>> attributesByEnum;
  /** The single SDK instrument; retained so the SDK keeps the callback referenced. */
  private final Object instrument;

  private AsyncMetricEntityStateTwoEnums(
      boolean emitOpenTelemetryMetrics,
      EnumMap<E1, EnumMap<E2, Attributes>> attributesByEnum,
      Object instrument) {
    this.emitOpenTelemetryMetrics = emitOpenTelemetryMetrics;
    this.attributesByEnum = attributesByEnum;
    this.instrument = instrument;
  }

  /**
   * Creates a state wrapper and registers a single multi-emit observable gauge. On every
   * collection the SDK invokes the callback, which for each {@code (e1, e2)} pair:
   * <ul>
   *   <li>calls {@code liveStateResolver.apply(e1, e2)} — if {@code null}, skips this pair for
   *       this cycle;</li>
   *   <li>otherwise calls {@code valueResolver.applyAsDouble(state, e1, e2)} and emits a data
   *       point with the precomputed attributes.</li>
   * </ul>
   *
   * @param <S> the state type returned by {@code liveStateResolver}. Any reference type — the
   *            infra never inspects it beyond null-check.
   */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, S> AsyncMetricEntityStateTwoEnums<E1, E2> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      BiFunction<E1, E2, S> liveStateResolver,
      ToDoubleTriFunction<S, E1, E2> valueResolver) {
    MetricType metricType = metricEntity.getMetricType();
    if (metricType != MetricType.ASYNC_GAUGE && metricType != MetricType.ASYNC_DOUBLE_GAUGE) {
      throw new IllegalArgumentException(
          "AsyncMetricEntityStateTwoEnums requires ASYNC_GAUGE or ASYNC_DOUBLE_GAUGE, got: " + metricType
              + " for metric: " + metricEntity.getMetricName());
    }

    // If OTel is disabled (or no repo supplied), short-circuit
    boolean emitOtel = otelRepository != null && otelRepository.emitOpenTelemetryMetrics();
    if (!emitOtel) {
      return new AsyncMetricEntityStateTwoEnums<>(false, null, null);
    }

    // Cache the enum constants arrays once. Class#getEnumConstants() clones its internal array on
    // every call; the callback below runs on every OTel collection cycle, so caching avoids
    // per-cycle allocation.
    E1[] enum1Constants = enumTypeClass1.getEnumConstants();
    E2[] enum2Constants = enumTypeClass2.getEnumConstants();

    // Precompute the Attributes once per enum value at construction time.
    EnumMap<E1, EnumMap<E2, Attributes>> attributesByEnum = new EnumMap<>(enumTypeClass1);
    for (E1 e1: enum1Constants) {
      EnumMap<E2, Attributes> inner = new EnumMap<>(enumTypeClass2);
      for (E2 e2: enum2Constants) {
        inner.put(e2, otelRepository.createAttributes(metricEntity, baseDimensionsMap, e1, e2));
      }
      attributesByEnum.put(e1, inner);
    }

    // Register exactly ONE SDK observable gauge whose callback, on every collection,
    // walks each enum value combination and calls liveStateResolver and if the result is not null,
    // calls valueResolver in sequence
    //
    // Per-pair try/catch isolates failures: a throw from liveStateResolver or valueResolver for
    // one (e1, e2) pair does NOT prevent emission for the remaining pairs in the same cycle.
    Object instrument;
    if (metricType == MetricType.ASYNC_DOUBLE_GAUGE) {
      instrument = otelRepository.registerObservableDoubleGauge(metricEntity, measurement -> {
        for (E1 e1: enum1Constants) {
          EnumMap<E2, Attributes> inner = attributesByEnum.get(e1);
          for (E2 e2: enum2Constants) {
            try {
              S state = liveStateResolver.apply(e1, e2);
              if (state != null) {
                measurement.record(valueResolver.applyAsDouble(state, e1, e2), inner.get(e2));
              }
            } catch (Exception e) {
              otelRepository.recordFailureMetric(metricEntity, e);
            }
          }
        }
      });
    } else { // ASYNC_GAUGE — SDK instrument is a long gauge, so the value is truncated to long.
      instrument = otelRepository.registerObservableLongGauge(metricEntity, measurement -> {
        for (E1 e1: enum1Constants) {
          EnumMap<E2, Attributes> inner = attributesByEnum.get(e1);
          for (E2 e2: enum2Constants) {
            try {
              S state = liveStateResolver.apply(e1, e2);
              if (state != null) {
                measurement.record((long) valueResolver.applyAsDouble(state, e1, e2), inner.get(e2));
              }
            } catch (Exception e) {
              otelRepository.recordFailureMetric(metricEntity, e);
            }
          }
        }
      });
    }
    return new AsyncMetricEntityStateTwoEnums<>(true, attributesByEnum, instrument);
  }

  public boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  /** Visible for testing — the cached attributes per {@code (e1, e2)}, or {@code null} if OTel disabled. */
  public EnumMap<E1, EnumMap<E2, Attributes>> getAttributesByEnum() {
    return attributesByEnum;
  }

  /** Visible for testing — the underlying SDK instrument handle, or {@code null} if OTel disabled. */
  public Object getInstrument() {
    return instrument;
  }
}
