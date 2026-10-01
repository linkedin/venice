package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.LiveStateResolver;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.LiveStateResolverOneEnum;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.ValueResolver;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.ValueResolverOneEnum;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


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
public class AsyncMetricEntityStateOneEnum<E extends Enum<E> & VeniceDimensionInterface> implements AutoCloseable {
  private final boolean emitOpenTelemetryMetrics;
  /** Precomputed per-enum attributes; {@code null} when OTel is disabled. */
  private final EnumMap<E, Attributes> attributesByEnum;
  /** The single SDK instrument; retained so the SDK keeps the callback referenced. */
  private final Object instrument;
  private final MetricEntity metricEntity;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private AsyncMetricEntityStateOneEnum(
      boolean emitOpenTelemetryMetrics,
      EnumMap<E, Attributes> attributesByEnum,
      Object instrument,
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository) {
    this.emitOpenTelemetryMetrics = emitOpenTelemetryMetrics;
    this.attributesByEnum = attributesByEnum;
    this.instrument = instrument;
    this.metricEntity = metricEntity;
    this.otelRepository = otelRepository;
  }

  /**
   * Creates a state wrapper and registers a single multi-emit observable gauge. On every
   * collection the SDK invokes the callback, which for each enum value:
   * <ul>
   *   <li>calls {@code liveStateResolver.resolve(enumValue)} — if {@code null}, skips this combo
   *       for this cycle;</li>
   *   <li>otherwise calls {@code valueResolver.extractValue(state, enumValue)} and emits a data
   *       point with the precomputed attributes if the value is finite.</li>
   * </ul>
   *
   * <p>When OTel is disabled, no registration happens and neither callback is invoked.
   *
   * @param <S> the state type returned by {@code liveStateResolver}. Can be any reference type
   *            (wrapper, task, counter, etc.) — the infra never inspects it beyond null-check.
   * @param scope closes this gauge when its component is retired
   */
  public static <E extends Enum<E> & VeniceDimensionInterface, S> AsyncMetricEntityStateOneEnum<E> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E> enumTypeClass,
      MetricScope scope,
      LiveStateResolverOneEnum<E, S> liveStateResolver,
      ValueResolverOneEnum<S, E> valueResolver) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(liveStateResolver, "liveStateResolver");
    Objects.requireNonNull(valueResolver, "valueResolver");
    MetricType metricType = metricEntity.getMetricType();
    if (metricType != MetricType.ASYNC_GAUGE && metricType != MetricType.ASYNC_DOUBLE_GAUGE) {
      throw new IllegalArgumentException(
          "AsyncMetricEntityStateOneEnum requires ASYNC_GAUGE or ASYNC_DOUBLE_GAUGE, got: " + metricType
              + " for metric: " + metricEntity.getMetricName());
    }

    // If OTel is disabled (or no repo supplied), short-circuit
    boolean emitOtel = otelRepository != null && otelRepository.emitOpenTelemetryMetrics();
    if (!emitOtel) {
      return new AsyncMetricEntityStateOneEnum<>(false, null, null, metricEntity, otelRepository);
    }

    // Attributes and per-enum resolvers are built once here, so a collection allocates nothing per enum value.
    EnumMap<E, Attributes> attributesByEnum = new EnumMap<>(enumTypeClass);
    List<Consumer<GaugeObservation>> samples = new ArrayList<>();
    for (E enumValue: enumTypeClass.getEnumConstants()) {
      Attributes attributes = otelRepository.createAttributes(metricEntity, baseDimensionsMap, enumValue);
      attributesByEnum.put(enumValue, attributes);
      LiveStateResolver<S> stateResolver = () -> liveStateResolver.resolve(enumValue);
      ValueResolver<S> enumValueResolver = state -> valueResolver.extractValue(state, enumValue);
      samples.add(observation -> observation.observe(attributes, stateResolver, enumValueResolver));
    }

    Object instrument = otelRepository.registerObservableGauge(metricEntity, scope, observation -> {
      for (Consumer<GaugeObservation> sample: samples) {
        sample.accept(observation);
      }
    });
    return new AsyncMetricEntityStateOneEnum<>(true, attributesByEnum, instrument, metricEntity, otelRepository);
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

  @Override
  public void close() {
    if (closed.compareAndSet(false, true) && emitOpenTelemetryMetrics && otelRepository != null && instrument != null) {
      otelRepository.closeObservableInstrument(metricEntity, instrument);
    }
  }
}
