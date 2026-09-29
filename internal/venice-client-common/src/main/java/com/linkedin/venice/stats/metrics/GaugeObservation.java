package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.LiveStateResolver;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.ValueResolver;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.function.Consumer;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjLongConsumer;
import javax.annotation.Nonnull;


/**
 * Emits async gauge samples. Callbacks get this instead of the OTel measurement, so every sample goes through a
 * live-state and a value resolver, and missing data emits nothing instead of a placeholder.
 */
public interface GaugeObservation {
  <S> void observe(
      Attributes attributes,
      @Nonnull LiveStateResolver<S> stateResolver,
      @Nonnull ValueResolver<S> valueResolver);

  /** Records into a long gauge: integral values exactly, finite floating-point values truncated. */
  static GaugeObservation of(ObservableLongMeasurement measurement, Consumer<Exception> onFailure) {
    return of(
        (attributes, value) -> measurement.record(value, attributes),
        (attributes, value) -> measurement.record((long) value, attributes),
        onFailure);
  }

  /** Records into a double gauge. */
  static GaugeObservation of(ObservableDoubleMeasurement measurement, Consumer<Exception> onFailure) {
    return of(
        (attributes, value) -> measurement.record((double) value, attributes),
        (attributes, value) -> measurement.record(value, attributes),
        onFailure);
  }

  /**
   * Records a sample only for non-null state: {@code Double} and {@code Float} values go to {@code doubleRecorder} when
   * finite, and other values go to {@code longRecorder} as {@link Number#longValue()}. A resolver exception skips the
   * sample and is passed to {@code onFailure}.
   */
  static GaugeObservation of(
      ObjLongConsumer<Attributes> longRecorder,
      ObjDoubleConsumer<Attributes> doubleRecorder,
      Consumer<Exception> onFailure) {
    return new GaugeObservation() {
      @Override
      public <S> void observe(
          Attributes attributes,
          @Nonnull LiveStateResolver<S> stateResolver,
          @Nonnull ValueResolver<S> valueResolver) {
        try {
          S state = stateResolver.resolve();
          if (state == null) {
            return;
          }
          Number value = valueResolver.extractValue(state);
          if (value instanceof Double || value instanceof Float) {
            double doubleValue = value.doubleValue();
            if (Double.isFinite(doubleValue)) {
              doubleRecorder.accept(attributes, doubleValue);
            }
          } else {
            longRecorder.accept(attributes, value.longValue());
          }
        } catch (Exception e) {
          onFailure.accept(e);
        }
      }
    };
  }
}
