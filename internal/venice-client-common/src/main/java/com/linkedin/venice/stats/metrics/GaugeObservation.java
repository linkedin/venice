package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.LiveStateResolver;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.ValueResolver;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
    return of((attributes, value) -> {
      if (value instanceof Double || value instanceof Float) {
        double doubleValue = value.doubleValue();
        if (Double.isFinite(doubleValue)) {
          measurement.record((long) doubleValue, attributes);
        }
      } else {
        measurement.record(value.longValue(), attributes);
      }
    }, onFailure);
  }

  /** Records finite values into a double gauge. */
  static GaugeObservation of(ObservableDoubleMeasurement measurement, Consumer<Exception> onFailure) {
    return of((attributes, value) -> {
      double doubleValue = value.doubleValue();
      if (Double.isFinite(doubleValue)) {
        measurement.record(doubleValue, attributes);
      }
    }, onFailure);
  }

  /**
   * Passes each sample's value to {@code recorder}, only for non-null state. A resolver or recorder exception skips the
   * sample and is passed to {@code onFailure}.
   */
  static GaugeObservation of(BiConsumer<Attributes, Number> recorder, Consumer<Exception> onFailure) {
    return new GaugeObservation() {
      @Override
      public <S> void observe(
          Attributes attributes,
          @Nonnull LiveStateResolver<S> stateResolver,
          @Nonnull ValueResolver<S> valueResolver) {
        try {
          S state = stateResolver.resolve();
          if (state != null) {
            recorder.accept(attributes, valueResolver.extractValue(state));
          }
        } catch (Exception e) {
          onFailure.accept(e);
        }
      }
    };
  }
}
