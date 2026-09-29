package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.LiveStateResolver;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.ValueResolver;
import io.opentelemetry.api.common.Attributes;
import java.util.function.Consumer;
import java.util.function.ObjDoubleConsumer;
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

  /**
   * Records a sample only for non-null state with a finite value. A resolver exception skips the sample and is
   * passed to {@code onFailure}.
   */
  static GaugeObservation of(ObjDoubleConsumer<Attributes> recorder, Consumer<Exception> onFailure) {
    return new GaugeObservation() {
      @Override
      public <S> void observe(
          Attributes attributes,
          @Nonnull LiveStateResolver<S> stateResolver,
          @Nonnull ValueResolver<S> valueResolver) {
        try {
          S state = stateResolver.resolve();
          if (state != null) {
            double value = valueResolver.extractValue(state);
            if (Double.isFinite(value)) {
              recorder.accept(attributes, value);
            }
          }
        } catch (Exception e) {
          onFailure.accept(e);
        }
      }
    };
  }
}
