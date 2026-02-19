package com.linkedin.venice.stats;

import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.List;
import java.util.function.Consumer;


/**
 * Holds an OpenTelemetry observable instrument together with the list of callbacks that should
 * be invoked during each metric collection cycle.
 *
 * <p><b>Problem:</b> OpenTelemetry allows only a single callback per observable instrument
 * (the one passed to {@code buildWithCallback}). When multiple stores each create their own
 * {@code IngestionOtelStats} instance, each calls
 * {@link VeniceOpenTelemetryMetricsRepository#registerObservableLongCounter} with its own
 * {@code this::reportToMeasurement} callback. With the previous {@code computeIfAbsent}
 * approach, only the first store's callback was retained; all subsequent stores' data was
 * silently dropped.</p>
 *
 * <p><b>Fix:</b> The instrument is built once with a wrapper callback that iterates over
 * all registered callbacks in this entry's list. New callbacks are appended to the thread-safe
 * list after {@code computeIfAbsent} returns.</p>
 *
 * @param <T> the observable instrument type (e.g. {@code ObservableLongCounter},
 *            {@code ObservableLongUpDownCounter})
 */
class ObservableInstrumentEntry<T> {
  final T instrument;
  final List<Consumer<ObservableLongMeasurement>> callbacks;

  /**
   * @param instrument the observable instrument built with a wrapper callback that iterates {@code callbacks}
   * @param callbacks the shared callback list; the same list must be captured by the instrument's
   *                  wrapper callback so that newly added callbacks are visible during collection
   */
  ObservableInstrumentEntry(T instrument, List<Consumer<ObservableLongMeasurement>> callbacks) {
    this.instrument = instrument;
    this.callbacks = callbacks;
  }
}
