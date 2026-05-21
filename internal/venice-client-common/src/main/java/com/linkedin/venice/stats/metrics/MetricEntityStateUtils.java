package com.linkedin.venice.stats.metrics;

import java.io.Closeable;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Static helpers for adopters of the {@code MetricEntityState*} / {@code AsyncMetricEntityState*}
 * wrapper hierarchy.
 *
 * <p>Adopters typically hold per-store / per-version maps of wrappers and need to close each entry
 * on store deletion or process shutdown. The wrapper itself may be {@code null} (no-op subclasses
 * leave fields null when OTel is disabled), so a null-safe close helper avoids replicating the
 * boilerplate at every adopter.
 */
public final class MetricEntityStateUtils {
  private static final Logger LOGGER = LogManager.getLogger(MetricEntityStateUtils.class);

  private MetricEntityStateUtils() {
    // Utility class.
  }

  /**
   * Null-safe close. Accepts any {@link Closeable} — covers every metric wrapper type
   * ({@link AsyncMetricEntityState} subclasses, {@link AsyncMetricEntityStateOneEnum},
   * {@link AsyncMetricEntityStateTwoEnums}). Catches every {@link Exception} (not just
   * {@link java.io.IOException}) so a misbehaving wrapper — including a {@code RuntimeException}
   * from a half-initialised SDK instrument or a double-close — cannot abort an enclosing close
   * loop (e.g., {@link CompositeCloseable#close()}) and skip subsequent siblings.
   */
  public static void closeQuietly(Closeable wrapper) {
    if (wrapper == null) {
      return;
    }
    try {
      wrapper.close();
    } catch (Exception e) {
      LOGGER.warn("Close threw for {}", wrapper.getClass().getSimpleName(), e);
    }
  }

  /**
   * Closes the SDK-side OTel instrument referenced by the async wrapper hierarchy, if it is
   * {@link AutoCloseable}. Used by {@link AsyncMetricEntityState#close()} and the independent
   * close() implementations on {@link AsyncMetricEntityStateOneEnum} / {@link AsyncMetricEntityStateTwoEnums}.
   *
   * <p>The {@code instrument} is expected to be the snapshot of the wrapper's volatile field
   * (read once into a local) so a concurrent second close() cannot dereference a now-null reference.
   * Sync metric instruments are not {@link AutoCloseable} on the SDK side; this method is a no-op
   * for them.
   *
   * <p>Logs SDK close failures at WARN with the metric name and full stack trace and swallows them
   * so a misbehaving SDK instrument cannot abort an enclosing close loop.
   */
  public static void closeOtelInstrumentQuietly(Object instrument, MetricEntity metricEntity) {
    if (instrument instanceof AutoCloseable) {
      try {
        ((AutoCloseable) instrument).close();
      } catch (Exception e) {
        LOGGER.warn("OTel SDK close threw for metric {}", metricEntity.getMetricName(), e);
      }
    }
  }

  /**
   * Closes every value in the map via {@link #closeQuietly} and clears the map. Each value's
   * close() is exception-isolated so one misbehaving entry cannot prevent siblings from closing.
   */
  public static void closeAndClear(Map<?, ? extends Closeable> map) {
    map.values().forEach(MetricEntityStateUtils::closeQuietly);
    map.clear();
  }
}
