package com.linkedin.venice.stats.metrics;

import java.io.Closeable;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/** Static helpers for closing {@code MetricEntityState*} / {@code AsyncMetricEntityState*} wrappers. */
public final class MetricEntityStateUtils {
  private static final Logger LOGGER = LogManager.getLogger(MetricEntityStateUtils.class);

  private MetricEntityStateUtils() {
  }

  /**
   * Null-safe close. Catches every {@link Exception} so a misbehaving wrapper cannot abort an
   * enclosing close loop (e.g., {@link CompositeCloseable#close()}) and skip later siblings.
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
   * Closes the SDK-side OTel instrument if it is {@link AutoCloseable}. Sync instruments are not
   * AutoCloseable on the SDK side; this is a no-op for them. Caller should pass the snapshot of
   * the wrapper's volatile field so a concurrent second close() cannot deref a now-null reference.
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

  /** Closes every value in the map via {@link #closeQuietly} and clears the map. */
  public static void closeAndClear(Map<?, ? extends Closeable> map) {
    map.values().forEach(MetricEntityStateUtils::closeQuietly);
    map.clear();
  }
}
