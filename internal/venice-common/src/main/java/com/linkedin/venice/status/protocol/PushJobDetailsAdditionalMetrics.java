package com.linkedin.venice.status.protocol;

import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Keys and accessors for {@link PushJobDetails#additionalPushMetrics}, the open-ended
 * {@code map<string, long>} the push job uses to report extra per-push measurements.
 *
 * <p>Both sides of the protocol go through here so the key strings exist exactly once: the push job writes
 * them, the controller reads them. Two absence cases are distinguished and both mean "not reported" rather
 * than zero:
 * <ul>
 *   <li>a {@code null} map — the push reported no additional metrics at all, which is also what a v5 record
 *       resolves to when read by a v6 reader;</li>
 *   <li>a present map with the key absent — the push reported some metrics but not this one.</li>
 * </ul>
 *
 * <p>Avro deserializes map keys as {@code Utf8}, not {@code String}, so a plain {@code Map#get(Object)} with a
 * {@code String} key silently misses on a record that came off the wire. {@link #getMetric} compares by
 * {@link CharSequence#toString()} to work for both freshly built and deserialized records.
 */
public final class PushJobDetailsAdditionalMetrics {
  /**
   * Summed data-writer task wall-clock milliseconds spent in the external storage write path: throttling wait,
   * {@code batchPut} calls including retries and retry backoff, external flush and external close.
   */
  public static final String EXTERNAL_STORAGE_WRITE_TIME_MS = "externalStorageWriteTimeMs";

  /**
   * Summed data-writer task wall-clock milliseconds spent invoking the Venice/Kafka writes and flushing and
   * closing the Venice writer.
   */
  public static final String VENICE_WRITE_TIME_MS = "veniceWriteTimeMs";

  private PushJobDetailsAdditionalMetrics() {
  }

  /**
   * Read one metric.
   *
   * @return the reported value, or {@code null} when the map is {@code null} or does not carry {@code key}.
   *         A {@code null} return means "not reported" and must not be recorded as a zero observation.
   */
  public static Long getMetric(PushJobDetails pushJobDetails, String key) {
    if (pushJobDetails == null) {
      return null;
    }
    Map<CharSequence, Long> metrics = pushJobDetails.getAdditionalPushMetrics();
    if (metrics == null) {
      return null;
    }
    for (Map.Entry<CharSequence, Long> entry: metrics.entrySet()) {
      CharSequence entryKey = entry.getKey();
      if (entryKey != null && key.equals(entryKey.toString())) {
        return entry.getValue();
      }
    }
    return null;
  }

  /**
   * Record one metric, lazily creating the map so a push that reports nothing leaves it {@code null} rather
   * than serializing an empty map.
   */
  public static void putMetric(PushJobDetails pushJobDetails, String key, long value) {
    Map<CharSequence, Long> metrics = pushJobDetails.getAdditionalPushMetrics();
    if (metrics == null) {
      // Insertion ordered so the serialized map and any log line built from it are stable across runs.
      metrics = new LinkedHashMap<>();
      pushJobDetails.setAdditionalPushMetrics(metrics);
    }
    metrics.put(key, value);
  }
}
