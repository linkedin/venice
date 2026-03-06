package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.utils.ByteUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Per-partition detector for write-compute amplification. Tracks partition-level aggregates and a bounded set of the
 * heaviest keys (by total result bytes) within a configurable reporting window.
 *
 * <h3>Two-Level Design</h3>
 * <ul>
 *   <li><b>Level 1 (always on):</b> O(1) per-event partition aggregates — total write-compute count, total result
 *       bytes, and count of large results exceeding the threshold.</li>
 *   <li><b>Level 2 (large results only):</b> Bounded {@code HashMap} of up to {@link #MAX_TRACKED_KEYS} keys,
 *       tracking per-key count, total request/result bytes, and max result size. Only activated when a write-compute
 *       result exceeds the configured threshold.</li>
 * </ul>
 *
 * <h3>Reporting</h3>
 * Reporting is piggy-backed on the write-compute path: after each {@link #record} call, the caller checks
 * {@link #shouldReport} and, if true, calls {@link #buildReportAndReset} to get a snapshot and reset the window.
 * This produces at most one summary log per partition per window — not per key, not per event.
 *
 * <h3>Threading</h3>
 * All public methods are {@code synchronized}. Write-compute processing may be parallel when
 * {@code isAAWCWorkloadParallelProcessingEnabled} is true, but contention is negligible since only large-result
 * events (a small fraction of total write-computes) touch the Level 2 map.
 *
 * <h3>Memory</h3>
 * ~1.5 KB per partition with active large results (Level 1: 40 bytes, Level 2: 20 entries × ~70 bytes).
 * Level 2 is lazily allocated only when the first large result is detected.
 */
public class WriteComputeAmplificationDetector {
  static final int MAX_TRACKED_KEYS = 20;
  static final int TOP_KEYS_TO_REPORT = 5;

  private final long reportIntervalMs;

  // Level 1: partition aggregates
  private int writeComputeCount;
  private long writeComputeTotalResultBytes;
  private int largeResultCount;
  private long windowStartMs;

  // Level 2: top keys by total result bytes (only for large results, lazily allocated)
  private HashMap<ByteArrayKey, KeyAmplificationStats> heavyKeys;

  public WriteComputeAmplificationDetector(long reportIntervalMs) {
    this.reportIntervalMs = reportIntervalMs;
    this.windowStartMs = System.currentTimeMillis();
  }

  /**
   * Record a write-compute event. If the result size exceeds the threshold, the key is tracked in the heavy key map.
   *
   * @param keyBytes the key bytes of the record
   * @param requestSizeBytes the size of the incoming UPDATE payload (partial update request)
   * @param resultSizeBytes the size of the compressed result value (full record after applying partial update)
   * @param largeResultThreshold results larger than this are tracked in the heavy key map
   */
  public synchronized void record(
      byte[] keyBytes,
      int requestSizeBytes,
      int resultSizeBytes,
      int largeResultThreshold) {
    writeComputeCount++;
    writeComputeTotalResultBytes += resultSizeBytes;

    if (resultSizeBytes > largeResultThreshold) {
      largeResultCount++;
      trackHeavyKey(keyBytes, requestSizeBytes, resultSizeBytes);
    }
  }

  /**
   * Atomically check if the reporting window has elapsed, and if so, build the report and reset the window.
   * Returns {@code null} if no report is due (window not elapsed or no large results).
   *
   * <p>This avoids TOCTOU races when multiple threads call concurrently in the parallel AA-WC path:
   * only one thread wins the report; others get {@code null}.
   *
   * @param currentTimeMs current wall-clock time
   * @param largeResultThreshold the threshold used, included in the report for self-contained logging
   * @return an immutable report snapshot, or {@code null} if no report is due
   */
  public synchronized AmplificationReport tryBuildReportAndReset(long currentTimeMs, int largeResultThreshold) {
    if (largeResultCount == 0 || (currentTimeMs - windowStartMs) < reportIntervalMs) {
      return null;
    }
    AmplificationReport report = new AmplificationReport(
        currentTimeMs - windowStartMs,
        writeComputeCount,
        writeComputeTotalResultBytes,
        largeResultCount,
        largeResultThreshold,
        getTopKeys(TOP_KEYS_TO_REPORT));

    // Reset for next window
    writeComputeCount = 0;
    writeComputeTotalResultBytes = 0;
    largeResultCount = 0;
    windowStartMs = currentTimeMs;
    if (heavyKeys != null) {
      heavyKeys.clear();
    }
    return report;
  }

  private void trackHeavyKey(byte[] keyBytes, int requestSize, int resultSize) {
    if (heavyKeys == null) {
      heavyKeys = new HashMap<>();
    }
    // Copy key bytes to avoid retaining references to mutable buffers
    ByteArrayKey key = ByteArrayKey.wrap(Arrays.copyOf(keyBytes, keyBytes.length));
    KeyAmplificationStats stats = heavyKeys.get(key);
    if (stats != null) {
      stats.update(requestSize, resultSize);
    } else if (heavyKeys.size() < MAX_TRACKED_KEYS) {
      heavyKeys.put(key, new KeyAmplificationStats(requestSize, resultSize));
    }
    // If map is full and key not found: skip. Persistent offenders will be captured in the next window.
  }

  private List<Map.Entry<ByteArrayKey, KeyAmplificationStats>> getTopKeys(int n) {
    if (heavyKeys == null || heavyKeys.isEmpty()) {
      return Collections.emptyList();
    }
    return heavyKeys.entrySet()
        .stream()
        .sorted(
            Comparator
                .comparingLong((Map.Entry<ByteArrayKey, KeyAmplificationStats> e) -> e.getValue().totalResultBytes)
                .reversed())
        .limit(n)
        .collect(Collectors.toList());
  }

  /** Per-key amplification stats accumulated within one reporting window. */
  static class KeyAmplificationStats {
    int count;
    long totalResultBytes;
    long totalRequestBytes;
    int maxResultBytes;

    KeyAmplificationStats(int requestSize, int resultSize) {
      this.count = 1;
      this.totalResultBytes = resultSize;
      this.totalRequestBytes = requestSize;
      this.maxResultBytes = resultSize;
    }

    void update(int requestSize, int resultSize) {
      count++;
      totalResultBytes += resultSize;
      totalRequestBytes += requestSize;
      maxResultBytes = Math.max(maxResultBytes, resultSize);
    }

    double getAvgAmplification() {
      return totalRequestBytes > 0 ? (double) totalResultBytes / totalRequestBytes : 0;
    }
  }

  /** Immutable snapshot of one reporting window's amplification data. */
  static class AmplificationReport {
    final long windowDurationMs;
    final int totalWriteComputeCount;
    final long totalResultBytes;
    final int largeResultCount;
    final int largeResultThreshold;
    final List<Map.Entry<ByteArrayKey, KeyAmplificationStats>> topKeys;

    AmplificationReport(
        long windowDurationMs,
        int totalWriteComputeCount,
        long totalResultBytes,
        int largeResultCount,
        int largeResultThreshold,
        List<Map.Entry<ByteArrayKey, KeyAmplificationStats>> topKeys) {
      this.windowDurationMs = windowDurationMs;
      this.totalWriteComputeCount = totalWriteComputeCount;
      this.totalResultBytes = totalResultBytes;
      this.largeResultCount = largeResultCount;
      this.largeResultThreshold = largeResultThreshold;
      this.topKeys = topKeys;
    }

    @Override
    public String toString() {
      double largeResultPct = totalWriteComputeCount > 0 ? 100.0 * largeResultCount / totalWriteComputeCount : 0;
      StringBuilder sb = new StringBuilder();
      sb.append(
          String.format(
              "  Window: %ds | WC total: %d | Large (>%s): %d (%.1f%%) | Total result: %s%n",
              windowDurationMs / 1000,
              totalWriteComputeCount,
              formatBytes(largeResultThreshold),
              largeResultCount,
              largeResultPct,
              formatBytes(totalResultBytes)));

      if (!topKeys.isEmpty()) {
        sb.append("  Top keys by total result bytes:\n");
        int rank = 1;
        for (Map.Entry<ByteArrayKey, KeyAmplificationStats> entry: topKeys) {
          ByteArrayKey key = entry.getKey();
          KeyAmplificationStats stats = entry.getValue();
          sb.append(
              String.format(
                  "    #%d key=0x%s (%dB) | count=%d | total=%s | max=%s | avgAmplification=%.1fx%n",
                  rank++,
                  ByteUtils.toHexString(key.getContent()),
                  key.getContent().length,
                  stats.count,
                  formatBytes(stats.totalResultBytes),
                  formatBytes(stats.maxResultBytes),
                  stats.getAvgAmplification()));
        }
      }
      return sb.toString();
    }

    private static String formatBytes(long bytes) {
      if (bytes >= 1024L * 1024 * 1024) {
        return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
      } else if (bytes >= 1024L * 1024) {
        return String.format("%.1fMB", bytes / (1024.0 * 1024));
      } else if (bytes >= 1024) {
        return String.format("%.1fKB", bytes / 1024.0);
      }
      return bytes + "B";
    }
  }
}
