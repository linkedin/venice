package com.linkedin.venice.listener.profiler;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.Logger;


/**
 * On-demand profiler that records per-partition request counts and per-partition top-K hot keys
 * for a single Venice store version over a bounded time window.
 *
 * <p>Memory layout per session (at 1024 partitions, default top-K=100):
 * <ul>
 *   <li>{@code partitionCounters}: 1024 × ~24 B ≈ 24 KB</li>
 *   <li>{@code cms} (shared, {@code long[][]}): ~106 KB</li>
 *   <li>{@code partitionTopK}: 1024 × ~4 KB ≈ 4 MB</li>
 *   <li>Total: ~4.15 MB</li>
 * </ul>
 *
 *
 * <p><b>PII safety.</b> Raw keys are never stored. Each key is hashed with murmur3-128 before
 * being passed to the CMS or heap.
 */
public final class KeyPartitionProfiler {
  private static final HashFunction KEY_HASH = Hashing.murmur3_128();
  private static final BaseEncoding HEX = BaseEncoding.base16().lowerCase();

  private final String storeName;
  private final String storeVersion;
  private final long startTimeMs;
  private final long durationMs;
  private final int partitionCount;

  private final LongAdder totalRequests = new LongAdder();
  private final LongAdder[] partitionCounters;
  private final CountMinSketch cms;
  private final PartitionTopK[] partitionTopK;

  public KeyPartitionProfiler(
      String storeName,
      String storeVersion,
      long startTimeMs,
      long durationMs,
      int partitionCount,
      int maxTopK) {
    if (partitionCount <= 0) {
      throw new IllegalArgumentException("partitionCount must be positive");
    }
    if (maxTopK <= 0) {
      throw new IllegalArgumentException("maxTopK must be positive");
    }
    this.storeName = storeName;
    this.storeVersion = storeVersion;
    this.startTimeMs = startTimeMs;
    this.durationMs = durationMs;
    this.partitionCount = partitionCount;
    this.partitionCounters = new LongAdder[partitionCount];
    this.partitionTopK = new PartitionTopK[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      this.partitionCounters[i] = new LongAdder();
      this.partitionTopK[i] = new PartitionTopK(maxTopK);
    }
    this.cms = new CountMinSketch();
  }

  /**
   * Hot-path entry. Records a single read for the given key/partition. No-op once the profiling
   * window has expired or if {@code partitionId} is out of range.
   */
  public void record(byte[] keyBytes, int partitionId) {
    if (isExpired()) {
      return;
    }
    if (partitionId < 0 || partitionId >= partitionCount) {
      return;
    }
    totalRequests.increment();
    partitionCounters[partitionId].increment();

    byte[] hashedKey = KEY_HASH.hashBytes(keyBytes).asBytes();
    cms.add(hashedKey, 1);
    long estimatedCount = cms.estimateCount(hashedKey);
    partitionTopK[partitionId].update(hashedKey, estimatedCount);
  }

  public boolean isExpired() {
    return System.currentTimeMillis() >= startTimeMs + durationMs;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getStoreVersion() {
    return storeVersion;
  }

  /**
   * Emit the full profile as a single-line JSON object suitable for logging.
   *
   * <p>Shape:
   * <pre>
   * {
   *   "storeName":              string  — Venice store name (no version suffix)
   *   "storeVersion":           string  — version topic, e.g. "myStore_v3"
   *   "startTimeMs":            long    — wall-clock start of the profiling window
   *   "durationMs":             long    — configured window length in ms
   *   "actualDurationMs":       long    — elapsed time at emit (≤ durationMs)
   *   "totalRequests":          long    — total recorded reads across all partitions
   *   "skewFactor":             double  — max partition count / avg non-zero partition
   *                                       count; ≥ 5 indicates significant skew
   *   "partitionDistribution":  array sorted by count DESC, non-zero partitions only:
   *     [ { "partitionId": int, "count": long, "percentage": double of total } ]
   *   "topKeysByPartition":     object keyed by partition id (ascending):
   *     { "&lt;partitionId&gt;": [
   *         { "keyHash":             string — murmur3-128 hex of the raw key (PII-safe),
   *           "estimatedCount":      long   — CMS estimate; over-counts possible,
   *                                           under-counts never,
   *           "percentageOfPartition": double }, ... sorted by estimatedCount DESC ] }
   * }
   * </pre>
   *
   * <p>Keys are emitted only for partitions that received at least one request. Within each
   * partition the heap is capped at the configured top-K (default 100). Numeric percentages
   * use 3 decimal places. The output never contains raw key bytes — only murmur3-128 hashes.
   */
  public String toJson() {
    Snapshot snapshot = buildSnapshot();
    StringBuilder sb = new StringBuilder(1024);
    appendHeader(sb, snapshot, /*includeTopKeysByPartition*/ true);
    return sb.toString();
  }

  /**
   * Emit the profile across multiple log lines instead of a single (potentially multi-MB)
   * payload. One header line carries the session-level metrics + the partition distribution;
   * each non-empty partition's top-K is emitted on its own subsequent line. All lines share a
   * common {@code sessionId} so a consumer can correlate them.
   *
   * <p>This avoids:
   * <ul>
   *   <li>Single-line lengths in the hundreds of MB at large {@code topK} × {@code partitionCount}
   *       (some async appenders OOM or pre-truncate at line-length limits)</li>
   *   <li>Holding any monitor while the JSON is built — the snapshot is purely local state</li>
   * </ul>
   */
  public void emitJsonTo(Logger logger, String logPrefix, String reason) {
    Snapshot snapshot = buildSnapshot();
    long sessionId = startTimeMs;

    StringBuilder header = new StringBuilder(1024);
    appendHeader(header, snapshot, /*includeTopKeysByPartition*/ false);
    logger.info("{}: {} sessionId={} header {}", logPrefix, reason, sessionId, header);

    StringBuilder partitionLine = new StringBuilder(1024);
    for (long[] entry: snapshot.partitionStatsByPartitionIdAsc) {
      int partitionId = (int) entry[0];
      List<PartitionTopK.Entry> top = partitionTopK[partitionId].snapshotSortedDescending();
      if (top.isEmpty()) {
        continue;
      }
      partitionLine.setLength(0);
      long partitionTotal = entry[1];
      appendPartitionTopK(partitionLine, partitionId, partitionTotal, top);
      logger.info("{}: {} sessionId={} partition {}", logPrefix, reason, sessionId, partitionLine);
    }
  }

  private void appendHeader(StringBuilder sb, Snapshot snapshot, boolean includeTopKeysByPartition) {
    sb.append('{');
    appendString(sb, "storeName", storeName).append(',');
    appendString(sb, "storeVersion", storeVersion).append(',');
    appendLong(sb, "startTimeMs", startTimeMs).append(',');
    appendLong(sb, "durationMs", durationMs).append(',');
    appendLong(sb, "actualDurationMs", snapshot.actualDurationMs).append(',');
    appendLong(sb, "totalRequests", snapshot.total).append(',');
    appendDouble(sb, "skewFactor", snapshot.skewFactor).append(',');

    sb.append("\"partitionDistribution\":[");
    boolean first = true;
    for (long[] entry: snapshot.partitionStatsSortedByCountDesc) {
      if (!first) {
        sb.append(',');
      }
      first = false;
      long count = entry[1];
      double percentage = snapshot.total == 0 ? 0.0 : (count * 100.0) / snapshot.total;
      sb.append('{');
      appendLong(sb, "partitionId", entry[0]).append(',');
      appendLong(sb, "count", count).append(',');
      appendDouble(sb, "percentage", percentage);
      sb.append('}');
    }
    sb.append(']');

    if (includeTopKeysByPartition) {
      sb.append(",\"topKeysByPartition\":{");
      first = true;
      for (long[] entry: snapshot.partitionStatsByPartitionIdAsc) {
        int partitionId = (int) entry[0];
        List<PartitionTopK.Entry> top = partitionTopK[partitionId].snapshotSortedDescending();
        if (top.isEmpty()) {
          continue;
        }
        if (!first) {
          sb.append(',');
        }
        first = false;
        sb.append('"').append(partitionId).append("\":[");
        long partitionTotal = entry[1];
        appendTopKEntries(sb, partitionTotal, top);
        sb.append(']');
      }
      sb.append('}');
    }
    sb.append('}');
  }

  private void appendPartitionTopK(
      StringBuilder sb,
      int partitionId,
      long partitionTotal,
      List<PartitionTopK.Entry> top) {
    sb.append('{');
    appendLong(sb, "partitionId", partitionId).append(',');
    appendLong(sb, "count", partitionTotal).append(',');
    sb.append("\"topKeys\":[");
    appendTopKEntries(sb, partitionTotal, top);
    sb.append("]}");
  }

  private void appendTopKEntries(StringBuilder sb, long partitionTotal, List<PartitionTopK.Entry> top) {
    boolean firstKey = true;
    for (PartitionTopK.Entry keyEntry: top) {
      if (!firstKey) {
        sb.append(',');
      }
      firstKey = false;
      double percentage = partitionTotal == 0 ? 0.0 : (keyEntry.count * 100.0) / partitionTotal;
      sb.append('{');
      appendString(sb, "keyHash", HEX.encode(keyEntry.keyHash)).append(',');
      appendLong(sb, "estimatedCount", keyEntry.count).append(',');
      appendDouble(sb, "percentageOfPartition", percentage);
      sb.append('}');
    }
  }

  private Snapshot buildSnapshot() {
    long now = System.currentTimeMillis();
    long actualDurationMs = Math.min(now - startTimeMs, durationMs);
    long total = totalRequests.sum();

    List<long[]> partitionStats = new ArrayList<>(partitionCount);
    long maxPartitionCount = 0;
    long nonZeroPartitions = 0;
    long sumPartitionCounts = 0;
    for (int p = 0; p < partitionCount; p++) {
      long count = partitionCounters[p].sum();
      sumPartitionCounts += count;
      if (count > 0) {
        nonZeroPartitions++;
        partitionStats.add(new long[] { p, count });
        if (count > maxPartitionCount) {
          maxPartitionCount = count;
        }
      }
    }
    List<long[]> byCountDesc = new ArrayList<>(partitionStats);
    byCountDesc.sort((a, b) -> Long.compare(b[1], a[1]));
    List<long[]> byPartitionIdAsc = new ArrayList<>(partitionStats);
    byPartitionIdAsc.sort(Comparator.comparingLong(a -> a[0]));

    double averagePartitionCount = nonZeroPartitions == 0 ? 0.0 : (double) sumPartitionCounts / nonZeroPartitions;
    double skewFactor = averagePartitionCount == 0 ? 0.0 : maxPartitionCount / averagePartitionCount;
    return new Snapshot(actualDurationMs, total, skewFactor, byCountDesc, byPartitionIdAsc);
  }

  private static final class Snapshot {
    final long actualDurationMs;
    final long total;
    final double skewFactor;
    final List<long[]> partitionStatsSortedByCountDesc;
    final List<long[]> partitionStatsByPartitionIdAsc;

    Snapshot(
        long actualDurationMs,
        long total,
        double skewFactor,
        List<long[]> partitionStatsSortedByCountDesc,
        List<long[]> partitionStatsByPartitionIdAsc) {
      this.actualDurationMs = actualDurationMs;
      this.total = total;
      this.skewFactor = skewFactor;
      this.partitionStatsSortedByCountDesc = partitionStatsSortedByCountDesc;
      this.partitionStatsByPartitionIdAsc = partitionStatsByPartitionIdAsc;
    }
  }

  private static StringBuilder appendString(StringBuilder sb, String name, String value) {
    sb.append('"').append(name).append("\":\"");
    if (value != null) {
      for (int i = 0; i < value.length(); i++) {
        char c = value.charAt(i);
        switch (c) {
          case '"':
            sb.append("\\\"");
            break;
          case '\\':
            sb.append("\\\\");
            break;
          case '\n':
            sb.append("\\n");
            break;
          case '\r':
            sb.append("\\r");
            break;
          case '\t':
            sb.append("\\t");
            break;
          default:
            if (c < 0x20) {
              sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
            } else {
              sb.append(c);
            }
        }
      }
    }
    sb.append('"');
    return sb;
  }

  private static StringBuilder appendLong(StringBuilder sb, String name, long value) {
    return sb.append('"').append(name).append("\":").append(value);
  }

  private static StringBuilder appendDouble(StringBuilder sb, String name, double value) {
    sb.append('"').append(name).append("\":");
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      sb.append("0.0");
    } else {
      // Locale.ROOT keeps the JSON-required '.' decimal separator regardless of JVM locale.
      sb.append(String.format(Locale.ROOT, "%.3f", value));
    }
    return sb;
  }
}
