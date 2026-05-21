package com.linkedin.venice.listener.profiler;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.Logger;


/**
 * On-demand profiler that records per-partition request counts and per-partition top-K hot keys
 * for a single Venice store version over a bounded time window.
 *
 * <h3>Two-phase recording</h3>
 *
 * <p>The hot path is split into a warm-up phase and a capture phase to keep per-record cost
 * lock-free even on the hot partition:
 *
 * <ul>
 *   <li><b>Phase 1 — warm-up</b> (first {@link #WARMUP_FRACTION} of the session): every
 *   {@link #record} call increments the per-partition counter ({@link LongAdder}) and the CMS
 *   ({@link CountMinSketch} backed by {@link java.util.concurrent.atomic.AtomicLongArray}).
 *   Nothing else is touched. Every operation is lock-free.</li>
 *   <li><b>Phase 2 — capture</b> (remaining session): counters + CMS as above, plus a per-partition
 *   {@link Set} of hashed keys whose CMS estimate has crossed the natural top-K threshold
 *   ({@code count × K ≥ partitionTotal}). The Set is {@link ConcurrentHashMap#newKeySet()} —
 *   also lock-free. Long-tail keys never enter the Set; only keys that are at-or-above the
 *   {@code 1/K} fraction of partition traffic do.</li>
 * </ul>
 *
 * <p>At snapshot time we re-evaluate each Set member against the <em>final</em>
 * {@code partitionTotal / K} threshold (filters out keys that briefly crossed early but fell
 * back) and push them through a bounded top-K min-heap. The heap is built once per snapshot —
 * not on the hot path — so the per-update lock-contention problem of an always-on heap is gone.
 *
 * <h3>Why {@code WARMUP_FRACTION = 1/3}</h3>
 *
 * <p>A key emerging at fraction {@code τ} of the session with frequency {@code p} is captured
 * iff {@code p × K × (1 − τ/T) ≥ 1}. The worst-case captured key — one that emerges right at
 * the warm-up boundary — needs {@code p ≥ 1 / (K × (1 − w))}. Setting {@code w = 1/3} means
 * we still capture keys that are at least <b>1.5× the natural top-K threshold</b>, which
 * leaves enough fidelity to surface meaningfully-hot late-emerging keys while keeping enough
 * Phase 1 time for the CMS to settle. ({@code w = 1/2} would only catch ≥ 2× keys; {@code
 * w = 1/5} would catch ≥ 1.25× keys but with less CMS warm-up.)
 *
 * <h3>PII safety</h3>
 *
 * <p>Raw keys are never stored. Each key is hashed with murmur3-128 before being passed to the
 * CMS or the per-partition Set.
 */
public final class KeyPartitionProfiler {
  private static final HashFunction KEY_HASH = Hashing.murmur3_128();
  private static final BaseEncoding HEX = BaseEncoding.base16().lowerCase();

  /**
   * Fraction of the session spent in Phase 1 (CMS + counters only). At {@code 1/3} the worst-
   * case late-emerging key captured by Phase 2 has frequency ≥ {@code 1.5/K}, derived from
   * {@code p_min = 1/(K × (1 − w))}. See class Javadoc for the full derivation.
   */
  public static final double WARMUP_FRACTION = 1.0 / 3.0;

  private final String storeName;
  private final String storeVersion;
  private final long startTimeMs;
  private final long durationMs;
  private final int partitionCount;
  private final int maxTopK;
  /** Wall-clock at which Phase 1 ends and the Set starts being populated. */
  private final long warmupEndTimeMs;

  private final LongAdder totalRequests = new LongAdder();
  private final LongAdder[] partitionCounters;
  private final CountMinSketch cms;
  private final List<Set<ByteBuffer>> partitionHotKeys;

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
    this.maxTopK = maxTopK;
    this.warmupEndTimeMs = startTimeMs + (long) (durationMs * WARMUP_FRACTION);
    this.partitionCounters = new LongAdder[partitionCount];
    this.partitionHotKeys = new ArrayList<>(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      this.partitionCounters[i] = new LongAdder();
      this.partitionHotKeys.add(ConcurrentHashMap.newKeySet());
    }
    this.cms = new CountMinSketch();
  }

  /**
   * Hot-path entry. Records a single read for the given key/partition. Every operation is
   * lock-free.
   *
   * <p>Phase 1: increments counter and CMS only.<br>
   * Phase 2: also adds the key to the per-partition Set iff its CMS estimate already crosses
   * the natural top-K threshold ({@code count × K ≥ partitionTotal}).
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
    long estimatedCount = cms.addAndEstimate(hashedKey, 1);

    // Phase 1: just counters + CMS. No set touches — zero contention on the hot partition.
    if (System.currentTimeMillis() < warmupEndTimeMs) {
      return;
    }

    // Phase 2: track only above-threshold keys in the lock-free set. Long-tail keys whose
    // count is below partitionTotal / K are filtered out and never take any extra work.
    long partitionTotal = partitionCounters[partitionId].sum();
    if (estimatedCount * maxTopK >= partitionTotal) {
      partitionHotKeys.get(partitionId).add(ByteBuffer.wrap(hashedKey));
    }
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
   * Emit the full profile as a single-line JSON object. Convenience for tests; production code
   * should prefer {@link #emitJsonTo} which splits the payload across multiple log lines.
   *
   * <p>Shape:
   * <pre>
   * {
   *   "storeName": ..., "storeVersion": ...,
   *   "startTimeMs": ..., "durationMs": ..., "actualDurationMs": ...,
   *   "totalRequests": ..., "skewFactor": ...,
   *   "partitionDistribution": [ { "partitionId", "count", "percentage" }, ... ],
   *   "topKeysByPartition": { "&lt;partitionId&gt;": [ { "keyHash", "estimatedCount",
   *                                                 "percentageOfPartition" }, ... ] }
   * }
   * </pre>
   */
  public String toJson() {
    Snapshot snapshot = buildSnapshot();
    StringBuilder sb = new StringBuilder(1024);
    appendHeader(sb, snapshot, /*includeTopKeysByPartition*/ true);
    return sb.toString();
  }

  /**
   * Emit the profile across multiple log lines. One header line carries the session-level
   * metrics + partition distribution; each non-empty partition's top-K is emitted on its own
   * subsequent line, sharing a common {@code sessionId} for correlation.
   *
   * <p>Splitting avoids single log lines in the hundreds of MB at large {@code topK} × {@code
   * partitionCount} (some async appenders OOM or truncate at line-length limits).
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
      long partitionTotal = entry[1];
      List<Entry> top = topKForPartition(partitionId);
      if (top.isEmpty()) {
        continue;
      }
      partitionLine.setLength(0);
      appendPartitionTopK(partitionLine, partitionId, partitionTotal, top);
      logger.info("{}: {} sessionId={} partition {}", logPrefix, reason, sessionId, partitionLine);
    }
  }

  /**
   * Build the top-K for one partition by scanning its Phase-2 candidate Set, looking up each
   * candidate's current CMS count and pushing through a bounded min-heap.
   */
  private List<Entry> topKForPartition(int partitionId) {
    Set<ByteBuffer> candidates = partitionHotKeys.get(partitionId);
    if (candidates.isEmpty()) {
      return Collections.emptyList();
    }
    // Cap initial capacity at the actual candidate count — at small Set sizes this avoids
    // allocating maxTopK + 1 = 10_001 slots just to hold a handful of entries.
    int initialCapacity = Math.min(candidates.size(), maxTopK);
    PriorityQueue<Entry> minHeap = new PriorityQueue<>(initialCapacity, Comparator.comparingLong(e -> e.count));
    for (ByteBuffer wrappedKey: candidates) {
      byte[] hashedKey = wrappedKey.array();
      long count = cms.estimateCount(hashedKey);
      if (minHeap.size() < maxTopK) {
        minHeap.offer(new Entry(hashedKey, count));
        continue;
      }
      // peek() is null-safe even though the surrounding invariants (maxTopK > 0 and
      // size >= maxTopK in this branch) guarantee a non-empty heap — the null guard keeps
      // this method standalone-correct against any future loosening of those invariants.
      Entry currentMin = minHeap.peek();
      if (currentMin != null && count > currentMin.count) {
        minHeap.poll();
        minHeap.offer(new Entry(hashedKey, count));
      }
    }
    List<Entry> result = new ArrayList<>(minHeap);
    result.sort((a, b) -> Long.compare(b.count, a.count));
    return result;
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
        long partitionTotal = entry[1];
        List<Entry> top = topKForPartition(partitionId);
        if (top.isEmpty()) {
          continue;
        }
        if (!first) {
          sb.append(',');
        }
        first = false;
        sb.append('"').append(partitionId).append("\":[");
        appendTopKEntries(sb, partitionTotal, top);
        sb.append(']');
      }
      sb.append('}');
    }
    sb.append('}');
  }

  private void appendPartitionTopK(StringBuilder sb, int partitionId, long partitionTotal, List<Entry> top) {
    sb.append('{');
    appendLong(sb, "partitionId", partitionId).append(',');
    appendLong(sb, "count", partitionTotal).append(',');
    sb.append("\"topKeys\":[");
    appendTopKEntries(sb, partitionTotal, top);
    sb.append("]}");
  }

  private void appendTopKEntries(StringBuilder sb, long partitionTotal, List<Entry> top) {
    boolean firstKey = true;
    for (Entry keyEntry: top) {
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

  /** Snapshot-time top-K entry. Constructed only inside {@link #topKForPartition}. */
  static final class Entry {
    final byte[] keyHash;
    final long count;

    Entry(byte[] keyHash, long count) {
      this.keyHash = keyHash;
      this.count = count;
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
