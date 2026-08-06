package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.utils.ByteUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;


/**
 * Per-partition tracker for keys whose assembled value has exceeded the nearline record size limit.
 *
 * <h3>Why block per-key instead of pausing the partition</h3>
 * A single oversized record must not stall ingestion for an entire store version. Pausing consumption (the previously
 * explored approach) creates heartbeat lag, risks permanent data loss once real-time topic retention elapses, and in a
 * multitenant cluster amplifies one tenant's problem into a cluster-wide one. Blocking writes to just the offending key
 * keeps offsets advancing and confines the blast radius to that key.
 *
 * <h3>What is blocked</h3>
 * Only partial updates (UPDATE) are blocked. Full puts and deletes always pass through, because they are the only way
 * to shrink or reset an oversized record; blocking them would make the record permanently unrecoverable. A put or
 * delete therefore also {@link #unblock unblocks} the key, so a reset event restores writes without operator action.
 *
 * <h3>Bounded and non-authoritative</h3>
 * The blocked-key map is capped at {@link #maxTrackedKeys} entries with LRU eviction, so memory is bounded even if a
 * store misbehaves at scale. Eviction is safe: this map is only a fast path that lets repeat offenders skip the
 * expensive merge (chunk assembly, Avro deserialization, re-serialization, compression). The authoritative decision is
 * always the post-merge size check, which re-detects and re-inserts an evicted key. For the same reason the map needs
 * no persistence — it rebuilds itself after a restart or leader failover.
 *
 * <h3>Reporting</h3>
 * Reporting is piggy-backed on the write path: {@link #recordBlockAndMaybeReport} records the event and, once the
 * window has elapsed, returns a snapshot and resets the window counters. This yields at most one log per partition per
 * window rather than one per rejected write. Window resets clear only the counters; the blocked-key map persists,
 * since it is what keeps the keys blocked.
 *
 * <h3>Threading</h3>
 * All public methods are {@code synchronized}, matching {@link PartialUpdateAmplificationDetector}. Partial-update
 * processing may run in parallel when {@code isAAWCWorkloadParallelProcessingEnabled} is true. Contention is
 * negligible: {@link #isBlocked} is an O(1) map lookup on an empty map in the overwhelmingly common case, and the
 * heavier paths only run for records that actually exceed the limit.
 */
public class LargeRecordBlockDetector {
  public static final int DEFAULT_MAX_TRACKED_KEYS = 1000;
  static final int TOP_KEYS_TO_REPORT = 10;

  private final long reportIntervalMs;
  private final int maxTrackedKeys;
  private final LongSupplier clock;

  /** Access-ordered so eviction drops the least recently offending key, keeping hot offenders on the fast path. */
  private final LinkedHashMap<ByteArrayKey, BlockedKeyStats> blockedKeys;

  private int blockedWriteCount;
  private int evictedKeyCount;
  private long windowStartMs;
  private boolean reportPending;

  public LargeRecordBlockDetector(long reportIntervalMs, int maxTrackedKeys) {
    this(reportIntervalMs, maxTrackedKeys, System::currentTimeMillis);
  }

  /** Package-private constructor for testing with a controllable clock. */
  LargeRecordBlockDetector(long reportIntervalMs, int maxTrackedKeys, LongSupplier clock) {
    this.reportIntervalMs = reportIntervalMs;
    this.maxTrackedKeys = maxTrackedKeys > 0 ? maxTrackedKeys : DEFAULT_MAX_TRACKED_KEYS;
    this.clock = clock;
    this.windowStartMs = clock.getAsLong();
    this.blockedKeys = new LinkedHashMap<ByteArrayKey, BlockedKeyStats>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<ByteArrayKey, BlockedKeyStats> eldest) {
        if (size() > LargeRecordBlockDetector.this.maxTrackedKeys) {
          evictedKeyCount++;
          return true;
        }
        return false;
      }
    };
  }

  /**
   * Whether writes to this key are currently blocked. Callers use this to skip the merge entirely for a known
   * offender. A {@code false} result does not mean the write is safe — it only means this key has not been seen to
   * exceed the limit yet, so the post-merge size check still applies.
   */
  public synchronized boolean isBlocked(byte[] keyBytes) {
    return !blockedKeys.isEmpty() && blockedKeys.containsKey(ByteArrayKey.wrap(keyBytes));
  }

  /**
   * Record that a write to this key was blocked, and return a report if the window has elapsed.
   *
   * @param keyBytes the key whose assembled value exceeded the limit
   * @param assembledSizeBytes assembled (post-merge, post-compression) value size that triggered the block, or
   *                           {@link BlockedKeyStats#SIZE_UNKNOWN} when the write was skipped on the fast path and the
   *                           value was therefore never assembled
   * @param limitBytes the configured nearline record size limit, for the report
   * @return an immutable snapshot, or {@code null} if no report is due
   */
  public synchronized BlockReport recordBlockAndMaybeReport(byte[] keyBytes, int assembledSizeBytes, int limitBytes) {
    blockedWriteCount++;
    ByteArrayKey key = ByteArrayKey.wrap(keyBytes);
    long nowMs = clock.getAsLong();
    BlockedKeyStats stats = blockedKeys.get(key);
    if (stats == null) {
      blockedKeys.put(key, new BlockedKeyStats(assembledSizeBytes, nowMs));
      // A newly blocked key is always worth surfacing, even mid-window.
      reportPending = true;
    } else {
      stats.update(assembledSizeBytes);
    }

    if (!reportPending || (nowMs - windowStartMs) < reportIntervalMs) {
      return null;
    }
    BlockReport report =
        new BlockReport(nowMs - windowStartMs, blockedWriteCount, evictedKeyCount, limitBytes, getTopKeys(nowMs));
    blockedWriteCount = 0;
    evictedKeyCount = 0;
    windowStartMs = nowMs;
    reportPending = false;
    return report;
  }

  /**
   * Stop blocking this key. Called for full puts and deletes, which can only shrink or reset the record.
   *
   * @return {@code true} if the key had been blocked
   */
  public synchronized boolean unblock(byte[] keyBytes) {
    return !blockedKeys.isEmpty() && blockedKeys.remove(ByteArrayKey.wrap(keyBytes)) != null;
  }

  /** Number of keys currently blocked in this partition. */
  public synchronized int getBlockedKeyCount() {
    return blockedKeys.size();
  }

  private List<Map.Entry<ByteArrayKey, BlockedKeyStats>> getTopKeys(long nowMs) {
    List<Map.Entry<ByteArrayKey, BlockedKeyStats>> snapshot = new ArrayList<>();
    for (Map.Entry<ByteArrayKey, BlockedKeyStats> entry: blockedKeys.entrySet()) {
      snapshot.add(new BlockedKeyEntry(entry.getKey(), entry.getValue().copy(nowMs)));
    }
    snapshot.sort(
        Comparator.comparingInt((Map.Entry<ByteArrayKey, BlockedKeyStats> e) -> e.getValue().blockedWriteCount)
            .reversed());
    return snapshot.size() > TOP_KEYS_TO_REPORT ? snapshot.subList(0, TOP_KEYS_TO_REPORT) : snapshot;
  }

  /** Immutable {@link Map.Entry} so report snapshots do not alias the live map. */
  private static final class BlockedKeyEntry implements Map.Entry<ByteArrayKey, BlockedKeyStats> {
    private final ByteArrayKey key;
    private final BlockedKeyStats value;

    BlockedKeyEntry(ByteArrayKey key, BlockedKeyStats value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public ByteArrayKey getKey() {
      return key;
    }

    @Override
    public BlockedKeyStats getValue() {
      return value;
    }

    @Override
    public BlockedKeyStats setValue(BlockedKeyStats value) {
      throw new UnsupportedOperationException();
    }
  }

  /** Per-key state for a blocked key. Lives until the key is unblocked or evicted. */
  static class BlockedKeyStats {
    /** Sentinel for writes rejected on the fast path, where the value is never assembled. */
    static final int SIZE_UNKNOWN = -1;

    int blockedWriteCount;
    int maxObservedSizeBytes;
    final long firstBlockedTimestampMs;
    long blockedDurationMs;

    BlockedKeyStats(int assembledSizeBytes, long nowMs) {
      this.blockedWriteCount = 1;
      this.maxObservedSizeBytes = assembledSizeBytes;
      this.firstBlockedTimestampMs = nowMs;
    }

    private BlockedKeyStats(BlockedKeyStats other, long nowMs) {
      this.blockedWriteCount = other.blockedWriteCount;
      this.maxObservedSizeBytes = other.maxObservedSizeBytes;
      this.firstBlockedTimestampMs = other.firstBlockedTimestampMs;
      this.blockedDurationMs = nowMs - other.firstBlockedTimestampMs;
    }

    void update(int assembledSizeBytes) {
      blockedWriteCount++;
      maxObservedSizeBytes = Math.max(maxObservedSizeBytes, assembledSizeBytes);
    }

    BlockedKeyStats copy(long nowMs) {
      return new BlockedKeyStats(this, nowMs);
    }
  }

  /** Immutable snapshot of one reporting window. */
  static class BlockReport {
    final long windowDurationMs;
    final int blockedWriteCount;
    final int evictedKeyCount;
    final int limitBytes;
    final List<Map.Entry<ByteArrayKey, BlockedKeyStats>> topKeys;

    BlockReport(
        long windowDurationMs,
        int blockedWriteCount,
        int evictedKeyCount,
        int limitBytes,
        List<Map.Entry<ByteArrayKey, BlockedKeyStats>> topKeys) {
      this.windowDurationMs = windowDurationMs;
      this.blockedWriteCount = blockedWriteCount;
      this.evictedKeyCount = evictedKeyCount;
      this.limitBytes = limitBytes;
      this.topKeys = topKeys;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(
          String.format(
              "  Window: %ds | Blocked writes: %d | Blocked keys: %d | Limit: %s%s%n",
              windowDurationMs / 1000,
              blockedWriteCount,
              topKeys.size(),
              formatBytes(limitBytes),
              evictedKeyCount > 0 ? " | Evicted from tracking: " + evictedKeyCount : ""));
      sb.append("  Partial updates to these keys are being rejected because the record exceeds the size limit.\n");
      sb.append("  To restore writes, send a full put or a delete with a current timestamp for the key.\n");
      sb.append("  Blocked keys:\n");
      int rank = 1;
      for (Map.Entry<ByteArrayKey, BlockedKeyStats> entry: topKeys) {
        ByteArrayKey key = entry.getKey();
        BlockedKeyStats stats = entry.getValue();
        sb.append(
            String.format(
                "    #%d key=0x%s (%dB) | rejectedWrites=%d | maxSize=%s | blockedFor=%ds%n",
                rank++,
                ByteUtils.toHexString(key.getContent()),
                key.getContent().length,
                stats.blockedWriteCount,
                stats.maxObservedSizeBytes == BlockedKeyStats.SIZE_UNKNOWN
                    ? "unknown"
                    : formatBytes(stats.maxObservedSizeBytes),
                stats.blockedDurationMs / 1000));
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
