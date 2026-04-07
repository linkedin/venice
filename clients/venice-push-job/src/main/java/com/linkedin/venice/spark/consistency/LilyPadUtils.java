package com.linkedin.venice.spark.consistency;

import com.linkedin.venice.utils.consistency.DiffValidationUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;


/**
 * Core logic for the lily-pad VT consistency algorithm.
 *
 * <p>Given per-key record snapshots from two DCs, identifies keys where the two DCs disagree
 * despite both having had full information. This class is generic over the position type and
 * has no PubSub dependencies. See {@link LilyPadSnapshotBuilder} for building snapshots from PubSub.
 */
public final class LilyPadUtils {
  private LilyPadUtils() {
  }

  public enum InconsistencyType {
    MISSING_IN_DC0, MISSING_IN_DC1, VALUE_MISMATCH
  }

  /**
   * Per-key snapshot of a single VT record, capturing the value, the upstream position vector
   * that the leader used for its DCR decision, the global high-watermark at that moment, and
   * the logical timestamp used by DCR to pick the winner.
   *
   * @param <T> the position type — must be {@link Comparable} so the lily-pad algorithm can
   *            determine whether one DC's high-watermark covers another's position vector.
   */
  public static class KeyRecord<T extends Comparable<T>> {
    /** Hash of the raw value bytes for equality comparison, or null for DELETE (tombstone). */
    public final Integer valueHash;
    /** Upstream RT position per region, indexed by region ID. */
    public final List<T> upstreamRTPosition;
    /** Global max upstream RT position per region at the moment this record was written, indexed by region ID. */
    public final List<T> highWatermark;
    /** Logical timestamp used by Venice DCR; higher value wins. */
    public final long logicalTimestamp;
    /** This record's position in the VT partition. Useful for forensic lookup when reporting inconsistencies. */
    public final T vtPosition;

    public KeyRecord(
        Integer valueHash,
        List<T> upstreamRTPosition,
        List<T> highWatermark,
        long logicalTimestamp,
        T vtPosition) {
      this.valueHash = valueHash;
      this.upstreamRTPosition = upstreamRTPosition;
      this.highWatermark = highWatermark;
      this.logicalTimestamp = logicalTimestamp;
      this.vtPosition = vtPosition;
    }
  }

  /**
   * The per-key record history plus the final partition high-watermark.
   * The high-watermark is needed to distinguish genuine MISSING inconsistencies
   * when scanning live (non-completed) version topics.
   *
   * @param <T> the position type used in {@link KeyRecord}
   */
  public static class Snapshot<T extends Comparable<T>> {
    /** Per-key record history for this partition, keyed by 64-bit hash of the raw key bytes. */
    public final Map<Long, List<KeyRecord<T>>> keyRecords;
    /** Final running high-watermark at the end of the scan, indexed by region ID. */
    public final List<T> partitionHighWatermark;

    public Snapshot(Map<Long, List<KeyRecord<T>>> keyRecords, List<T> partitionHighWatermark) {
      this.keyRecords = keyRecords;
      this.partitionHighWatermark = partitionHighWatermark;
    }
  }

  /** A single detected VT inconsistency between two DCs for one key. */
  public static class Inconsistency<T extends Comparable<T>> {
    public final long keyHash;
    public final InconsistencyType type;
    /** Null when type is MISSING_IN_DC0. */
    public final KeyRecord<T> dc0Record;
    /** Null when type is MISSING_IN_DC1. */
    public final KeyRecord<T> dc1Record;

    public Inconsistency(long keyHash, InconsistencyType type, KeyRecord<T> dc0Record, KeyRecord<T> dc1Record) {
      this.keyHash = keyHash;
      this.type = type;
      this.dc0Record = dc0Record;
      this.dc1Record = dc1Record;
    }
  }

  /**
   * Runs the lily-pad algorithm over two pre-built snapshots and returns all detected
   * inconsistencies. An empty list means the two DCs agree on every comparable key.
   *
   * <p>Two records are comparable when each DC's global high-watermark covers every per-key
   * position the other DC considered — meaning both leaders had full information when writing.
   * Non-comparable pairs are skipped (replication lag, not a real inconsistency).
   *
   * @param dc0Snapshot     snapshot built by {@link LilyPadSnapshotBuilder#buildSnapshot} for DC-0
   * @param dc1Snapshot     snapshot built by {@link LilyPadSnapshotBuilder#buildSnapshot} for DC-1
   * @return list of {@link Inconsistency} objects, one per inconsistent key
   */
  public static <T extends Comparable<T>> List<Inconsistency<T>> findInconsistencies(
      Snapshot<T> dc0Snapshot,
      Snapshot<T> dc1Snapshot) {
    List<Inconsistency<T>> result = new ArrayList<>();

    Set<Long> allKeys = new TreeSet<>(dc0Snapshot.keyRecords.keySet());
    allKeys.addAll(dc1Snapshot.keyRecords.keySet());

    for (long keyHash: allKeys) {
      List<KeyRecord<T>> dc0History = dc0Snapshot.keyRecords.getOrDefault(keyHash, Collections.emptyList());
      List<KeyRecord<T>> dc1History = dc1Snapshot.keyRecords.getOrDefault(keyHash, Collections.emptyList());
      Inconsistency<T> inc = findInconsistencyForKey(
          keyHash,
          dc0History,
          dc1History,
          dc0Snapshot.partitionHighWatermark,
          dc1Snapshot.partitionHighWatermark);
      if (inc != null) {
        result.add(inc);
      }
    }
    return result;
  }

  /**
   * Runs the lily-pad comparison for a single key. Returns an {@link Inconsistency} if one is
   * detected, or {@code null} if the key is consistent (or not yet comparable due to replication lag).
   *
   * @param keyHash                  64-bit hash of the key being compared
   * @param dc0History               ordered record history for this key in DC-0 (may be empty)
   * @param dc1History               ordered record history for this key in DC-1 (may be empty)
   * @param dc0PartitionHighWatermark final partition-level high watermark from DC-0's snapshot
   * @param dc1PartitionHighWatermark final partition-level high watermark from DC-1's snapshot
   * @return an {@link Inconsistency} if detected, or {@code null} if consistent
   */
  public static <T extends Comparable<T>> Inconsistency<T> findInconsistencyForKey(
      long keyHash,
      List<KeyRecord<T>> dc0History,
      List<KeyRecord<T>> dc1History,
      List<T> dc0PartitionHighWatermark,
      List<T> dc1PartitionHighWatermark) {
    if (dc0History.isEmpty()) {
      KeyRecord<T> dc1Last = dc1History.get(dc1History.size() - 1);
      if (DiffValidationUtils.isRecordMissing(dc1Last.upstreamRTPosition, dc0PartitionHighWatermark)) {
        return new Inconsistency<>(keyHash, InconsistencyType.MISSING_IN_DC0, null, dc1Last);
      }
      return null;
    }
    if (dc1History.isEmpty()) {
      KeyRecord<T> dc0Last = dc0History.get(dc0History.size() - 1);
      if (DiffValidationUtils.isRecordMissing(dc0Last.upstreamRTPosition, dc1PartitionHighWatermark)) {
        return new Inconsistency<>(keyHash, InconsistencyType.MISSING_IN_DC1, dc0Last, null);
      }
      return null;
    }

    int iA = 0, iB = 0;
    while (iA < dc0History.size() && iB < dc1History.size()) {
      KeyRecord<T> a = dc0History.get(iA);
      KeyRecord<T> b = dc1History.get(iB);

      boolean aHwCoversB = DiffValidationUtils.hasOffsetAdvanced(b.upstreamRTPosition, a.highWatermark);
      boolean bHwCoversA = DiffValidationUtils.hasOffsetAdvanced(a.upstreamRTPosition, b.highWatermark);
      if (aHwCoversB && bHwCoversA) {
        if (!Objects.equals(a.valueHash, b.valueHash)) {
          return new Inconsistency<>(keyHash, InconsistencyType.VALUE_MISMATCH, a, b);
        }
        iA++;
        iB++;
      } else {
        boolean aTrailing = !aHwCoversB;
        boolean bTrailing = !bHwCoversA;
        if (aTrailing && bTrailing) {
          iA++;
          iB++;
        } else if (aTrailing) {
          iA++;
        } else {
          iB++;
        }
      }
    }
    // Any remaining records in the longer history are writes the other DC hasn't replicated yet.
    // This is replication lag, not inconsistency, so it's safe to stop here.
    return null;
  }

}
