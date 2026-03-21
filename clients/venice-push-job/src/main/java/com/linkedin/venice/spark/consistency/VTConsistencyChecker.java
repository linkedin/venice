package com.linkedin.venice.spark.consistency;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.consistency.DiffValidationUtils;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.codec.digest.DigestUtils;


/**
 * Core logic for the lily-pad VT consistency algorithm.
 *
 * <p>Consumes a single VT partition from two DCs, builds a per-key record snapshot for each,
 * and identifies keys where the two DCs disagree despite both having had full information.
 *
 * <p>This class is intentionally free of Spark and test infrastructure, so it can be used
 * both from the VT consistency Spark job and from integration tests.
 */
public final class VTConsistencyChecker {
  private VTConsistencyChecker() {
  }

  public enum InconsistencyType {
    MISSING_IN_DC0, MISSING_IN_DC1, VALUE_MISMATCH
  }

  /**
   * Per-key snapshot of a single VT record, capturing the value, the upstream offset vector
   * that the leader used for its DCR decision, the global high-watermark at that moment, and
   * the logical timestamp used by DCR to pick the winner.
   */
  public static class KeyRecord {
    /** SHA-256 hex digest of the raw value bytes. Used for equality comparison across DCs. */
    public final String valueHash;
    /** Upstream RT offset per colo; index = colo ID, -1 means not yet seen. */
    public final List<Long> upstreamRTOffset;
    /** Global max upstream RT offset per colo at the moment this record was written; index = colo ID. */
    public final List<Long> highWatermark;
    /** Logical timestamp used by Venice DCR; higher value wins. */
    public final long logicalTimestamp;
    /** String representation of this record's position in the VT partition. Useful for direct forensic lookup. */
    public final String vtPosition;

    public KeyRecord(
        String valueHash,
        List<Long> upstreamRTOffset,
        List<Long> highWatermark,
        long logicalTimestamp,
        String vtPosition) {
      this.valueHash = valueHash;
      this.upstreamRTOffset = upstreamRTOffset;
      this.highWatermark = highWatermark;
      this.logicalTimestamp = logicalTimestamp;
      this.vtPosition = vtPosition;
    }
  }

  /**
   * Result of {@link #buildSnapshot}: the per-key record history plus the final partition
   * high-watermark. The high-watermark is needed to distinguish genuine MISSING inconsistencies
   * when scanning live (non-completed) version topics.
   */
  public static class Snapshot {
    /** Per-key record history for this partition, keyed by decoded key string. */
    public final Map<String, List<KeyRecord>> keyRecords;
    /** Final running high-watermark at the end of the scan; index = colo ID. */
    public final List<Long> partitionHighWatermark;

    public Snapshot(Map<String, List<KeyRecord>> keyRecords, List<Long> partitionHighWatermark) {
      this.keyRecords = keyRecords;
      this.partitionHighWatermark = partitionHighWatermark;
    }
  }

  /** A single detected VT inconsistency between two DCs for one key. */
  public static class Inconsistency {
    public final String key;
    public final InconsistencyType type;
    /** Null when type is MISSING_IN_DC0. */
    public final KeyRecord dc0Record;
    /** Null when type is MISSING_IN_DC1. */
    public final KeyRecord dc1Record;

    public Inconsistency(String key, InconsistencyType type, KeyRecord dc0Record, KeyRecord dc1Record) {
      this.key = key;
      this.type = type;
      this.dc0Record = dc0Record;
      this.dc1Record = dc1Record;
    }
  }

  /**
   * Iterates over a pre-built {@link PubSubSplitIterator} and returns a per-key history of PUT
   * records in the order they appeared in the VT partition.
   *
   * <p>The caller is responsible for constructing the iterator (including consumer creation and
   * position discovery) and for closing it after this method returns.
   *
   * <p>The running high-watermark is computed in Kafka offset order, so each {@link KeyRecord}'s
   * {@code highWatermark} reflects global RT progress up to that point.
   *
   * @param iterator a ready-to-read iterator over a single VT partition split
   * @return {@link Snapshot} containing the per-key record map and the final partition high-watermark
   */
  public static Snapshot buildSnapshot(PubSubSplitIterator iterator) {
    Map<String, List<KeyRecord>> snapshot = new TreeMap<>();

    // Two colos in every AA topology this checker targets; index = colo ID, -1 = not yet seen.
    List<Long> runningHighWatermark = new ArrayList<>(Collections.nCopies(2, -1L));

    try {
      PubSubSplitIterator.PubSubInputRecord record;
      while ((record = iterator.next()) != null) {
        DefaultPubSubMessage msg = record.getPubSubMessage();
        KafkaMessageEnvelope kme = msg.getValue();
        if (MessageType.valueOf(kme) != MessageType.PUT) {
          continue;
        }
        LeaderMetadata leaderMetadata = kme.leaderMetadataFooter;
        if (leaderMetadata == null) {
          continue;
        }
        int coloId = leaderMetadata.upstreamKafkaClusterId;
        long upstreamOffset = leaderMetadata.upstreamOffset;
        runningHighWatermark.set(coloId, Math.max(runningHighWatermark.get(coloId), upstreamOffset));
        String key = DigestUtils.sha256Hex(msg.getKey().getKey());
        String valueHash = DigestUtils.sha256Hex(ByteUtils.extractByteArray(((Put) kme.payloadUnion).putValue));
        List<KeyRecord> history = snapshot.computeIfAbsent(key, k -> new ArrayList<>());
        // Inherit previous offset vector for this key, then update only the current colo.
        List<Long> offsetVector = history.isEmpty()
            ? new ArrayList<>(Collections.nCopies(2, -1L))
            : new ArrayList<>(history.get(history.size() - 1).upstreamRTOffset);
        offsetVector.set(coloId, upstreamOffset);
        history.add(
            new KeyRecord(
                valueHash,
                offsetVector,
                new ArrayList<>(runningHighWatermark),
                kme.producerMetadata.logicalTimestamp,
                msg.getPosition().toString()));
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to scan " + iterator.getTopicPartition(), e);
    }
    return new Snapshot(snapshot, runningHighWatermark);
  }

  /**
   * Runs the lily-pad algorithm over two pre-built snapshots and returns all detected
   * inconsistencies. An empty list means the two DCs agree on every comparable key.
   *
   * <p>Two records are comparable when each DC's global high-watermark covers every per-key
   * offset the other DC considered — meaning both leaders had full information when writing.
   * Non-comparable pairs are skipped (replication lag, not a real inconsistency).
   *
   * @param dc0Snapshot snapshot built by {@link #buildSnapshot} for DC-0
   * @param dc1Snapshot snapshot built by {@link #buildSnapshot} for DC-1
   * @return list of {@link Inconsistency} objects, one per inconsistent key
   */
  public static List<Inconsistency> findInconsistencies(Snapshot dc0Snapshot, Snapshot dc1Snapshot) {
    List<Inconsistency> result = new ArrayList<>();

    Set<String> allKeys = new TreeSet<>(dc0Snapshot.keyRecords.keySet());
    allKeys.addAll(dc1Snapshot.keyRecords.keySet());

    for (String key: allKeys) {
      List<KeyRecord> histA = dc0Snapshot.keyRecords.getOrDefault(key, Collections.emptyList());
      List<KeyRecord> histB = dc1Snapshot.keyRecords.getOrDefault(key, Collections.emptyList());

      if (histA.isEmpty()) {
        KeyRecord dc1Last = histB.get(histB.size() - 1);
        if (DiffValidationUtils.isRecordMissing(dc1Last.upstreamRTOffset, dc0Snapshot.partitionHighWatermark)) {
          result.add(new Inconsistency(key, InconsistencyType.MISSING_IN_DC0, null, dc1Last));
        }
        continue;
      }
      if (histB.isEmpty()) {
        KeyRecord dc0Last = histA.get(histA.size() - 1);
        if (DiffValidationUtils.isRecordMissing(dc0Last.upstreamRTOffset, dc1Snapshot.partitionHighWatermark)) {
          result.add(new Inconsistency(key, InconsistencyType.MISSING_IN_DC1, dc0Last, null));
        }
        continue;
      }

      int iA = 0, iB = 0;
      while (iA < histA.size() && iB < histB.size()) {
        KeyRecord a = histA.get(iA);
        KeyRecord b = histB.get(iB);

        boolean aHwCoversB = DiffValidationUtils.hasOffsetAdvanced(b.upstreamRTOffset, a.highWatermark);
        boolean bHwCoversA = DiffValidationUtils.hasOffsetAdvanced(a.upstreamRTOffset, b.highWatermark);
        if (aHwCoversB && bHwCoversA) {
          if (!a.valueHash.equals(b.valueHash)) {
            result.add(new Inconsistency(key, InconsistencyType.VALUE_MISMATCH, a, b));
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
    }
    return result;
  }

}
