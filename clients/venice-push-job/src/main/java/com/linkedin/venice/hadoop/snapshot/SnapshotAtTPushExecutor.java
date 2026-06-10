package com.linkedin.venice.hadoop.snapshot;

import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.snapshot.SnapshotAtTRecordMerger.KeyMergeState;
import com.linkedin.venice.hadoop.snapshot.SnapshotAtTRecordMerger.MergedRecord;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Produces the snapshot-at-T merged version: for every key, it seeds {@link SnapshotAtTRecordMerger} from the
 * offline batch value (if any), folds in that key's real-time (RT) records collected from all regions up to the
 * cutoff {@code T}, and writes the merged value + RMD (or an RMD-carrying delete tombstone) to the new version
 * topic via a {@link AbstractVeniceWriter}. The writer partitions each key itself, so no manual partitioning is
 * needed.
 *
 * <p>Start-Of-Push / End-Of-Push are sent by the caller around {@link #execute}; this class only produces the
 * data records. The shortened rewind is applied separately at version-creation time (the control plane).
 */
public class SnapshotAtTPushExecutor {
  private static final Logger LOGGER = LogManager.getLogger(SnapshotAtTPushExecutor.class);

  /** Counters describing what a run produced. */
  public static final class Stats {
    private long puts;
    private long deletes;
    private long batchOnlyKeys;
    private long rtTouchedKeys;

    public long getPuts() {
      return puts;
    }

    public long getDeletes() {
      return deletes;
    }

    public long getBatchOnlyKeys() {
      return batchOnlyKeys;
    }

    public long getRtTouchedKeys() {
      return rtTouchedKeys;
    }

    public long getTotalProduced() {
      return puts + deletes;
    }
  }

  /**
   * @param writer destination writer for the new version topic (caller owns its lifecycle and SOP/EOP)
   * @param batchValuesByKey offline batch values keyed by serialized key (no schema-id prefix); may be empty
   * @param batchValueSchemaId the value schema id the batch values are serialized with
   * @param rtRecords RT records from all regions, already read up to {@code T} and tagged with their colo id
   * @param merger the merge engine (its rmdUseFieldLevelTimestamp must match the store config)
   * @param compressor compresses each merged value to the version's compression strategy before it is written
   */
  public Stats execute(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      Map<ByteBuffer, ByteBuffer> batchValuesByKey,
      int batchValueSchemaId,
      List<SnapshotAtTRtRecord> rtRecords,
      SnapshotAtTRecordMerger merger,
      VeniceCompressor compressor) {
    Map<ByteBuffer, List<SnapshotAtTRtRecord>> rtByKey = new HashMap<>();
    for (SnapshotAtTRtRecord rt: rtRecords) {
      rtByKey.computeIfAbsent(rt.getKey(), k -> new ArrayList<>()).add(rt);
    }
    // Folding order does not change the converged DCR result, but a deterministic timestamp order keeps runs
    // reproducible and mirrors how a server applies writes.
    for (List<SnapshotAtTRtRecord> perKey: rtByKey.values()) {
      perKey.sort(Comparator.comparingLong(SnapshotAtTRtRecord::getWriteTimestamp));
    }

    Set<ByteBuffer> allKeys = new LinkedHashSet<>(batchValuesByKey.keySet());
    allKeys.addAll(rtByKey.keySet());

    Stats stats = new Stats();
    for (ByteBuffer key: allKeys) {
      ByteBuffer batchValue = batchValuesByKey.get(key);
      List<SnapshotAtTRtRecord> perKeyRt = rtByKey.get(key);

      KeyMergeState state = batchValue != null
          ? merger.seedFromBatch(batchValue.duplicate(), batchValueSchemaId)
          : merger.newState(batchValueSchemaId);
      if (batchValue != null && perKeyRt == null) {
        stats.batchOnlyKeys++;
      }
      if (perKeyRt != null) {
        stats.rtTouchedKeys++;
        for (SnapshotAtTRtRecord rt: perKeyRt) {
          switch (rt.getOp()) {
            case PUT:
              merger.applyPut(
                  state,
                  rt.getPayload().duplicate(),
                  rt.getValueSchemaId(),
                  rt.getWriteTimestamp(),
                  rt.getColoId());
              break;
            case UPDATE:
              merger.applyUpdate(
                  state,
                  rt.getPayload().duplicate(),
                  rt.getValueSchemaId(),
                  rt.getUpdateProtocolVersion(),
                  rt.getWriteTimestamp(),
                  rt.getColoId());
              break;
            case DELETE:
              merger.applyDelete(state, rt.getWriteTimestamp(), rt.getColoId());
              break;
            default:
              throw new IllegalStateException("Unexpected RT op: " + rt.getOp());
          }
        }
      }

      MergedRecord merged = merger.finalizeRecord(state);
      byte[] keyBytes = toByteArray(key);
      if (merged.isDelete()) {
        writer.delete(
            keyBytes,
            null,
            new DeleteMetadata(merged.getValueSchemaId(), merged.getRmdProtocolVersion(), merged.getRmd()));
        stats.deletes++;
      } else {
        writer.put(
            keyBytes,
            compress(compressor, toByteArray(merged.getValue())),
            merged.getValueSchemaId(),
            null,
            new PutMetadata(merged.getRmdProtocolVersion(), merged.getRmd()));
        stats.puts++;
      }
    }

    LOGGER.info(
        "Snapshot-at-T merge produced {} records ({} puts, {} deletes) from {} batch keys and {} RT-touched keys.",
        stats.getTotalProduced(),
        stats.puts,
        stats.deletes,
        batchValuesByKey.size(),
        stats.rtTouchedKeys);
    return stats;
  }

  private static byte[] compress(VeniceCompressor compressor, byte[] value) {
    try {
      return compressor.compress(value);
    } catch (IOException e) {
      throw new VeniceException("Failed to compress merged value for snapshot-at-T push", e);
    }
  }

  private static byte[] toByteArray(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    duplicate.rewind();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }
}
