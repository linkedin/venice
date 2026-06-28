package com.linkedin.davinci.store;

import java.nio.ByteBuffer;
import java.util.Arrays;


/**
 * A {@link DelegatingStorageEngine} used during active-active conflict resolution for stores backed by the
 * merged value-RMD column family. For a given key, the value top-level read and the RMD top-level read both
 * resolve to the same merged record, so this wrapper serves both of them from a single
 * {@link StorageEngine#getValueAndReplicationMetadata} lookup instead of two separate
 * {@link StorageEngine#get}/{@link StorageEngine#getReplicationMetadata} reads.
 *
 * <p>Only the first top-level read of the record triggers the merged lookup; both value and RMD reads use the
 * identical (chunking-suffixed, when chunking is enabled) top-level key, so the second one is served from the
 * cached result. Chunk reads use different keys and therefore fall through to the delegate unchanged, so chunked
 * value/RMD reassembly behaves exactly as it does without this wrapper.
 *
 * <p>Not thread-safe: an instance is created per record and used by a single ingestion thread.
 */
public class MergedValueRmdReadOptimizingStorageEngine extends DelegatingStorageEngine {
  private final int targetPartition;
  private byte[] primaryKey;
  private byte[] cachedValue;
  private byte[] cachedRmd;
  private boolean loaded;

  public MergedValueRmdReadOptimizingStorageEngine(StorageEngine delegate, int targetPartition) {
    super(delegate);
    this.targetPartition = targetPartition;
  }

  @Override
  public byte[] get(int partitionId, ByteBuffer keyBuffer) {
    if (isTopLevelRead(partitionId, keyBuffer)) {
      return cachedValue;
    }
    return super.get(partitionId, keyBuffer);
  }

  @Override
  public byte[] getReplicationMetadata(int partitionId, ByteBuffer key) {
    if (isTopLevelRead(partitionId, key)) {
      return cachedRmd;
    }
    return super.getReplicationMetadata(partitionId, key);
  }

  /**
   * Returns true if {@code (partitionId, key)} is the record's top-level read, loading the merged record once on
   * the first such access. Chunk reads (which use different keys) return false and are delegated. The first
   * top-level access in conflict resolution is always the value or RMD top-level read; chunk reads only happen
   * afterwards, during reassembly, so the first key seen for the target partition is the primary key.
   */
  private boolean isTopLevelRead(int partitionId, ByteBuffer key) {
    if (partitionId != targetPartition) {
      return false;
    }
    byte[] keyBytes = toBytes(key);
    if (!loaded) {
      primaryKey = keyBytes;
      byte[][] valueAndRmd = getDelegate().getValueAndReplicationMetadata(partitionId, key);
      if (valueAndRmd != null) {
        cachedValue = valueAndRmd[0];
        cachedRmd = valueAndRmd[1];
      }
      loaded = true;
      return true;
    }
    return Arrays.equals(keyBytes, primaryKey);
  }

  private static byte[] toBytes(ByteBuffer key) {
    ByteBuffer duplicate = key.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }
}
