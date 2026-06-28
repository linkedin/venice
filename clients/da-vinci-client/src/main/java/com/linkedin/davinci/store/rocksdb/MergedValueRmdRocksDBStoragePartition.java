package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.rocksdb.RocksDB;


/**
 * A storage partition that stores both value and replication metadata (RMD) in a single
 * RocksDB column family. This reduces read amplification during A/A ingestion by eliminating
 * the need for two separate RocksDB get() calls.
 *
 * Storage format for each record:
 * <pre>
 * +------------------+-------------+------------+-----------+
 * | valueSchemaId 4B | valueLen 4B | valueBytes | rmdBytes  |
 * +------------------+-------------+------------+-----------+
 * </pre>
 *
 * - Client reads: Read first 8 bytes (schemaId + valueLen), then extract valueBytes. RMD is ignored.
 * - A/A ingestion reads: Parse all fields to get both value and RMD.
 * - RMD length is implicit: totalLength - 8 - valueLen.
 * - When RMD is empty (deleted key or non-A/A): rmdBytes section is empty.
 */
public class MergedValueRmdRocksDBStoragePartition extends RocksDBStoragePartition {
  /** Size of the header: 4 bytes for valueSchemaId + 4 bytes for valueLen */
  public static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES;

  private static final byte[] EMPTY_RMD = new byte[0];

  public MergedValueRmdRocksDBStoragePartition(
      StoragePartitionConfig storagePartitionConfig,
      RocksDBStorageEngineFactory factory,
      String dbDir,
      RocksDBMemoryStats rocksDBMemoryStats,
      RocksDBThrottler rocksDbThrottler,
      RocksDBServerConfig rocksDBServerConfig) {
    super(
        storagePartitionConfig,
        factory,
        dbDir,
        rocksDBMemoryStats,
        rocksDbThrottler,
        rocksDBServerConfig,
        Collections.singletonList(RocksDB.DEFAULT_COLUMN_FAMILY));
  }

  /**
   * The merged value-RMD format rewrites each record as {@code [schemaId][valueLen][value][rmd]}, so the
   * bytes persisted to the SST file intentionally differ from the producer's original value bytes. The
   * push-time SST checksum is computed by the producer over those original value bytes, so it can never
   * match the merged SST contents. Skip it by passing an empty checksum supplier, analogous to how the
   * separate replication-metadata column family is excluded from SST checksum verification.
   */
  @Override
  public synchronized void beginBatchWrite(
      Map<String, String> checkpointedInfo,
      Optional<Supplier<byte[]>> expectedChecksumSupplier) {
    super.beginBatchWrite(checkpointedInfo, Optional.empty());
  }

  /**
   * Overrides the standard put() to store value in the merged format with empty RMD.
   * This ensures that data written during batch push (which uses put() instead of
   * putWithReplicationMetadata()) is stored in the correct merged format so that
   * subsequent get() calls can correctly parse it.
   */
  @Override
  public synchronized void put(byte[] key, byte[] value) {
    putMergedRecord(key, mergeValueAndRmd(value, EMPTY_RMD));
  }

  @Override
  public synchronized void put(byte[] key, ByteBuffer valueBuffer) {
    put(key, ByteUtils.extractByteArray(valueBuffer));
  }

  /**
   * Writes a key with both value and replication metadata in the merged format.
   */
  @Override
  public synchronized void putWithReplicationMetadata(byte[] key, byte[] value, byte[] metadata) {
    putMergedRecord(key, mergeValueAndRmd(value, metadata));
  }

  /**
   * Writes an already-merged record to storage. Delegates to the superclass {@link ByteBuffer} write,
   * which honors deferred-write (SST) vs direct RocksDB mode along with the read/close lifecycle guards.
   * Routing through {@code super.put} (a non-virtual call) is required to avoid re-entering this class's
   * overridden {@link #put(byte[], byte[])} / {@link #put(byte[], ByteBuffer)} methods, which would
   * otherwise recurse infinitely in deferred-write (batch) mode.
   */
  private void putMergedRecord(byte[] key, byte[] mergedRecord) {
    super.put(key, ByteBuffer.wrap(mergedRecord));
  }

  @Override
  public synchronized void putWithReplicationMetadata(byte[] key, ByteBuffer value, byte[] metadata) {
    byte[] valueBytes = ByteUtils.extractByteArray(value);
    putWithReplicationMetadata(key, valueBytes, metadata);
  }

  /**
   * Writes only replication metadata for a key. In merged mode, we need to read the existing
   * value (if any) and re-write with the new RMD.
   */
  @Override
  public synchronized void putReplicationMetadata(byte[] key, byte[] metadata) {
    if (deferredWrite) {
      // putReplicationMetadata performs a read-modify-write to preserve the existing value, which
      // cannot observe records still buffered in the SST writer during deferred-write (batch) mode.
      throw new VeniceException(
          "putReplicationMetadata is not supported in deferred-write mode for the merged value-RMD column family, replica: "
              + replicaId);
    }
    // Read the existing merged record to preserve its value portion, then re-write it with the new RMD.
    byte[] existingRecord = withOpenDatabase(db -> db.get(key));
    byte[] valueBytes = extractValueHeaderAndBytes(existingRecord);
    byte[] mergedRecord = new byte[valueBytes.length + metadata.length];
    System.arraycopy(valueBytes, 0, mergedRecord, 0, valueBytes.length);
    System.arraycopy(metadata, 0, mergedRecord, valueBytes.length, metadata.length);
    putMergedRecord(key, mergedRecord);
  }

  /**
   * Returns the {@code [schemaId 4B][valueLen 4B][valueBytes]} prefix of an existing merged record,
   * with the RMD section stripped off. If the record is null, shorter than the header, or its declared
   * value length is inconsistent with the stored bytes, an empty tombstone header
   * (schemaId=0, valueLen=0) is returned instead of throwing.
   */
  private static byte[] extractValueHeaderAndBytes(byte[] mergedRecord) {
    if (mergedRecord != null && mergedRecord.length >= HEADER_SIZE) {
      int valueLen = ByteUtils.readInt(mergedRecord, Integer.BYTES);
      if (valueLen >= 0 && HEADER_SIZE + valueLen <= mergedRecord.length) {
        byte[] valueBytes = new byte[HEADER_SIZE + valueLen];
        System.arraycopy(mergedRecord, 0, valueBytes, 0, valueBytes.length);
        return valueBytes;
      }
    }
    byte[] valueBytes = new byte[HEADER_SIZE];
    ByteUtils.writeInt(valueBytes, 0, 0);
    ByteUtils.writeInt(valueBytes, 0, Integer.BYTES);
    return valueBytes;
  }

  /**
   * Retrieves only the replication metadata portion from the merged record.
   */
  @Override
  public byte[] getReplicationMetadata(ByteBuffer key) {
    return withOpenDatabase(db -> {
      byte[] mergedRecord = db.get(READ_OPTIONS_DEFAULT, key.array(), key.position(), key.remaining());
      return mergedRecord == null ? null : extractRmd(mergedRecord);
    });
  }

  /**
   * Retrieves both value and replication metadata from a single read.
   * Returns an array of two byte arrays: [value, rmd].
   * Returns null if the key does not exist.
   */
  @Override
  public byte[][] getValueAndReplicationMetadata(ByteBuffer key) {
    return withOpenDatabase(db -> {
      byte[] mergedRecord = db.get(READ_OPTIONS_DEFAULT, key.array(), key.position(), key.remaining());
      return mergedRecord == null ? null : splitMergedRecord(mergedRecord);
    });
  }

  /**
   * For client reads, the get() method returns the value portion only (including the schema ID prefix).
   * Since the first 4 bytes are already the valueSchemaId and the next 4 bytes are valueLen,
   * we extract [valueSchemaId][valueBytes] which is what client reads expect.
   *
   * However, for the ingestion path which uses get() to read the full value (with schemaId prefix),
   * the merged format already has schemaId as the first 4 bytes.
   * The value portion is: bytes[0..HEADER_SIZE+valueLen-1], but we need to strip out the valueLen field
   * and return [schemaId 4B][valueBytes].
   */
  @Override
  public byte[] get(byte[] key) {
    return withOpenDatabase(db -> {
      byte[] mergedRecord = db.get(key);
      return mergedRecord == null ? null : extractValue(mergedRecord);
    });
  }

  @Override
  public byte[] get(ByteBuffer keyBuffer) {
    return withOpenDatabase(db -> {
      byte[] mergedRecord = db.get(keyBuffer.array(), keyBuffer.position(), keyBuffer.remaining());
      return mergedRecord == null ? null : extractValue(mergedRecord);
    });
  }

  /**
   * The reuse-buffer read path (used by {@link com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter},
   * e.g. server-side single-get/compute) calls this overload. The parent copies the raw stored bytes verbatim; for
   * the merged format those bytes carry the extra {@code [valueLen][rmdBytes]}, so the header must be stripped to
   * the standard {@code [schemaId][value]} format before populating the buffer, otherwise schema-id parsing and
   * value deserialization break.
   */
  @Override
  public ByteBuffer get(byte[] key, ByteBuffer valueToBePopulated) {
    byte[] value = get(key);
    if (value == null) {
      return null;
    }
    ByteBuffer target =
        valueToBePopulated.capacity() >= value.length ? valueToBePopulated : ByteBuffer.allocate(value.length);
    target.clear();
    target.put(value);
    target.flip();
    return target;
  }

  /**
   * Deletes the value but preserves the RMD. In merged mode, this means writing
   * just the RMD with an empty value portion.
   */
  @Override
  public synchronized void deleteWithReplicationMetadata(byte[] key, byte[] replicationMetadata) {
    // Write a tombstone (schemaId=0, valueLen=0) that preserves the RMD bytes.
    putMergedRecord(key, mergeValueAndRmd(null, replicationMetadata));
  }

  /**
   * Merges value bytes and RMD bytes into the combined format.
   * Value bytes are expected to already contain the schemaId prefix (first 4 bytes).
   *
   * Input value format: [valueSchemaId 4B][actualValueBytes...]
   * Output merged format: [valueSchemaId 4B][valueLen 4B][actualValueBytes...][rmdBytes...]
   *
   * where valueLen = length of actualValueBytes (i.e., value.length - 4)
   */
  static byte[] mergeValueAndRmd(byte[] value, byte[] rmd) {
    if (value == null || value.length < Integer.BYTES) {
      // No value or invalid value: write header + RMD only
      byte[] result = new byte[HEADER_SIZE + (rmd != null ? rmd.length : 0)];
      ByteUtils.writeInt(result, 0, 0); // schemaId = 0
      ByteUtils.writeInt(result, 0, Integer.BYTES); // valueLen = 0
      if (rmd != null && rmd.length > 0) {
        System.arraycopy(rmd, 0, result, HEADER_SIZE, rmd.length);
      }
      return result;
    }

    int schemaId = ByteUtils.readInt(value, 0);
    int actualValueLen = value.length - Integer.BYTES;
    int rmdLen = (rmd != null) ? rmd.length : 0;
    byte[] result = new byte[HEADER_SIZE + actualValueLen + rmdLen];

    ByteUtils.writeInt(result, schemaId, 0);
    ByteUtils.writeInt(result, actualValueLen, Integer.BYTES);
    if (actualValueLen > 0) {
      System.arraycopy(value, Integer.BYTES, result, HEADER_SIZE, actualValueLen);
    }
    if (rmdLen > 0) {
      System.arraycopy(rmd, 0, result, HEADER_SIZE + actualValueLen, rmdLen);
    }
    return result;
  }

  /**
   * Extracts the value portion from a merged record.
   * Returns [valueSchemaId 4B][actualValueBytes...] which is the standard value format.
   */
  static byte[] extractValue(byte[] mergedRecord) {
    if (mergedRecord == null || mergedRecord.length < HEADER_SIZE) {
      return null;
    }
    int schemaId = ByteUtils.readInt(mergedRecord, 0);
    int valueLen = ByteUtils.readInt(mergedRecord, Integer.BYTES);
    // A corrupt or out-of-bounds length is treated as missing.
    if (valueLen < 0 || HEADER_SIZE + valueLen > mergedRecord.length) {
      return null;
    }
    // A delete tombstone is the sentinel header schemaId==0 && valueLen==0 (written by deleteWithReplicationMetadata);
    // it reads back as null even when RMD bytes follow. A legitimate value with an empty payload (schemaId>0,
    // valueLen==0) still round-trips as a 4-byte [schemaId].
    if (valueLen == 0 && schemaId == 0) {
      return null;
    }
    // Reconstruct the standard value format: [schemaId 4B][actualValueBytes]
    byte[] value = new byte[Integer.BYTES + valueLen];
    ByteUtils.writeInt(value, schemaId, 0);
    if (valueLen > 0) {
      System.arraycopy(mergedRecord, HEADER_SIZE, value, Integer.BYTES, valueLen);
    }
    return value;
  }

  /**
   * Extracts the RMD portion from a merged record.
   */
  static byte[] extractRmd(byte[] mergedRecord) {
    if (mergedRecord == null || mergedRecord.length < HEADER_SIZE) {
      return null;
    }
    int valueLen = ByteUtils.readInt(mergedRecord, Integer.BYTES);
    if (valueLen < 0 || HEADER_SIZE + valueLen > mergedRecord.length) {
      // Corrupt length, or a value-only record with no RMD section.
      return null;
    }
    int rmdOffset = HEADER_SIZE + valueLen;
    int rmdLen = mergedRecord.length - rmdOffset;
    if (rmdLen <= 0) {
      return null;
    }
    byte[] rmd = new byte[rmdLen];
    System.arraycopy(mergedRecord, rmdOffset, rmd, 0, rmdLen);
    return rmd;
  }

  /**
   * Splits a merged record into [value, rmd].
   * Value is in standard format: [schemaId 4B][actualValueBytes]
   */
  static byte[][] splitMergedRecord(byte[] mergedRecord) {
    byte[] value = extractValue(mergedRecord);
    byte[] rmd = extractRmd(mergedRecord);
    return new byte[][] { value, rmd };
  }
}
