package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


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
  private static final Logger LOGGER = LogManager.getLogger(MergedValueRmdRocksDBStoragePartition.class);

  /** Size of the header: 4 bytes for valueSchemaId + 4 bytes for valueLen */
  public static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES;

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
   * Writes a key with both value and replication metadata in the merged format.
   */
  @Override
  public synchronized void putWithReplicationMetadata(byte[] key, byte[] value, byte[] metadata) {
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make writes while database is opened in read-only mode for replica: " + replicaId);
    }
    byte[] mergedValue = mergeValueAndRmd(value, metadata);
    try {
      if (deferredWrite) {
        super.put(key, mergedValue);
      } else {
        rocksDB.put(writeOptions, key, mergedValue);
      }
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to put key/value pair to RocksDB: " + replicaId, e);
    }
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
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make writes while database is opened in read-only mode for replica: " + replicaId);
    }
    try {
      // Read existing merged record to extract the value portion
      byte[] existingRecord = rocksDB.get(key);
      byte[] valueBytes;
      if (existingRecord != null && existingRecord.length >= HEADER_SIZE) {
        int valueLen = ByteUtils.readInt(existingRecord, Integer.BYTES);
        valueBytes = new byte[HEADER_SIZE + valueLen];
        System.arraycopy(existingRecord, 0, valueBytes, 0, valueBytes.length);
      } else {
        // No existing value; create an empty value with schema ID 0
        valueBytes = new byte[HEADER_SIZE];
        ByteUtils.writeInt(valueBytes, 0, 0);
        ByteUtils.writeInt(valueBytes, 0, Integer.BYTES);
      }
      byte[] mergedRecord = new byte[valueBytes.length + metadata.length];
      System.arraycopy(valueBytes, 0, mergedRecord, 0, valueBytes.length);
      System.arraycopy(metadata, 0, mergedRecord, valueBytes.length, metadata.length);
      rocksDB.put(writeOptions, key, mergedRecord);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to put replication metadata to RocksDB: " + replicaId, e);
    }
  }

  /**
   * Retrieves only the replication metadata portion from the merged record.
   */
  @Override
  public byte[] getReplicationMetadata(ByteBuffer key) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      byte[] mergedRecord = rocksDB.get(READ_OPTIONS_DEFAULT, key.array(), key.position(), key.remaining());
      if (mergedRecord == null) {
        return null;
      }
      return extractRmd(mergedRecord);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to get replication metadata from RocksDB: " + replicaId, e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  /**
   * Retrieves both value and replication metadata from a single read.
   * Returns an array of two byte arrays: [value, rmd].
   * Returns null if the key does not exist.
   */
  public byte[][] getValueAndReplicationMetadata(ByteBuffer key) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      byte[] mergedRecord = rocksDB.get(READ_OPTIONS_DEFAULT, key.array(), key.position(), key.remaining());
      if (mergedRecord == null) {
        return null;
      }
      return splitMergedRecord(mergedRecord);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to get value and replication metadata from RocksDB: " + replicaId, e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
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
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      byte[] mergedRecord = rocksDB.get(key);
      if (mergedRecord == null) {
        return null;
      }
      return extractValue(mergedRecord);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to get value from RocksDB: " + replicaId, e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  @Override
  public byte[] get(ByteBuffer keyBuffer) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      byte[] mergedRecord = rocksDB.get(keyBuffer.array(), keyBuffer.position(), keyBuffer.remaining());
      if (mergedRecord == null) {
        return null;
      }
      return extractValue(mergedRecord);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to get value from RocksDB: " + replicaId, e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  /**
   * Deletes the value but preserves the RMD. In merged mode, this means writing
   * just the RMD with an empty value portion.
   */
  @Override
  public synchronized void deleteWithReplicationMetadata(byte[] key, byte[] replicationMetadata) {
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make writes while database is opened in read-only mode for replica: " + replicaId);
    }
    try {
      // Write a record with empty value but with RMD
      byte[] mergedRecord = new byte[HEADER_SIZE + replicationMetadata.length];
      // schemaId = 0 for deleted records
      ByteUtils.writeInt(mergedRecord, 0, 0);
      // valueLen = 0 for deleted records
      ByteUtils.writeInt(mergedRecord, 0, Integer.BYTES);
      System.arraycopy(replicationMetadata, 0, mergedRecord, HEADER_SIZE, replicationMetadata.length);

      if (deferredWrite) {
        super.put(key, mergedRecord);
      } else {
        rocksDB.put(writeOptions, key, mergedRecord);
      }
    } catch (RocksDBException e) {
      throw new VeniceException(
          "Failed to delete with replication metadata in RocksDB: " + replicaId,
          e);
    }
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
    if (valueLen == 0 && mergedRecord.length == HEADER_SIZE) {
      // Deleted record with no value
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
