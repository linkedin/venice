package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;


/**
 * This {@link TimestampMetadataRocksDBStoragePartition} is built to store key value pair along with the timestamp
 * metadata. It is designed for active/active replication mode, which uses putWithTimestampMetadata and getTimestampMetadata
 * to insert and retrieve timestamp metadata associated with a key. The implementation relies on different column family
 * in RocksDB to isolate the value and timestamp metadata of a key.
 */
public class TimestampMetadataRocksDBStoragePartition extends RocksDBStoragePartition {
  private static final byte[] TIMESTAMP_METADATA_COLUMN_FAMILY = "timestamp_metadata".getBytes();
  private static final int DEFAULT_COLUMN_FAMILY_INDEX = 0;
  private static final int REPLICATION_METADATA_COLUMN_FAMILY_INDEX = 1;

  public TimestampMetadataRocksDBStoragePartition(StoragePartitionConfig storagePartitionConfig,
      RocksDBStorageEngineFactory factory, String dbDir, RocksDBMemoryStats rocksDBMemoryStats,
      RocksDBThrottler rocksDbThrottler, RocksDBServerConfig rocksDBServerConfig) {
    super(storagePartitionConfig, factory, dbDir, rocksDBMemoryStats, rocksDbThrottler, rocksDBServerConfig,
        Arrays.asList(RocksDB.DEFAULT_COLUMN_FAMILY, TIMESTAMP_METADATA_COLUMN_FAMILY));
  }

  @Override
  public synchronized void putWithReplicationMetadata(byte[] key, byte[] value, byte[] metadata) {
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make writes while partition is opened in read-only mode" + ", partition=" + storeName + "_" + partitionId);
    }

    try (WriteBatch writeBatch = new WriteBatch()) {
      writeBatch.put(columnFamilyHandleList.get(DEFAULT_COLUMN_FAMILY_INDEX), key, value);
      writeBatch.put(columnFamilyHandleList.get(REPLICATION_METADATA_COLUMN_FAMILY_INDEX), key, metadata);
      rocksDB.write(writeOptions, writeBatch);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to put key/value pair to store: " + storeName + ", partition id: " + partitionId, e);
    }
  }

  /**
   * This API takes in value and metadata as ByteBuffer format and put it into RocksDB.
   * Note that it is not an efficient implementation as it copies the content to perform the ByteBuffer -> byte[] conversion.
   * TODO: Rewrite this implementation after we adopt the thread-local direct bytebuffer approach.
   */
  @Override
  public synchronized void putWithReplicationMetadata(byte[] key, ByteBuffer value, byte[] metadata) {
    byte[] valueBytes = ByteUtils.extractByteArray(value);
    putWithReplicationMetadata(key, valueBytes, metadata);
  }

  @Override
  public byte[] getReplicationMetadata(byte[] key) {
    readCloseRWLock.readLock().lock();
    try {
      makeSureRocksDBIsStillOpen();
      return rocksDB.get(columnFamilyHandleList.get(REPLICATION_METADATA_COLUMN_FAMILY_INDEX), key);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to get value from store: " + storeName + ", partition id: " + partitionId, e);
    } finally {
      readCloseRWLock.readLock().unlock();
    }
  }

  /**
   * This API deletes a record from RocksDB but updates the metadata in ByteBuffer format and puts it into RocksDB.
   */
  @Override
  public synchronized void deleteWithReplicationMetadata(byte[] key, byte[] replicationMetadata) {
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make writes while partition is opened in read-only mode" + ", partition=" + storeName + "_" + partitionId);
    }
    if (deferredWrite) {
      throw new VeniceException("Deletion is unexpected in 'deferredWrite' mode");
    } else {
      try (WriteBatch writeBatch = new WriteBatch()) {
        writeBatch.delete(columnFamilyHandleList.get(DEFAULT_COLUMN_FAMILY_INDEX), key);
        writeBatch.put(columnFamilyHandleList.get(REPLICATION_METADATA_COLUMN_FAMILY_INDEX), key, replicationMetadata);
        rocksDB.write(writeOptions, writeBatch);
      } catch (RocksDBException e) {
        throw new VeniceException("Failed to delete entry to store: " + storeName + ", partition id: " + partitionId, e);
      }
    }
  }
}
