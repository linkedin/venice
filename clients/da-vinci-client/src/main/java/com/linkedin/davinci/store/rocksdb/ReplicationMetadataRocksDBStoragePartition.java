package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.rocksdb.RocksDBSstFileWriter.DEFAULT_COLUMN_FAMILY_INDEX;
import static com.linkedin.davinci.store.rocksdb.RocksDBSstFileWriter.REPLICATION_METADATA_COLUMN_FAMILY_INDEX;

import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;


/**
 * This {@link ReplicationMetadataRocksDBStoragePartition} is built to store key value pair along with the timestamp
 * metadata. It is designed for active/active replication mode, which uses putWithReplicationMetadata and getReplicationMetadata
 * to insert and retrieve replication metadata associated with a key. The implementation relies on different column family
 * in RocksDB to isolate the value and replication metadata of a key.
 */
public class ReplicationMetadataRocksDBStoragePartition extends RocksDBStoragePartition {
  private static final Logger LOGGER = LogManager.getLogger(ReplicationMetadataRocksDBStoragePartition.class);

  // The value still uses "timestamp" for backward compatibility
  private RocksDBSstFileWriter rocksDBSstFileWriter = null;
  private final String fullPathForTempSSTFileDir;

  public ReplicationMetadataRocksDBStoragePartition(
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
        Arrays.asList(RocksDB.DEFAULT_COLUMN_FAMILY, REPLICATION_METADATA_COLUMN_FAMILY));
    this.fullPathForTempSSTFileDir = RocksDBUtils.composeTempRMDSSTFileDir(dbDir, storeNameAndVersion, partitionId);
    if (deferredWrite) {
      this.rocksDBSstFileWriter = new RocksDBSstFileWriter(
          storeNameAndVersion,
          partitionId,
          dbDir,
          super.getEnvOptions(),
          super.getOptions(),
          fullPathForTempSSTFileDir,
          true,
          rocksDBServerConfig);
    }
  }

  /**
   * Stores a key-value pair along with replication metadata. In deferred-write mode, the value is
   * written via the parent SST writer and metadata via this partition's SST writer. Otherwise,
   * both are written atomically via a {@link WriteBatch}.
   *
   * @throws VeniceException if the database is closed, read-only, or the write fails
   */
  @Override
  public synchronized void putWithReplicationMetadata(byte[] key, byte[] value, byte[] metadata) {
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make writes while database is opened in read-only mode for replica: " + replicaId);
    }

    if (deferredWrite) {
      try {
        super.put(key, value);
        rocksDBSstFileWriter.put(key, ByteBuffer.wrap(metadata));
      } catch (RocksDBException e) {
        throw new VeniceException("Failed to put key/value pair to RocksDB: " + replicaId, e);
      }
    } else {
      withSynchronizedDatabaseVoid(db -> {
        try (WriteBatch writeBatch = new WriteBatch()) {
          writeBatch.put(columnFamilyHandleList.get(DEFAULT_COLUMN_FAMILY_INDEX), key, value);
          writeBatch.put(columnFamilyHandleList.get(REPLICATION_METADATA_COLUMN_FAMILY_INDEX), key, metadata);
          db.write(writeOptions, writeBatch);
        }
      });
    }
  }

  /**
   * Stores replication metadata for a key. In deferred-write mode, writes via the SST writer;
   * otherwise writes directly to the replication metadata column family.
   *
   * @throws VeniceException if the database is closed, read-only, or the write fails
   */
  @Override
  public synchronized void putReplicationMetadata(byte[] key, byte[] metadata) {
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make writes while database is opened in read-only mode for replica: " + replicaId);
    }
    if (deferredWrite) {
      try {
        rocksDBSstFileWriter.put(key, ByteBuffer.wrap(metadata));
      } catch (RocksDBException e) {
        throw new VeniceException("Failed to put replication metadata to RocksDB: " + replicaId, e);
      }
    } else {
      withSynchronizedDatabaseVoid(
          db -> db
              .put(columnFamilyHandleList.get(REPLICATION_METADATA_COLUMN_FAMILY_INDEX), writeOptions, key, metadata));
    }
  }

  public long getRmdByteUsage() {
    return withOpenDatabase(
        db -> db.getColumnFamilyMetaData(columnFamilyHandleList.get(REPLICATION_METADATA_COLUMN_FAMILY_INDEX)).size());
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
  public byte[] getReplicationMetadata(ByteBuffer key) {
    return withOpenDatabase(
        db -> db.get(
            columnFamilyHandleList.get(REPLICATION_METADATA_COLUMN_FAMILY_INDEX),
            READ_OPTIONS_DEFAULT,
            key.array(),
            key.position(),
            key.remaining()));
  }

  /**
   * This API deletes a record from RocksDB but updates the metadata in ByteBuffer format and puts it into RocksDB.
   */
  /**
   * Deletes a key's value but updates its replication metadata. In deferred-write mode (repush),
   * only the metadata is written via the SST writer. Otherwise, the delete and metadata update
   * are applied atomically via a {@link WriteBatch}.
   *
   * @throws VeniceException if the database is closed, read-only, or the write fails
   */
  @Override
  public synchronized void deleteWithReplicationMetadata(byte[] key, byte[] replicationMetadata) {
    makeSureRocksDBIsStillOpen();
    if (readOnly) {
      throw new VeniceException(
          "Cannot make writes while database is opened in read-only mode for replica: " + replicaId);
    }
    if (deferredWrite) {
      // Just update the RMD for deletion during repush
      try {
        rocksDBSstFileWriter.put(key, ByteBuffer.wrap(replicationMetadata));
      } catch (RocksDBException e) {
        throw new VeniceException("Failed to put metadata while deleting key from RocksDB: " + replicaId, e);
      }
    } else {
      withSynchronizedDatabaseVoid(db -> {
        try (WriteBatch writeBatch = new WriteBatch()) {
          writeBatch.delete(columnFamilyHandleList.get(DEFAULT_COLUMN_FAMILY_INDEX), key);
          writeBatch
              .put(columnFamilyHandleList.get(REPLICATION_METADATA_COLUMN_FAMILY_INDEX), key, replicationMetadata);
          db.write(writeOptions, writeBatch);
        }
      });
    }
  }

  @Override
  public boolean checkDatabaseIntegrity(Map<String, String> checkpointedInfo) {
    makeSureRocksDBIsStillOpen();
    if (!deferredWrite) {
      LOGGER.info("checkDatabaseIntegrity will do nothing since 'deferredWrite' is disabled");
      return true;
    }
    return (super.checkDatabaseIntegrity(checkpointedInfo)
        && rocksDBSstFileWriter.checkDatabaseIntegrity(checkpointedInfo));
  }

  @Override
  public synchronized void beginBatchWrite(
      Map<String, String> checkpointedInfo,
      Optional<Supplier<byte[]>> expectedChecksumSupplier) {
    if (!deferredWrite) {
      LOGGER.info("'beginBatchWrite' will do nothing since 'deferredWrite' is disabled");
      return;
    }
    super.beginBatchWrite(checkpointedInfo, expectedChecksumSupplier);
    rocksDBSstFileWriter.open(checkpointedInfo, expectedChecksumSupplier);
  }

  @Override
  public synchronized void endBatchWrite() {
    super.endBatchWrite();

    if (deferredWrite) {
      withSynchronizedDatabaseVoid(db -> rocksDBSstFileWriter.ingestSSTFiles(db, getColumnFamilyHandleList()));
    }
  }

  @Override
  public synchronized Map<String, String> sync() {
    Map<String, String> checkpointingInfo = super.sync();
    // if deferredWrite is false, super.sync will flush both the column families
    if (deferredWrite) {
      checkpointingInfo.putAll(rocksDBSstFileWriter.sync());
    }
    return checkpointingInfo;
  }

  @Override
  public synchronized void close() {
    super.close();
    if (deferredWrite) {
      rocksDBSstFileWriter.close();
    }
  }

  @Override
  public synchronized boolean validateBatchIngestion() {
    if (!deferredWrite) {
      return true;
    }
    if (!super.validateBatchIngestion()) {
      return false;
    }
    return rocksDBSstFileWriter.validateBatchIngestion();
  }

  @Override
  public synchronized void drop() {
    super.deleteFilesInDirectory(fullPathForTempSSTFileDir);
    super.drop();
  }

  // Visible for testing
  public String getFullPathForTempSSTFileDir() {
    return fullPathForTempSSTFileDir;
  }

  // Visible for testing
  public RocksDBSstFileWriter getRocksDBSstFileWriter() {
    return rocksDBSstFileWriter;
  }

  // Visible for testing
  public String getValueFullPathForTempSSTFileDir() {
    return super.getFullPathForTempSSTFileDir();
  }

  // Visible for testing
  public RocksDBSstFileWriter getValueRocksDBSstFileWriter() {
    return super.getRocksDBSstFileWriter();
  }
}
