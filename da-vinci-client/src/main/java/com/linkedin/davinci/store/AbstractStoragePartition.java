package com.linkedin.davinci.store;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.store.rocksdb.ReplicationMetadataRocksDBStoragePartition;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An abstract implementation of a storage partition. This could be a database in BDB
 * environment or a concurrent hashmap in the case of an in-memory implementation depending
 * on the storage-partition model.
 */
public abstract class AbstractStoragePartition {
  protected final Logger logger = LogManager.getLogger(getClass());
  protected final Integer partitionId;

  public AbstractStoragePartition(Integer partitionId) { this.partitionId = partitionId; }

  /**
   * returns the id of this partition
   */
  public Integer getPartitionId() {
    return this.partitionId;
  }

  /**
   * Puts a value into the partition database
   */
  public abstract void put(byte[] key, byte[] value);

  public abstract void put(byte[] key, ByteBuffer value);

  public abstract <K, V> void put(K key, V value);

  /**
   * Get a value from the partition database
   * @param key key to be retrieved
   * @param skipCache
   * @return null if the key does not exist, byte[] value if it exists.
   */
  public abstract byte[] get(byte[] key, boolean skipCache);

  public ByteBuffer get(byte[] key, ByteBuffer valueToBePopulated, boolean skipCache) {
    // Naive default impl is not optimized... only storage engines that support the optimization implement it.
    return ByteBuffer.wrap(get(key, skipCache));
  }

  /**
   * Get a Value from the partition database
   * @param <K> the type for Key
   * @param <V> the type for the return value
   * @param key key to be retrieved
   * @param skipCache
   * @return null if the key does not exist, V value if it exists
   */
  public abstract <K, V> V get(K key, boolean skipCache);

  public abstract byte[] get(ByteBuffer key, boolean skipCache);

  /**
   * Populate provided callback with all key-value pairs from the partition database where the keys have
   * the provided prefix
   * @param keyPrefix
   * @param callback
   */
  public abstract void getByKeyPrefix(byte[] keyPrefix, BytesStreamingCallback callback);

  /**
   * Delete a key from the partition database
   */
  public abstract void delete(byte[] key);

  /**
   * Sync current database.
   *
   * @return Database related info, which is required to be checkpointed.
   */
  public abstract Map<String, String> sync();

  /**
   * Drop when it is not required anymore.
   */
  public abstract void drop();

  /**
   * Close the specific partition
   */
  public abstract void close();

  /**
   * Check whether current storage partition verifyConfig the given partition config
   * @param storagePartitionConfig
   * @return
   */
  public abstract boolean verifyConfig(StoragePartitionConfig storagePartitionConfig);

  public void beginBatchWrite(Map<String, String> checkpointedInfo, Optional<Supplier<byte[]>> checksumSupplier) {}

  public void endBatchWrite() {}

  /**
   * Get the partition database size in bytes
   * @return partition database size
   */
  public abstract long getPartitionSizeInBytes();

  public boolean validateBatchIngestion() {
    return true;
  }

  /**
   * Warm-up the database.
   */
  public void warmUp() {
    // Do nothing by default
    logger.info("Warming up is not implemented by default");
  }

  /**
   * One-time database compaction.
   * @return CompletableFuture to track the compaction progress.
   */
  public CompletableFuture<Void> compactDB() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * This API takes in value and metadata as ByteBuffer format and put it into RocksDB.
   * Only {@link ReplicationMetadataRocksDBStoragePartition} will execute this method,
   * other storage partition implementation will UnsupportedOperationException.
   */
  public void putWithReplicationMetadata(byte[] key, ByteBuffer value, byte[] metadata) {
    throw new VeniceUnsupportedOperationException("putWithReplicationMetadata");
  }

  /**
   * This API takes in value and metadata as byte array format and put it into RocksDB.
   * Only {@link ReplicationMetadataRocksDBStoragePartition} will execute this method,
   * other storage partition implementation will VeniceUnsupportedOperationException.
   */
  public void putWithReplicationMetadata(byte[] key, byte[] value, byte[] metadata) {
    throw new VeniceUnsupportedOperationException("putWithReplicationMetadata");
  }

  /**
   * This API retrieves replication metadata from replicationMetadataColumnFamily.
   * Only {@link ReplicationMetadataRocksDBStoragePartition} will execute this method,
   * other storage partition implementation will VeniceUnsupportedOperationException.
   */
  public byte[] getReplicationMetadata(byte[] key) {
    throw new VeniceUnsupportedOperationException("getReplicationMetadata");
  }

  /**
   * This API deletes a record from RocksDB but updates the metadata in ByteBuffer format and puts it into RocksDB.
   * Only {@link ReplicationMetadataRocksDBStoragePartition} will execute this method,
   * other storage partition implementation will VeniceUnsupportedOperationException.
   */
  public void deleteWithReplicationMetadata(byte[] key, byte[] metadata) {
    throw new VeniceUnsupportedOperationException("deleteWithReplicationMetadata");
  }

  public long getRmdByteUsage() {
    throw new VeniceUnsupportedOperationException("getRmdByteUsage");
  }
}
