package com.linkedin.davinci.store;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.log4j.Logger;


/**
 * An abstract implementation of a storage partition. This could be a database in BDB
 * environment or a concurrent hashmap incase of an inMemory implementation depending
 * on the storage-partition model.
 */
public abstract class AbstractStoragePartition {
  protected final Logger LOGGER = Logger.getLogger(getClass());

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
   * @return null if the key does not exist, byte[] value if it exists.
   */
  public abstract byte[] get(byte[] key);

  public ByteBuffer get(byte[] key, ByteBuffer valueToBePopulated) {
    // Naive default impl is not optimized... only storage engines that support the optimization implement it.
    return ByteBuffer.wrap(get(key));
  }

  /**
   * Get a Value from the partition database
   * @param key key to be retrieved
   * @param <K> the type for Key
   * @param <V> the type for the return value
   * @return null if the key does not exist, V value if it exists
   */
  public abstract <K, V> V get(K key);

  public abstract byte[] get(ByteBuffer key);

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
    LOGGER.info("Warming up is not implemented by default");
  }

  /**
   * One-time database compaction.
   * @return CompletableFuture to track the compaction progress.
   */
  public CompletableFuture<Void> compactDB() {
    return CompletableFuture.completedFuture(null);
  }
}
