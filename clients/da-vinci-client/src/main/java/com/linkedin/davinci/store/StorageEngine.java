package com.linkedin.davinci.store;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;


public interface StorageEngine<Partition extends AbstractStoragePartition> extends Closeable {
  String getStoreVersionName();

  PersistenceType getType();

  Set<Integer> getPersistedPartitionIds();

  /**
   * Adjust the opened storage partition according to the provided storagePartitionConfig.
   * It will throw exception if there is no opened storage partition for the given partition id.
   *
   * The reason to have {@param partitionId} is mainly used to ease the unit test.
   */
  void adjustStoragePartition(
      int partitionId,
      StoragePartitionAdjustmentTrigger mode,
      StoragePartitionConfig partitionConfig);

  void addStoragePartitionIfAbsent(int partitionId);

  void closePartition(int partitionId);

  void closeMetadataPartition();

  /**
   * Removes and returns a partition from the current store
   *
   * @param partitionId - id of partition to retrieve and remove
   */
  void dropPartition(int partitionId);

  /**
   * Removes and returns a partition from the current store
   *
   * @param partitionId - id of partition to retrieve and remove
   * @param dropMetadataPartitionWhenEmpty - if true, the whole store will be dropped if ALL partitions are removed
   */
  void dropPartition(int partitionId, boolean dropMetadataPartitionWhenEmpty);

  /**
   * Drop the whole store
   */
  void drop();

  Map<String, String> sync(int partitionId);

  @Override
  void close() throws VeniceException;

  boolean isClosed();

  /**
   * checks whether the current state of the database is valid
   * during the start of ingestion.
   */
  boolean checkDatabaseIntegrity(
      int partitionId,
      Map<String, String> checkpointedInfo,
      StoragePartitionConfig storagePartitionConfig);

  /**
   * A lot of storage engines support efficient methods for performing large
   * number of writes (puts/deletes) against the data source. This method puts
   * the storage engine in this batch write mode
   */
  void beginBatchWrite(
      StoragePartitionConfig storagePartitionConfig,
      Map<String, String> checkpointedInfo,
      Optional<Supplier<byte[]>> checksumSupplier);

  /**
   * @return true if the storage engine successfully returned to normal mode
   */
  void endBatchWrite(StoragePartitionConfig storagePartitionConfig);

  /**
   * Reopen the underlying database.
   */
  void reopenStoragePartition(int partitionId);

  void put(int partitionId, byte[] key, byte[] value) throws VeniceException;

  void put(int partitionId, byte[] key, ByteBuffer value) throws VeniceException;

  void putWithReplicationMetadata(int partitionId, byte[] key, ByteBuffer value, byte[] replicationMetadata)
      throws VeniceException;

  void putReplicationMetadata(int partitionId, byte[] key, byte[] replicationMetadata) throws VeniceException;

  byte[] get(int partitionId, byte[] key) throws VeniceException;

  ByteBuffer get(int partitionId, byte[] key, ByteBuffer valueToBePopulated) throws VeniceException;

  byte[] get(int partitionId, ByteBuffer keyBuffer) throws VeniceException;

  void getByKeyPrefix(int partitionId, byte[] partialKey, BytesStreamingCallback bytesStreamingCallback);

  void delete(int partitionId, byte[] key) throws VeniceException;

  void deleteWithReplicationMetadata(int partitionId, byte[] key, byte[] replicationMetadata) throws VeniceException;

  byte[] getReplicationMetadata(int partitionId, ByteBuffer key);

  /**
   * Put the offset associated with the partitionId into the metadata partition.
   */
  void putPartitionOffset(int partitionId, OffsetRecord offsetRecord);

  /**
   * Retrieve the offset associated with the partitionId from the metadata partition.
   */
  Optional<OffsetRecord> getPartitionOffset(int partitionId);

  /**
   * Clear the offset associated with the partitionId in the metadata partition.
   */
  void clearPartitionOffset(int partitionId);

  /**
   * Put the store version state into the metadata partition.
   */
  void putStoreVersionState(StoreVersionState versionState);

  /**
   * Used in ingestion isolation mode update the storage engine's cache in sync with the updates to the state in
   * {@link com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService}
   */
  void updateStoreVersionStateCache(StoreVersionState versionState);

  /**
   * Retrieve the store version state from the metadata partition.
   */
  StoreVersionState getStoreVersionState();

  /**
   * Clear the store version state in the metadata partition.
   */
  void clearStoreVersionState();

  /**
   * Return true or false based on whether a given partition exists within this storage engine
   *
   * @param partitionId The partition to look for
   * @return True/False, does the partition exist on this node
   */
  boolean containsPartition(int partitionId);

  /**
   * Get all Partition Ids which are assigned to the current Node.
   *
   * @return partition Ids that are hosted in the current Storage Engine.
   */
  Set<Integer> getPartitionIds();

  Partition getPartitionOrThrow(int partitionId);

  AbstractStorageIterator getIterator(int partitionId);

  void suppressLogs(boolean b);

  default StorageEngineStats getStats() {
    return StorageEngineNoOpStats.SINGLETON;
  }
}
