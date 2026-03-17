package com.linkedin.davinci.store;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;


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

  void addStoragePartition(StoragePartitionConfig storagePartitionConfig);

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
  Optional<OffsetRecord> getPartitionOffset(int partitionId, PubSubContext pubSubContext);

  /**
   * Clear the offset associated with the partitionId in the metadata partition.
   */
  void clearPartitionOffset(int partitionId);

  /**
   * Put the store version state into the metadata partition.
   */
  void putStoreVersionState(StoreVersionState versionState);

  /**
   * Update the storage engine's store version state cache.
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
   * Put serialized Global RT DIV state into the metadata partition.
   */
  void putGlobalRtDivState(int partitionId, String brokerUrl, byte[] valueBytes);

  /**
   * Retrieve serialized Global RT DIV state from the metadata partition.
   */
  Optional<byte[]> getGlobalRtDivState(int partitionId, String brokerUrl);

  /**
   * Clear serialized Global RT DIV state from the metadata partition.
   */
  void clearGlobalRtDivState(int partitionId, String brokerUrl);

  /**
   * Put a GlobalRtDiv intermediate chunk (with schema header prepended) into the metadata partition.
   */
  void putGlobalRtDivChunk(int partitionId, byte[] chunkKey, byte[] chunkValue);

  /**
   * Retrieve a GlobalRtDiv intermediate chunk from the metadata partition.
   *
   * @return the chunk bytes, or {@code null} if the chunk is not present
   */
  @Nullable
  byte[] getGlobalRtDivChunk(int partitionId, byte[] chunkKey);

  /**
   * Delete a GlobalRtDiv intermediate chunk from the metadata partition.
   */
  void deleteGlobalRtDivChunk(int partitionId, byte[] chunkKey);

  /**
   * Put a GlobalRtDiv chunked-value manifest (with schema header prepended) into the metadata partition.
   * The manifest key must include the non-chunked key suffix appended by
   * {@link com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer}.
   */
  void putGlobalRtDivManifest(int partitionId, byte[] manifestKey, byte[] manifestBytesWithHeader);

  /**
   * Retrieve a GlobalRtDiv chunked-value manifest from the metadata partition.
   *
   * @return the manifest bytes (schema header prepended), or {@code null} if not present
   */
  @Nullable
  byte[] getGlobalRtDivManifest(int partitionId, byte[] manifestKey);

  /**
   * Delete a GlobalRtDiv chunked-value manifest from the metadata partition.
   */
  void deleteGlobalRtDivManifest(int partitionId, byte[] manifestKey);

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
