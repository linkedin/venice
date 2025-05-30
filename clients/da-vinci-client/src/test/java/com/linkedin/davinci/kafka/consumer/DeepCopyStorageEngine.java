package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStorageIterator;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;


/**
 * This class is to provide a deep copy implementation of {@link AbstractStorageEngine},
 * so that the delegate will be passed a deep copy of ByteBuffer every time for
 * {@link AbstractStorageEngine#put(int, byte[], ByteBuffer)}.
 *
 * If you need to pass a deep copy parameter to other functions, you can modify this class accordingly.
 */
public class DeepCopyStorageEngine implements StorageEngine<AbstractStoragePartition> {
  private final AbstractStorageEngine delegate;

  public DeepCopyStorageEngine(AbstractStorageEngine delegate) {
    this.delegate = delegate;
  }

  @Override
  public PersistenceType getType() {
    return this.delegate.getType();
  }

  @Override
  public Set<Integer> getPersistedPartitionIds() {
    return this.delegate.getPersistedPartitionIds();
  }

  @Override
  public synchronized void addStoragePartition(int partitionId) {
    this.delegate.addStoragePartition(partitionId);
  }

  @Override
  public synchronized void dropPartition(int partitionId) {
    this.delegate.dropPartition(partitionId);
  }

  @Override
  public void dropPartition(int partitionId, boolean dropMetadataPartitionWhenEmpty) {
    this.delegate.dropPartition(partitionId, dropMetadataPartitionWhenEmpty);
  }

  @Override
  public synchronized void drop() {
    this.delegate.drop();
  }

  @Override
  public synchronized void closePartition(int partitionId) {
    this.delegate.closePartition(partitionId);
  }

  @Override
  public void closeMetadataPartition() {
    this.delegate.closeMetadataPartition();
  }

  @Override
  public String getStoreVersionName() {
    return this.delegate.getStoreVersionName();
  }

  @Override
  public boolean containsPartition(int partitionId) {
    return this.delegate.containsPartition(partitionId);
  }

  @Override
  public synchronized Set<Integer> getPartitionIds() {
    return this.delegate.getPartitionIds();
  }

  public boolean checkDatabaseIntegrity(
      int partitionId,
      Map<String, String> checkpointedInfo,
      StoragePartitionConfig storagePartitionConfig) {
    return this.delegate.checkDatabaseIntegrity(partitionId, checkpointedInfo, storagePartitionConfig);
  }

  @Override
  public void beginBatchWrite(
      StoragePartitionConfig storagePartitionConfig,
      Map<String, String> checkpointedInfo,
      Optional<Supplier<byte[]>> checksumSupplier) {
    this.delegate.beginBatchWrite(storagePartitionConfig, checkpointedInfo, checksumSupplier);
  }

  @Override
  public void endBatchWrite(StoragePartitionConfig storagePartitionConfig) {
    this.delegate.endBatchWrite(storagePartitionConfig);
  }

  @Override
  public void put(int logicalPartitionId, byte[] key, byte[] value) {
    this.delegate.put(logicalPartitionId, key, value);
  }

  @Override
  public void deleteWithReplicationMetadata(int partitionId, byte[] key, byte[] replicationMetadata) {
    this.delegate.deleteWithReplicationMetadata(partitionId, key, replicationMetadata);
  }

  @Override
  public byte[] getReplicationMetadata(int partitionId, ByteBuffer key) {
    return this.delegate.getReplicationMetadata(partitionId, key);
  }

  @Override
  public void putPartitionOffset(int partitionId, OffsetRecord offsetRecord) {
    this.delegate.putPartitionOffset(partitionId, offsetRecord);
  }

  @Override
  public Optional<OffsetRecord> getPartitionOffset(int partitionId) {
    return this.delegate.getPartitionOffset(partitionId);
  }

  @Override
  public void clearPartitionOffset(int partitionId) {
    this.delegate.clearPartitionOffset(partitionId);
  }

  @Override
  public void putStoreVersionState(StoreVersionState versionState) {
    this.delegate.putStoreVersionState(versionState);
  }

  @Override
  public void updateStoreVersionStateCache(StoreVersionState versionState) {
    this.delegate.updateStoreVersionStateCache(versionState);
  }

  @Override
  public StoreVersionState getStoreVersionState() {
    return this.delegate.getStoreVersionState();
  }

  @Override
  public void clearStoreVersionState() {
    this.delegate.clearStoreVersionState();
  }

  /**
   * Deep copy implementation.
   *
   * @param logicalPartitionId
   * @param key
   * @param value
   */
  @Override
  public void put(int logicalPartitionId, byte[] key, ByteBuffer value) {
    ByteBuffer deepCopyByteBuffer = ByteBuffer.allocate(value.remaining());
    // Record the original position for recovery
    deepCopyByteBuffer.mark();
    value.mark();
    deepCopyByteBuffer.put(value);
    // Recover the original position
    value.reset();
    deepCopyByteBuffer.reset();
    this.delegate.put(logicalPartitionId, key, deepCopyByteBuffer);
  }

  @Override
  public void putWithReplicationMetadata(
      int logicalPartitionId,
      byte[] key,
      ByteBuffer value,
      byte[] replicationMetadata) {
    ByteBuffer deepCopyByteBuffer = ByteBuffer.allocate(value.remaining());
    // Record the original position for recovery
    deepCopyByteBuffer.mark();
    value.mark();
    deepCopyByteBuffer.put(value);
    // Recover the original position
    value.reset();
    deepCopyByteBuffer.reset();
    this.delegate.putWithReplicationMetadata(logicalPartitionId, key, deepCopyByteBuffer, replicationMetadata);
  }

  @Override
  public void putReplicationMetadata(int partitionId, byte[] key, byte[] replicationMetadata) throws VeniceException {
    this.delegate.putReplicationMetadata(partitionId, key, replicationMetadata);
  }

  @Override
  public byte[] get(int partitionId, byte[] key) throws VeniceException {
    return this.delegate.get(partitionId, key);
  }

  @Override
  public ByteBuffer get(int partitionId, byte[] key, ByteBuffer valueToBePopulated) throws VeniceException {
    return this.delegate.get(partitionId, key, valueToBePopulated);
  }

  @Override
  public byte[] get(int partitionId, ByteBuffer keyBuffer) throws VeniceException {
    return this.delegate.get(partitionId, keyBuffer);
  }

  @Override
  public void getByKeyPrefix(int partitionId, byte[] partialKey, BytesStreamingCallback bytesStreamingCallback) {
    this.delegate.getByKeyPrefix(partitionId, partialKey, bytesStreamingCallback);
  }

  @Override
  public void delete(int logicalPartitionId, byte[] key) {
    this.delegate.delete(logicalPartitionId, key);
  }

  @Override
  public synchronized void adjustStoragePartition(
      int partitionId,
      StoragePartitionAdjustmentTrigger mode,
      StoragePartitionConfig partitionConfig) {
    delegate.adjustStoragePartition(partitionId, mode, partitionConfig);
  }

  @Override
  public Map<String, String> sync(int partitionId) {
    return this.delegate.sync(partitionId);
  }

  @Override
  public void close() {
    this.delegate.close();
  }

  @Override
  public boolean isClosed() {
    return this.delegate.isClosed();
  }

  @Override
  public AbstractStoragePartition getPartitionOrThrow(int partitionId) {
    return this.delegate.getPartitionOrThrow(partitionId);
  }

  @Override
  public AbstractStorageIterator getIterator(int partitionId) {
    return this.delegate.getIterator(partitionId);
  }

  @Override
  public void suppressLogs(boolean b) {
    this.delegate.suppressLogs(b);
  }

  @Override
  public void reopenStoragePartition(int partitionId) {
    this.delegate.reopenStoragePartition(partitionId);
  }
}
