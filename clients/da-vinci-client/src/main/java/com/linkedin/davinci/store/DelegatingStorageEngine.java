package com.linkedin.davinci.store;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.OffsetRecord;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


public class DelegatingStorageEngine<P extends AbstractStoragePartition> implements StorageEngine<P> {
  private volatile @Nonnull StorageEngine<P> delegate;

  public DelegatingStorageEngine(@Nonnull StorageEngine<P> delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  /**
   * This setter can be used to swap the delegate which all calls get forwarded to. In this way, in cases where a
   * storage engine needs to be closed and later on a new one needs to be re-opened, this fact can be hidden from
   * other classes needing to hold a reference to the storage engine. At the time of writing this JavaDoc, only the
   * {@link com.linkedin.davinci.storage.StorageService} class is tasked with managing storage engine lifecycles in
   * this way.
   */
  public void setDelegate(@Nonnull StorageEngine<P> delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  /**
   * Subclasses should be able to access the delegate, but other classes should not, otherwise they could keep a handle
   * on the delegate, while the delegate may later be swapped via {@link #setDelegate(StorageEngine)}.
   */
  @Nonnull
  protected StorageEngine<P> getDelegate() {
    return this.delegate;
  }

  @Override
  public String getStoreVersionName() {
    return this.delegate.getStoreVersionName();
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
  public void adjustStoragePartition(
      int partitionId,
      StoragePartitionAdjustmentTrigger mode,
      StoragePartitionConfig partitionConfig) {
    this.delegate.adjustStoragePartition(partitionId, mode, partitionConfig);
  }

  @Override
  public void addStoragePartitionIfAbsent(int partitionId) {
    this.delegate.addStoragePartitionIfAbsent(partitionId);
  }

  @Override
  public void closePartition(int partitionId) {
    this.delegate.closePartition(partitionId);
  }

  @Override
  public void closeMetadataPartition() {
    this.delegate.closeMetadataPartition();
  }

  @Override
  public void dropPartition(int partitionId) {
    this.delegate.dropPartition(partitionId);
  }

  @Override
  public void dropPartition(int partitionId, boolean dropMetadataPartitionWhenEmpty) {
    this.delegate.dropPartition(partitionId, dropMetadataPartitionWhenEmpty);
  }

  @Override
  public void drop() {
    this.delegate.drop();
  }

  @Override
  public Map<String, String> sync(int partitionId) {
    return this.delegate.sync(partitionId);
  }

  @Override
  public void close() throws VeniceException {
    this.delegate.close();
  }

  @Override
  public boolean isClosed() {
    return this.delegate.isClosed();
  }

  @Override
  public void endBatchWrite(StoragePartitionConfig storagePartitionConfig) {
    this.delegate.endBatchWrite(storagePartitionConfig);
  }

  @Override
  public void reopenStoragePartition(int partitionId) {
    this.delegate.reopenStoragePartition(partitionId);
  }

  @Override
  public void put(int partitionId, byte[] key, byte[] value) throws VeniceException {
    this.delegate.put(partitionId, key, value);
  }

  @Override
  public void put(int partitionId, byte[] key, ByteBuffer value) throws VeniceException {
    this.delegate.put(partitionId, key, value);
  }

  @Override
  public void putWithReplicationMetadata(int partitionId, byte[] key, ByteBuffer value, byte[] replicationMetadata)
      throws VeniceException {
    this.delegate.putWithReplicationMetadata(partitionId, key, value, replicationMetadata);
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
  public void delete(int partitionId, byte[] key) throws VeniceException {
    this.delegate.delete(partitionId, key);
  }

  @Override
  public void deleteWithReplicationMetadata(int partitionId, byte[] key, byte[] replicationMetadata)
      throws VeniceException {
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

  @Override
  public boolean containsPartition(int partitionId) {
    return this.delegate.containsPartition(partitionId);
  }

  @Override
  public Set<Integer> getPartitionIds() {
    return this.delegate.getPartitionIds();
  }

  @Override
  public P getPartitionOrThrow(int partitionId) {
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
  public StorageEngineStats getStats() {
    return this.delegate.getStats();
  }

  @Override
  public void beginBatchWrite(
      StoragePartitionConfig storagePartitionConfig,
      Map<String, String> checkpointedInfo,
      Optional<Supplier<byte[]>> checksumSupplier) {
    this.delegate.beginBatchWrite(storagePartitionConfig, checkpointedInfo, checksumSupplier);
  }

  @Override
  public boolean checkDatabaseIntegrity(
      int partitionId,
      Map<String, String> checkpointedInfo,
      StoragePartitionConfig storagePartitionConfig) {
    return this.delegate.checkDatabaseIntegrity(partitionId, checkpointedInfo, storagePartitionConfig);
  }
}
