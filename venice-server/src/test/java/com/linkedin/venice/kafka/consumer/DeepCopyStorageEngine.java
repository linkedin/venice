package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.store.StoragePartitionConfig;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


/**
 * This class is to provide a deep copy implementation of {@link AbstractStorageEngine},
 * so that the delegate will be passed a deep copy of ByteBuffer every time for
 * {@link AbstractStorageEngine#put(Integer, byte[], ByteBuffer)}.
 *
 * If you need to pass a deep copy parameter to other functions, you can modify this class accordingly.
 */
public class DeepCopyStorageEngine extends AbstractStorageEngine {
  private final AbstractStorageEngine delegate;

  public DeepCopyStorageEngine(AbstractStorageEngine delegate) {
    super(delegate.getName());
    this.delegate = delegate;
  }

  @Override
  public PersistenceType getType() {
    return this.delegate.getType();
  }

  @Override
  protected Set<Integer> getPersistedPartitionIds() {
    // We could not delegate protected function here.
    return Collections.emptySet();
  }

  @Override
  public AbstractStoragePartition createStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    return this.delegate.createStoragePartition(storagePartitionConfig);
  }

  @Override
  public synchronized void addStoragePartition(int partitionId) {
    this.delegate.addStoragePartition(partitionId);
  }

  @Override
  public synchronized void addStoragePartition(StoragePartitionConfig storagePartitionConfig) {
    this.delegate.addStoragePartition(storagePartitionConfig);
  }

  @Override
  public synchronized void dropPartition(int partitionId) {
    this.delegate.dropPartition(partitionId);
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
  public String getName() {
    return this.delegate.getName();
  }

  @Override
  public boolean containsPartition(int partitionId) {
    return this.delegate.containsPartition(partitionId);
  }

  @Override
  public synchronized Set<Integer> getPartitionIds() {
    return this.delegate.getPartitionIds();
  }

  @Override
  public void beginBatchWrite(StoragePartitionConfig storagePartitionConfig, Map<String, String> checkpointedInfo){
    this.delegate.beginBatchWrite(storagePartitionConfig, checkpointedInfo);
  }

  @Override
  public void endBatchWrite(StoragePartitionConfig storagePartitionConfig) {
    this.delegate.endBatchWrite(storagePartitionConfig);
  }

  @Override
  public void put(Integer logicalPartitionId, byte[] key, byte[] value) {
    this.delegate.put(logicalPartitionId, key, value);
  }

  /**
   * Deep copy implementation.
   *
   * @param logicalPartitionId
   * @param key
   * @param value
   */
  @Override
  public void put(Integer logicalPartitionId, byte[] key, ByteBuffer value) {
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
  public void delete(Integer logicalPartitionId, byte[] key) {
    this.delegate.delete(logicalPartitionId, key);
  }

  @Override
  public byte[] get(Integer logicalPartitionId, byte[] key) {
    return this.delegate.get(logicalPartitionId, key);
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
  public AbstractStoragePartition getStoragePartition(int partitionId) {
    return this.delegate.getStoragePartition(partitionId);
  }
}
