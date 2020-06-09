package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.store.StoragePartitionConfig;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;


/**
 * This class is to provide a deep copy implementation of {@link AbstractStorageEngine},
 * so that the delegate will be passed a deep copy of ByteBuffer every time for
 * {@link AbstractStorageEngine#put(Integer, byte[], ByteBuffer)}.
 *
 * If you need to pass a deep copy parameter to other functions, you can modify this class accordingly.
 */
public class DeepCopyStorageEngine extends AbstractStorageEngine<AbstractStoragePartition> {
  private final static InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
  private final static InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer = AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
  private final AbstractStorageEngine delegate;

  public DeepCopyStorageEngine(AbstractStorageEngine delegate) {
    super(delegate.getName(), storeVersionStateSerializer, partitionStateSerializer);
    this.delegate = delegate;
    restoreStoragePartitions();
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
  public void beginBatchWrite(StoragePartitionConfig storagePartitionConfig, Map<String, String> checkpointedInfo,
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
  public void delete(int logicalPartitionId, byte[] key) {
    this.delegate.delete(logicalPartitionId, key);
  }

  @Override
  public byte[] get(int logicalPartitionId, byte[] key) {
    return this.delegate.get(logicalPartitionId, key);
  }

  @Override
  public byte[] get(int logicalPartitionId, ByteBuffer keyBuffer) {
    return this.delegate.get(logicalPartitionId, keyBuffer);
  }

  public void preparePartitionForReading(int partition) {
     delegate.preparePartitionForReading(partition);
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
  public AbstractStoragePartition getPartitionOrThrow(int partitionId) {
    return this.delegate.getPartitionOrThrow(partitionId);
  }

  @Override
  public long getStoreSizeInBytes() {
    return this.delegate.getStoreSizeInBytes();
  }

  @Override
  public void warmUpStoragePartition(int partition) {
    this.delegate.warmUpStoragePartition(partition);
  }
}
