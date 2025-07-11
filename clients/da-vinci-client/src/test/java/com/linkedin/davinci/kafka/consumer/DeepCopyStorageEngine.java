package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.DelegatingStorageEngine;
import com.linkedin.davinci.store.StorageEngine;
import java.nio.ByteBuffer;


/**
 * This class is to provide a deep copy implementation of {@link AbstractStorageEngine},
 * so that the delegate will be passed a deep copy of ByteBuffer every time for
 * {@link AbstractStorageEngine#put(int, byte[], ByteBuffer)}.
 *
 * If you need to pass a deep copy parameter to other functions, you can modify this class accordingly.
 */
public class DeepCopyStorageEngine extends DelegatingStorageEngine<AbstractStoragePartition> {
  public DeepCopyStorageEngine(StorageEngine<AbstractStoragePartition> delegate) {
    super(delegate);
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
    getDelegate().put(logicalPartitionId, key, deepCopyByteBuffer);
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
    getDelegate().putWithReplicationMetadata(logicalPartitionId, key, deepCopyByteBuffer, replicationMetadata);
  }
}
