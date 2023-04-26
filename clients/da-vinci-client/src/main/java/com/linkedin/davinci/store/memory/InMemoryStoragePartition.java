package com.linkedin.davinci.store.memory;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.utils.ByteArray;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * An in-memory hashmap implementation of a storage partition
 *
 *
 *Assumptions:
 * 1.No need to worry about synchronizing write/deletes as the model is based on a single writer.
 * So all updates are already serialized.
 * 2. Concurrent reads may be stale if writes/deletes are going on. But the consistency model is also designed to be eventual.
 * Since "read your own writes semantics" is not guaranteed this eventual consistency is tolerable.
 *
 * Even though there will be one writer and 1 or more readers, we may still need a concurrentHashMap to avoid
 * ConcurrentModicfictionException thrown from the iterators
 */
public class InMemoryStoragePartition extends AbstractStoragePartition {
  private final ConcurrentMap<ByteArray, ByteArray> partitionDb;
  private long partitionSize = 0;

  public InMemoryStoragePartition(Integer partitionId) {
    super(partitionId);
    partitionDb = new ConcurrentHashMap<>();
  }

  public void put(byte[] key, byte[] value) {
    ByteArray k = new ByteArray(key);
    ByteArray v = new ByteArray(value);
    partitionSize += key.length;
    partitionSize += value.length;
    partitionDb.put(k, v);
  }

  @Override
  public void put(byte[] key, ByteBuffer valueBuffer) {
    byte[] value = new byte[valueBuffer.remaining()];
    System.arraycopy(valueBuffer.array(), valueBuffer.position(), value, 0, valueBuffer.remaining());
    put(key, value);
  }

  @Override
  public <K, V> void put(K key, V value) {
    throw new UnsupportedOperationException("Method not implemented!!");
  }

  public byte[] get(byte[] key) throws PersistenceFailureException {
    ByteArray k = new ByteArray(key);
    if (partitionDb.containsKey(k)) {
      return partitionDb.get(k).get();
    }
    return null;
  }

  @Override
  public <K, V> V get(K key) {
    throw new UnsupportedOperationException("Method not implemented!!");
  }

  @Override
  public byte[] get(ByteBuffer key) {
    ByteArray keyArray = new ByteArray(key.array());
    ByteArray byteArray = partitionDb.get(keyArray);
    if (byteArray != null) {
      return byteArray.get();
    }
    return null;
  }

  @Override
  public void getByKeyPrefix(byte[] keyPrefix, BytesStreamingCallback callback) {
    for (Map.Entry<ByteArray, ByteArray> entry: partitionDb.entrySet()) {
      if (keyPrefix == null || entry.getKey().startsWith(keyPrefix)) {
        callback.onRecordReceived(entry.getKey().get(), entry.getValue().get());
      }
    }
    callback.onCompletion();
  }

  public void delete(byte[] key) {
    ByteArray k = new ByteArray(key);
    ByteArray v = partitionDb.remove(k);
    partitionSize -= k.length();
    if (v != null) {
      partitionSize -= v.length();
    }
  }

  @Override
  public Map<String, String> sync() {
    // no-op
    return Collections.emptyMap();
  }

  @Override
  public void drop() {
    partitionDb.clear();
  }

  @Override
  public void close() {
    // Nothing to do here, since it is in memory implementation
  }

  @Override
  public boolean verifyConfig(StoragePartitionConfig storagePartitionConfig) {
    // no need to do any special check
    return true;
  }

  @Override
  public long getPartitionSizeInBytes() {
    return partitionSize;
  }
}
