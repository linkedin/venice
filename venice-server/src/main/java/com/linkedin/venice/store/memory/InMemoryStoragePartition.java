package com.linkedin.venice.store.memory;

import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.store.StoragePartitionConfig;
import com.linkedin.venice.utils.ByteArray;
import com.linkedin.venice.utils.partition.iterators.AbstractCloseablePartitionEntriesIterator;
import com.linkedin.venice.utils.partition.iterators.CloseablePartitionKeysIterator;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
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

  public InMemoryStoragePartition(Integer partitionId) {
    super(partitionId);
    partitionDb = new ConcurrentHashMap<ByteArray, ByteArray>();
  }

  public void put(byte[] key, byte[] value) {
    ByteArray k = new ByteArray(key);
    ByteArray v = new ByteArray(value);
    partitionDb.put(k, v);
  }

  public byte[] get(byte[] key)
      throws PersistenceFailureException {
    ByteArray k = new ByteArray(key);
    if (partitionDb.containsKey(k)) {
      return partitionDb.get(k).get();
    }
    return null;
  }

  public void delete(byte[] key) {
    ByteArray k = new ByteArray(key);
    partitionDb.remove(k);
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
}
