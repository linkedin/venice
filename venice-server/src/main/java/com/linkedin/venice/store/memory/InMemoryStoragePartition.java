package com.linkedin.venice.store.memory;

import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.utils.partition.iterators.AbstractCloseablePartitionEntriesIterator;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.partition.iterators.CloseablePartitionKeysIterator;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;


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
  private static final Logger logger = Logger.getLogger(InMemoryStoragePartition.class.getName());
  private final ConcurrentMap<byte[], byte[]> partitionDb;

  public InMemoryStoragePartition(Integer partitionId) {
    super(partitionId);
    partitionDb = new ConcurrentHashMap<byte[], byte[]>();
  }

  public void put(byte[] key, byte[] value) {
    partitionDb.put(key, value);
  }

  public byte[] get(byte[] key) {
    if (partitionDb.containsKey(key)) {
      return partitionDb.get(key);
    }
    logger.error("key:" + ByteUtils.toHexString(key) + " does not exist!");
    return null;
  }

  public void delete(byte[] key) {
    partitionDb.remove(key);
  }

  @Override
  public AbstractCloseablePartitionEntriesIterator partitionEntries() {
    return new InMemoryPartitionIterator(this.partitionDb);
  }

  @Override
  public CloseablePartitionKeysIterator partitionKeys() {
    return new CloseablePartitionKeysIterator(partitionEntries());
  }

  @Override
  public synchronized void truncate() {
    partitionDb.clear();
  }

  private class InMemoryPartitionIterator extends AbstractCloseablePartitionEntriesIterator {
    final Iterator<Map.Entry<byte[], byte[]>> partitionDbIterator;

    InMemoryPartitionIterator(ConcurrentMap<byte[], byte[]> partitionDb) {
      this.partitionDbIterator = partitionDb.entrySet().iterator();
    }

    @Override
    public void close()
        throws IOException {
      // Nothin to do here, since it is in memory implementation
    }

    @Override
    public boolean fetchNextEntry() {
      if (partitionDbIterator.hasNext()) {
        this.currentEntry = partitionDbIterator.next();
        return true;
      } else {
        return false;
      }
    }
  }
}
