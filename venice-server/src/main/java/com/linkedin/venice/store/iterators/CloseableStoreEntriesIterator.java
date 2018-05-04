package com.linkedin.venice.store.iterators;

import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.store.bdb.BdbStoragePartition;
import com.linkedin.venice.utils.partition.iterators.AbstractCloseablePartitionEntriesIterator;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;


/**
 * An implementation that iterate over each partition owned by the node for this store and within each
 * partition iterates over the entries one by one.
 *
 */
public class CloseableStoreEntriesIterator implements Iterator<Map.Entry<byte[], byte[]>>, Closeable {

  final Iterator<BdbStoragePartition> partitions;
  AbstractCloseablePartitionEntriesIterator currentIterator = null;
  Map.Entry<byte[], byte[]> currentEntry = null;
  final AbstractStorageEngine storageEngine;

  public CloseableStoreEntriesIterator(Collection<BdbStoragePartition> storagePartitions,
      AbstractStorageEngine storageEngine) {
    partitions = storagePartitions.iterator();
    this.storageEngine = storageEngine;
  }

  @Override
  public void close()
      throws IOException {
  }

  public boolean fetchNextEntry() {
    if (currentIterator != null && currentIterator.hasNext()) {
      currentEntry = currentIterator.next();
      return true;
    } else {


      /* current iterator can be null in these cases:
      *  1. Initially when we start scanning
      *
      *  CurrentIterator.hasNext() can be false:
      *  1. if there are no more entries in the current partition
      *
      */
      if (currentIterator != null) {
        // There are no more items in the current iterator. We need to dispose the current iterator
        // before we go forward.
        try {
          currentIterator.close();
        } catch (IOException e) {
          storageEngine.getLogger().error("Failure to close current Partition entries iterator", e);
          return false;
        }
      }

      boolean hasMore = false;
      while (partitions.hasNext() && !hasMore) {
        currentIterator = partitions.next().partitionEntries();
        if (currentIterator.hasNext()) {
          currentEntry = currentIterator.next();
          hasMore = true;
        }
      }
      /* We reach here in two conditions:
      *  1. We found some entry
      *  2. We dont have any data.
      */
      if (hasMore) {
        // case 1 - we found the next entry
        return true;
      } else {
        // case 2 - there are no partitions and hence no entries
        return false;
      }
    }
  }

  @Override
  public boolean hasNext() {
    return currentEntry != null || fetchNextEntry();
  }

  @Override
  public Map.Entry<byte[], byte[]> next() {
    Map.Entry<byte[], byte[]> result = null;
    if (currentEntry == null) {
      if (!fetchNextEntry()) {
        throw new PersistenceFailureException(
            "Either there was a problem closing the current partition entries iterated (OR) we have reached the end of all partitions");
      }
    }
    result = currentEntry;
    currentEntry = null;
    return result;
  }

  @Override
  public void remove() {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
