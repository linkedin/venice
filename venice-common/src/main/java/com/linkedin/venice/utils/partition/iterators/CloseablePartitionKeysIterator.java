package com.linkedin.venice.utils.partition.iterators;

import com.linkedin.venice.utils.partition.iterators.AbstractCloseablePartitionEntriesIterator;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


/**
 * An implementation to iterate through the keys in a partition
 *
 * Gets an entry iterator and filters out the value part.
 */
public class CloseablePartitionKeysIterator implements Iterator<byte[]>, Closeable {
  byte[] currentEntry;
  private final AbstractCloseablePartitionEntriesIterator entriesIterator;

  public CloseablePartitionKeysIterator(AbstractCloseablePartitionEntriesIterator entriesIterator) {
    this.entriesIterator = entriesIterator;
  }

  @Override
  public boolean hasNext() {
    return entriesIterator.hasNext();
  }

  @Override
  public void close()
      throws IOException {
    entriesIterator.close();
  }

  @Override
  public byte[] next() {
    Map.Entry<byte[], byte[]> result = entriesIterator.next();
    if (result == null) {
      return null;
    }
    return result.getKey();
  }
}
