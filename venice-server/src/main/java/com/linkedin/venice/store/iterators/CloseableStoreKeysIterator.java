package com.linkedin.venice.store.iterators;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


/**
 * An implementation to iterate through the keys in a store
 *
 * Gets an store entry iterator and filters out the value part from each entry.
 */
public class CloseableStoreKeysIterator implements Iterator<byte[]>, Closeable {
  byte[] currentEntry = null;
  private final CloseableStoreEntriesIterator entriesIterator;

  public CloseableStoreKeysIterator(CloseableStoreEntriesIterator entriesIterator) {
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
