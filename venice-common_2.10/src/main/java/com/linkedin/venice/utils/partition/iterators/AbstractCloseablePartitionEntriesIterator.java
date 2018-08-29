package com.linkedin.venice.utils.partition.iterators;

import com.linkedin.venice.exceptions.PersistenceFailureException;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


/**
 * An abstract implementation to iterate through the entries in the database
 *
 * The constructor and fetchNextEntry() needs to be implemented by the specific storage engine/ storage partition implementations
 */
public abstract class AbstractCloseablePartitionEntriesIterator implements Iterator<Map.Entry<byte[], byte[]>>, Closeable {

  protected Map.Entry<byte[], byte[]> currentEntry = null;

  public abstract void close()
      throws IOException;

  @Override
  public boolean hasNext() {
    return currentEntry != null || fetchNextEntry();
  }

  protected abstract boolean fetchNextEntry();

  @Override
  public Map.Entry<byte[], byte[]> next() {
    Map.Entry<byte[], byte[]> result = null;
    if (currentEntry == null) {
      if (!fetchNextEntry()) {
        throw new PersistenceFailureException("Iterated till end.");
      }
    }
    result = currentEntry;
    currentEntry = null;
    return result;
  }
}
