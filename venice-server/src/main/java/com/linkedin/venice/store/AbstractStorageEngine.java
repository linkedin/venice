package com.linkedin.venice.store;

import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.utils.ClosableIterator;
import com.linkedin.venice.utils.Pair;


/**
 * TODO: This is just a stub code for now. I plan to populate the methods later when implementing the storage engines.
 */
public class AbstractStorageEngine implements StorageEngine {

  private final String storeName;

  public AbstractStorageEngine(String storeName) {
    this.storeName = storeName;
  }

  /**
   * Get store name served by this storage engine
   * @return associated storeName
   */
  public String getStoreName() {
    return this.storeName;
  }

  @Override
  public ClosableIterator<Pair<byte[], byte[]>> entries() {
    return null;
  }

  @Override
  public ClosableIterator<byte[]> keys() {
    return null;
  }

  @Override
  public void truncate() {

  }

  @Override
  public boolean beginBatchModifications() {
    return false;
  }

  @Override
  public boolean endBatchModifications() {
    return false;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public byte[] get(byte[] key)
      throws VeniceStorageException {
    return new byte[0];
  }

  @Override
  public void put(byte[] key, byte[] value)
      throws VeniceStorageException {

  }

  @Override
  public void delete(byte[] key)
      throws VeniceStorageException {

  }

  @Override
  public void close()
      throws VeniceStorageException {

  }
}
