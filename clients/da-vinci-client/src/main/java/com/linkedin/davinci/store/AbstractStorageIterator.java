package com.linkedin.davinci.store;

public abstract class AbstractStorageIterator {
  /**
   * @return true if the iterator is valid
   */
  public boolean isValid() {
    throw new UnsupportedOperationException("Method not implemented");
  }

  /**
   * @return the key of the current entry
   */
  public byte[] key() {
    throw new UnsupportedOperationException("Method not implemented");
  }

  /**
   * @return the value of the current entry
   */
  public byte[] value() {
    throw new UnsupportedOperationException("Method not implemented");
  }

  /**
   * Moves to the next entry
   */
  public void next() {
    throw new UnsupportedOperationException("Method not implemented");
  }

  /**
   * Moves to the previous entry
   */
  public void prev() {
    throw new UnsupportedOperationException("Method not implemented");
  }

  /**
   * Seeks to the first entry whose key is greater than or equal to the given key
   *
   * @param key the key to seek to
   */
  public void seek(byte[] key) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  /**
   * Seek to the first key
   */
  public void seekToFirst() {
    throw new UnsupportedOperationException("Method not implemented");
  }

  /**
   * Seek to the last key
   */
  public void seekToLast() {
    throw new UnsupportedOperationException("Method not implemented");
  }
}
