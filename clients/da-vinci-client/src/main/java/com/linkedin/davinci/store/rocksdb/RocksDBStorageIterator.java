package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.store.AbstractStorageIterator;
import org.rocksdb.RocksIterator;


public class RocksDBStorageIterator extends AbstractStorageIterator {
  public final RocksIterator iterator;

  public RocksDBStorageIterator(RocksIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean isValid() {
    return iterator.isValid();
  }

  @Override
  public byte[] key() {
    return iterator.key();
  }

  @Override
  public byte[] value() {
    return iterator.value();
  }

  @Override
  public void next() {
    iterator.next();
  }

  @Override
  public void prev() {
    iterator.prev();
  }

  @Override
  public void seek(byte[] key) {
    iterator.seek(key);
  }

  @Override
  public void seekToFirst() {
    iterator.seekToFirst();
  }

  @Override
  public void seekToLast() {
    iterator.seekToLast();
  }

  @Override
  public void close() {
    iterator.close();
  }
}
