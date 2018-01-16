package com.linkedin.venice.router.cache;

import java.util.Optional;


public interface Cache<K, V> extends AutoCloseable {
  boolean put(K key, Optional<V> value);

  Optional<V> get(K key);

  boolean remove(K key);

  void clear();

  long getEntryNum();

  long getCacheSize();

  long getEntryNumMaxDiffBetweenBuckets();

  long getCacheSizeMaxDiffBetweenBuckets();
}
