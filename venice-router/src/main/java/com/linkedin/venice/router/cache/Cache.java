package com.linkedin.venice.router.cache;

import java.util.Optional;


public interface Cache<K, V> {
  Optional<V> put(K key, V value);

  default Optional<V> putNullValue(K key) {
    return put(key, null);
  }

  Optional<V> get(K key);

  Optional<V> remove(K key);

  void clear();
}
