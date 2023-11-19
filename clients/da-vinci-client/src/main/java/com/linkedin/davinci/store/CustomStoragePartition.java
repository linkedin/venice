package com.linkedin.davinci.store;

import com.linkedin.venice.utils.lazy.Lazy;


public interface CustomStoragePartition<K, V> {
  Integer partitionId = null;

  Integer getPartitionId();

  void put(Lazy<K> key, Lazy<V> value, int partition);

  void delete(Lazy<K> key, int partition);

  Lazy<V> get(Lazy<K> key, int partition);

  void close();
}
