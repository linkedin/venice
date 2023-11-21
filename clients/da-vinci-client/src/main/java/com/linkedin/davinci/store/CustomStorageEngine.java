package com.linkedin.davinci.store;

import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.Set;


public interface CustomStorageEngine<K, V> {
  String storeName = null;

  String getStoreName();

  void setStoreName();

  void addStoragePartition(int partitionId);

  boolean containsPartition(int partitionId);

  Set<Integer> getPartitionIds();

  void put(int partitionId, byte[] key, ByteBuffer value);

  void delete(Lazy<K> key, int partition);

  void close();
}
