package com.linkedin.davinci.store.blackhole;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StoragePartitionConfig;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;


public class BlackHoleStorageEnginePartition extends AbstractStoragePartition {
  public BlackHoleStorageEnginePartition(Integer partitionId) {
    super(partitionId);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    // ktnx
  }

  @Override
  public void put(byte[] key, ByteBuffer value) {
    // RAWR even faster than fastest
  }

  @Override
  public <K, V> void put(K key, V value) {
    // sah dude
  }

  @Override
  public byte[] get(byte[] key) {
    // I think this is what you're looking for...
    return null;
  }

  @Override
  public <K, V> V get(K key) {
    // this is fun
    return null;
  }

  @Override
  public byte[] get(ByteBuffer key) {
    return null;
  }

  @Override
  public void getByKeyPrefix(byte[] keyPrefix, BytesStreamingCallback callback) {
    // This should do it
    callback.onCompletion();
  }

  @Override
  public void delete(byte[] key) {
    // consider it done!
  }

  @Override
  public Map<String, String> sync() {
    return Collections.EMPTY_MAP;
  }

  @Override
  public void drop() {
    // Right away!
  }

  @Override
  public void close() {
    // kbye
  }

  @Override
  public boolean verifyConfig(StoragePartitionConfig storagePartitionConfig) {
    // All good!
    return true;
  }

  @Override
  public long getPartitionSizeInBytes() {
    throw new UnsupportedOperationException("Operation Not Supported");
  }
}
