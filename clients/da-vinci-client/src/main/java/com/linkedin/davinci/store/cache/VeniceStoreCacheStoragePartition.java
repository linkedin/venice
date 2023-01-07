package com.linkedin.davinci.store.cache;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import com.linkedin.davinci.store.cache.caffeine.CaffeineVeniceStoreCache;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.Schema;


public class VeniceStoreCacheStoragePartition extends AbstractStoragePartition {
  private final VeniceStoreCache veniceCache;
  private RecordDeserializer keyDeserializer;

  // TODO: AsyncCacheLoader is a caffeine interface, should come up with a generic one at some point
  public VeniceStoreCacheStoragePartition(
      Integer partitionId,
      ObjectCacheConfig cacheConfig,
      Schema keySchema,
      AsyncCacheLoader cacheLoader) {
    this(
        partitionId,
        cacheConfig,
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema),
        cacheLoader);
  }

  // To be used if a specific deserializer should be passed. The deserializer of the keys should match up in order to
  // invalidate records.
  // most cases used the generic deserializer for key deserialization.
  public VeniceStoreCacheStoragePartition(
      Integer partitionId,
      ObjectCacheConfig cacheConfig,
      RecordDeserializer keyRecordDeserializer,
      AsyncCacheLoader cacheLoader) {
    super(partitionId);
    // TODO: At some point we may want other cache implementations aside from caffeine. The config should inform this
    // assignment.
    // TODO: We should also consult the cacheConfig to determine if we should be caching nulls with ttl
    veniceCache = new CaffeineVeniceStoreCache(cacheConfig, cacheLoader);

    // We could use a specific record deserializer here, but wiring in the specific key class value is a bit confusing
    // in the interface. Since
    // the scenarios where we need to deserialize the key aren't in the hot path, we ues a generic deserializer to keep
    // it simple.
    keyDeserializer = keyRecordDeserializer;
  }

  @Override
  public void put(byte[] key, byte[] value) {
    put(key, ByteBuffer.wrap(value));
  }

  @Override
  public void put(byte[] key, ByteBuffer value) {
    // This is the method called by the store ingestion task. If the update policy is to purge, purge.
    // If in the future we want to store the data, store it. Anything that wants to guarantee
    // that something is stored irregardless should call the putDeserializedValue method.

    // Everything in the cache itself is stored deserialized in order to keep look ups fast. So
    // in order to find the key that we want to invalidate, we need to deserialize it on the ingestion
    // path.
    veniceCache.invalidate(keyDeserializer.deserialize(key));
  }

  @Override
  public <K, V> void put(K key, V value) {
    veniceCache.insert(key, value);
  }

  @Override
  public byte[] get(byte[] key) {
    return this.get(ByteBuffer.wrap(key));
  }

  @Override
  public <K, V> V get(K key) {
    throw new UnsupportedOperationException("Reading via a key synchronously is unsupported by this implementation!!");
  }

  @Override
  public byte[] get(ByteBuffer key) {
    throw new UnsupportedOperationException(
        "Reading via a serialized key for a serialized record unsupported by this implementation!!");
  }

  @Override
  public void getByKeyPrefix(byte[] keyPrefix, BytesStreamingCallback callback) {
    throw new UnsupportedOperationException(
        "Reading via a serialized key for a serialized record unsupported by this implementation!!");
  }

  @Override
  public void delete(byte[] key) {
    veniceCache.invalidate(keyDeserializer.deserialize(key));
  }

  @Override
  public Map<String, String> sync() {
    // no-op
    return Collections.emptyMap();
  }

  @Override
  public void drop() {
    veniceCache.clear();
  }

  @Override
  public void close() {
    veniceCache.close();
  }

  @Override
  public boolean verifyConfig(StoragePartitionConfig storagePartitionConfig) {
    return true;
  }

  @Override
  public long getPartitionSizeInBytes() {
    return veniceCache.size();
  }

  public VeniceStoreCache getVeniceCache() {
    return veniceCache;
  }
}
