package com.linkedin.davinci.store.cache;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.avro.Schema;


public class VeniceStoreCacheStorageEngine extends AbstractStorageEngine<VeniceStoreCacheStoragePartition> {
  private final ObjectCacheConfig cacheConfig;
  private final VeniceStoreCacheStoragePartition omniPartition;
  /**
   * Since there is only one storage partition per storage engine, we need to create a global lock here since
   * all the requests will be direct to partition 0.
   */
  private final ReadWriteLock globalRWlock = new ReentrantReadWriteLock();

  public VeniceStoreCacheStorageEngine(
      String storeVersionName,
      ObjectCacheConfig config,
      Schema keySchema,
      AsyncCacheLoader asyncCacheLoader) {
    super(
        storeVersionName,
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer());
    cacheConfig = config;
    omniPartition = new VeniceStoreCacheStoragePartition(0, cacheConfig, keySchema, asyncCacheLoader);
    // Add the 0 partitionId automatically in order to satisfy the supers metadata (and we automatically created the
    // omniPartition for this engine)
    this.addStoragePartitionIfAbsent(0);
  }

  @Override
  public PersistenceType getType() {
    return PersistenceType.CACHE;
  }

  @Override
  public Set<Integer> getPersistedPartitionIds() {
    return null;
  }

  @Override
  public VeniceStoreCacheStoragePartition createStoragePartition(StoragePartitionConfig partitionConfig) {
    // TODO: We create a single VeniceStoreCacheStoragePartition and pass that around. This makes look ups more
    // efficient
    // as we no longer need to discover the correct partition to read from, but it means that we can't efficiently
    // evict out data for partitions we are no longer subscribed to (as we can't just drop the specific partition).
    // this is an explicit tradeoff decision based on the idea that lookup speed is more important then immediate clean
    // up
    // on unsubscription. We can revisit clean up, but adding a TTL might be the right way to go.
    return omniPartition;
  }

  @Override
  public synchronized VeniceStoreCacheStoragePartition getPartitionOrThrow(int partitionId) {
    return omniPartition;
  }

  @Override
  public ReadWriteLock getRWLockForPartitionOrThrow(int partitionId) {
    return globalRWlock;
  }

  public <K, V> void putDeserializedValue(K key, V value) {
    omniPartition.put(key, value);
  }

  public VeniceStoreCache getCache() {
    return omniPartition.getVeniceCache();
  }
}
