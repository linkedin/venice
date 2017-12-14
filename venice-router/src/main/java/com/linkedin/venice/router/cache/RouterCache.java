package com.linkedin.venice.router.cache;

import com.linkedin.venice.common.Measurable;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;

import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


public class RouterCache implements RoutingDataRepository.RoutingDataChangedListener {
  private static Logger logger = Logger.getLogger(RouterCache.class);

  private final RoutingDataRepository routingDataRepository;

  private static class CacheKey implements Measurable {
    private final int storeId;
    private final int version;
    private final byte[] key;

    public CacheKey(int storeId, int version, byte[] key) {
      this.storeId = storeId;
      this.version = version;
      this.key = key;
    }

    @Override
    public int getSize() {
      return 4 + 4 + key.length;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CacheKey cacheKey = (CacheKey) o;

      if (storeId != cacheKey.storeId) {
        return false;
      }
      if (version != cacheKey.version) {
        return false;
      }
      return Arrays.equals(key, cacheKey.key);
    }

    @Override
    public int hashCode() {
      int result = storeId;
      result = 31 * result + version;
      result = 31 * result + Arrays.hashCode(key);
      return result;
    }
  }

  public static class CacheValue implements Measurable {
    private final byte[] value;
    private final int schemaId;

    public CacheValue(byte[] value, int schemaId) {
      this.value = value;
      this.schemaId = schemaId;
    }

    public byte[] getValue() {
      return value;
    }

    public int getSchemaId() {
      return schemaId;
    }

    @Override
    public int getSize() {
      return 4 + value.length;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CacheValue that = (CacheValue) o;

      if (schemaId != that.schemaId) {
        return false;
      }
      return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(value);
      result = 31 * result + schemaId;
      return result;
    }
  }

  private final LRUCache<CacheKey, CacheValue> cache;
  private final Map<String, Integer> storeNameIdMapping = new ConcurrentHashMap();
  private int storeId = 0;

  //store the int here since it's for constructing Http header
  private final Map<String, CompressionStrategy> compressionTypeCache;

  public RouterCache(long capacityInBytes, int concurrency, RoutingDataRepository routingDataRepository) {
    this.cache = new LRUCache<>(capacityInBytes, concurrency);
    this.compressionTypeCache = new ConcurrentHashMap<>();
    this.routingDataRepository = routingDataRepository;
  }

  private int getStoreId(String storeName) {
    return storeNameIdMapping.computeIfAbsent(storeName, (k) -> storeId++ );
  }

  public void put(String storeName, int version, byte[] key, CacheValue value) {
    CacheKey cacheKey = new CacheKey(getStoreId(storeName), version, key);
    cache.put(cacheKey, value);
  }

  public void putNullValue(String storeName, int version, byte[] key) {
    CacheKey cacheKey = new CacheKey(getStoreId(storeName), version, key);
    cache.putNullValue(cacheKey);
  }

  public Optional<CacheValue> get(String storeName, int version, byte[] key) {
    CacheKey cacheKey = new CacheKey(getStoreId(storeName), version, key);
    return cache.get(cacheKey);
  }

  public void setCompressionType(String topicName, CompressionStrategy compressionStrategy) {
    CompressionStrategy previouslyCachedStrategy = compressionTypeCache.put(topicName, compressionStrategy);
    if (previouslyCachedStrategy == null) {
      routingDataRepository.subscribeRoutingDataChange(topicName, this);
    } else if (previouslyCachedStrategy != compressionStrategy) {
      throw new VeniceException(String.format("Compression strategies are inconsistent among values. " +
          "Topic: %s, expected strategy: %s, actual strategy: %s", topicName, previouslyCachedStrategy.toString(),
          compressionStrategy.toString()));
    }
  }

  public CompressionStrategy getCompressionStrategy(String topicName) {
    CompressionStrategy compressionStrategy = compressionTypeCache.get(topicName);
    if (compressionStrategy == null) {
      logger.warn("Compression type shouldn't be null. Something unexpected happened.");
      compressionStrategy = CompressionStrategy.NO_OP;
    }

    return compressionStrategy;
  }

  public void clear() {
    cache.clear();
  }

  @Override
  public void onRoutingDataChanged(PartitionAssignment partitionAssignment) {

  }

  @Override
  public void onRoutingDataDeleted(String kafkaTopic) {
    compressionTypeCache.remove(kafkaTopic);
    routingDataRepository.unSubscribeRoutingDataChange(kafkaTopic, this);
  }
}
