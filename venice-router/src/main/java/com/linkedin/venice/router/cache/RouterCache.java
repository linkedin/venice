package com.linkedin.venice.router.cache;

import com.linkedin.venice.common.Measurable;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;

import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


public class RouterCache implements RoutingDataRepository.RoutingDataChangedListener {
  private static Logger logger = Logger.getLogger(RouterCache.class);

  private final RoutingDataRepository routingDataRepository;

  private static MessageDigest createMD5MessageDigest() throws NoSuchAlgorithmException {
    return MessageDigest.getInstance("MD5");
  }

  private static class CacheKey implements Measurable {
    private final int storeId;
    private final int version;
    private final byte[] key;
    private static final ThreadLocal<MessageDigest> messageDigest = ThreadLocal.withInitial(() -> {
      try {
        return createMD5MessageDigest();
      } catch (NoSuchAlgorithmException e) {
        return null;
      }
    });

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
      MessageDigest m = messageDigest.get();
      if (null == m) {
        // MD5 is not supported, so use the simple one
        int result = storeId;
        result = 31 * result + version;
        result = 31 * result + Arrays.hashCode(key);
        return result;
      } else {
        /**
         * Use MD5 digest to guarantee more even distribution.
         * The overhead is slightly higher than the simple hashcode calculation.
         */
        m.reset();
        m.update(key);
        byte[] digest = m.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        return bigInt.intValue();
      }
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

    // Check whether 'MD5' algorithm is available or not
    try {
      createMD5MessageDigest();
    } catch (NoSuchAlgorithmException e) {
      logger.error("MD5 algorithm is not available, it could impact the cache performance since the default"
          + " hashCode algorithm of CacheKey couldn't guarantee even distribution");
    }
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

  public long getCacheSize() {
    return cache.getCacheSize();
  }

  public long getEntryNum() {
    return cache.getEntryNum();
  }

  public long getEntryNumMaxDiffBetweenBuckets() {
    return cache.getEntryNumMaxDiffBetweenBuckets();
  }

  public long getCacheSizeMaxDiffBetweenBuckets() {
    return cache.getCacheSizeMaxDiffBetweenBuckets();
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
