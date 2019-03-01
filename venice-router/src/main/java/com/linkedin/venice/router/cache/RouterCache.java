package com.linkedin.venice.router.cache;

import com.linkedin.venice.common.Measurable;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;

import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.caffinitas.ohc.CacheSerializer;


public class RouterCache implements RoutingDataRepository.RoutingDataChangedListener, AutoCloseable {
  private static Logger logger = Logger.getLogger(RouterCache.class);

  private final RoutingDataRepository routingDataRepository;

  private static MessageDigest createMD5MessageDigest() throws NoSuchAlgorithmException {
    return MessageDigest.getInstance("MD5");
  }

  /**
   * Used in {@link OffHeapCache}.
   */
  public static class CacheKeySerializer implements CacheSerializer<CacheKey> {
    public static final int SIZE_OF_SERIALIZED_STORE_ID_AND_VERSION = 2 * Integer.BYTES;

    @Override
    public void serialize(CacheKey value, ByteBuffer buf) {
      buf.putInt(value.storeId);
      buf.putInt(value.version);
      value.keyBuffer.mark();
      buf.put(value.keyBuffer);
      value.keyBuffer.reset();
    }

    @Override
    public CacheKey deserialize(ByteBuffer buf) {
      int storeId = buf.getInt();
      int version = buf.getInt();
      int keyLen = buf.remaining();
      byte[] key = new byte[keyLen];
      buf.get(key);

      return new CacheKey(storeId, version, ByteBuffer.wrap(key));
    }

    @Override
    public int serializedSize(CacheKey value) {
      return SIZE_OF_SERIALIZED_STORE_ID_AND_VERSION + value.keyBuffer.remaining();
    }
  }

  public static class CacheKey implements Measurable {
    private final int storeId;
    private final int version;
    private final ByteBuffer keyBuffer;
    private static final ThreadLocal<MessageDigest> messageDigest = ThreadLocal.withInitial(() -> {
      try {
        return createMD5MessageDigest();
      } catch (NoSuchAlgorithmException e) {
        throw new VeniceException("Failed to create MD5 MessageDigest");
      }
    });

    public CacheKey(int storeId, int version, ByteBuffer keyBuffer) { //byte[] key) {
      this.storeId = storeId;
      this.version = version;
      this.keyBuffer = keyBuffer;
    }

    @Override
    public int getSize() {
      return Integer.BYTES + Integer.BYTES + keyBuffer.remaining();
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
      return keyBuffer.equals(cacheKey.keyBuffer);
    }

    @Override
    public int hashCode() {
      MessageDigest m = messageDigest.get();
      if (null == m) {
        // MD5 is not supported, so use the simple one
        int result = storeId;
        result = 31 * result + version;
        result = 31 * result + keyBuffer.hashCode();
        return result;
      } else {
        /**
         * Use MD5 digest to guarantee more even distribution.
         * The overhead is slightly higher than the simple hashcode calculation.
         */
        m.reset();
        m.update(keyBuffer.array(), keyBuffer.position(), keyBuffer.remaining());
        byte[] digest = m.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        return bigInt.intValue();
      }
    }
  }

  /**
   * Used in {@link OffHeapCache}.
   */
  public static class CacheValueSerializer implements CacheSerializer<Optional<CacheValue>> {
    public static final int INVALID_SCHEMA_ID = Integer.MIN_VALUE;
    public static final int SIZE_OF_SERIALIZED_SCHEMA_ID = Integer.BYTES;

    @Override
    public void serialize(Optional<CacheValue> value, ByteBuffer buf) {
      if (value.isPresent()) {
        int previousPosition = value.get().buf.position();
        buf.putInt(value.get().schemaId);
        buf.put(value.get().buf);
        // restore the previous position in case the ByteBuffer is needed again
        value.get().buf.position(previousPosition);
      } else {
        buf.putInt(INVALID_SCHEMA_ID);
      }
    }

    @Override
    public Optional<CacheValue> deserialize(ByteBuffer buf) {
      int schemaId = buf.getInt();
      if (INVALID_SCHEMA_ID == schemaId) {
        return Optional.empty();
      } else {
        return Optional.of(new CacheValue(buf, schemaId));
      }
    }

    @Override
    public int serializedSize(Optional<CacheValue> value) {
      if (value.isPresent()) {
        return SIZE_OF_SERIALIZED_SCHEMA_ID + value.get().getByteBuffer().remaining();
      }
      return SIZE_OF_SERIALIZED_SCHEMA_ID;
    }
  }

  public static class CacheValue implements Measurable {
    private final ByteBuffer buf;
    private final int schemaId;

    public CacheValue(ByteBuffer buf, int schemaId) {
      this.buf = buf;
      this.schemaId = schemaId;
    }

    public byte[] getValue() {
      int valueLen = buf.remaining();
      int previousPosition = buf.position();
      byte[] value = new byte[valueLen];
      buf.get(value);
      // restore the previous position in case the ByteBuffer is needed again
      buf.position(previousPosition);
      return value;
    }

    public ByteBuffer getByteBuffer() {
      return buf;
    }

    public int getSchemaId() {
      return schemaId;
    }

    @Override
    public int getSize() {
      return Integer.BYTES + buf.remaining();
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
      return buf.equals(that.buf);
    }

    @Override
    public int hashCode() {
      int previousPosition = buf.position();
      int valueLen = buf.remaining();
      int result = 1;
      for (int i = 0; i < valueLen; i++) {
        result = 31 * result + buf.get();
      }
      // restore the previous position in case the ByteBuffer is needed again
      buf.position(previousPosition);
      result = 31 * result + schemaId;
      return result;
    }
  }

  private final Cache<CacheKey, CacheValue> cache;
  private final Map<String, Integer> storeNameIdMapping = new VeniceConcurrentHashMap<>();
  private int storeId = 0;

  //store the int here since it's for constructing Http header
  private final Map<String, CompressionStrategy> compressionTypeCache;

  public RouterCache(CacheType cacheType, CacheEviction cacheEviction, long capacityInBytes, int concurrency,
      int hashTableSize, long cacheTTLmillis, RoutingDataRepository routingDataRepository) {
    if (cacheType.equals(CacheType.ON_HEAP_CACHE)) {
      // Check whether 'MD5' algorithm is available or not
      try {
        createMD5MessageDigest();
      } catch (NoSuchAlgorithmException e) {
        logger.error("MD5 algorithm is not available, it could impact the cache performance since the default"
            + " hashCode algorithm of CacheKey couldn't guarantee even distribution");
      }

      if (cacheEviction.equals(CacheEviction.LRU)) {
        this.cache = new OnHeapCache<>(capacityInBytes, concurrency);
      } else {
        throw new VeniceException("Not supported cache eviction algorithm for on-heap cache: " + cacheEviction);
      }
    } else if (cacheType.equals(CacheType.OFF_HEAP_CACHE)) {
      boolean isCacheTTLEnabled = (cacheTTLmillis <= 0) ? false : true;
      this.cache = new OffHeapCache<>(capacityInBytes, concurrency, hashTableSize, isCacheTTLEnabled, cacheTTLmillis,
          new CacheKeySerializer(), new CacheValueSerializer(), cacheEviction);
    } else {
      throw new VeniceException("Unknown cache type: " + cacheType);
    }
    this.compressionTypeCache = new ConcurrentHashMap<>();
    this.routingDataRepository = routingDataRepository;
  }

  @Override
  public void close() throws Exception {
    cache.close();
  }

  private int getStoreId(String storeName) {
    return storeNameIdMapping.computeIfAbsent(storeName, (k) -> storeId++ );
  }

  public void put(String storeName, int version, byte[] key, Optional<CacheValue> value) {
    put(storeName, version, ByteBuffer.wrap(key), value);
  }

  public void put(String storeName, int version, ByteBuffer keyBuffer, Optional<CacheValue> value) {
    CacheKey cacheKey = new CacheKey(getStoreId(storeName), version, keyBuffer);
    cache.put(cacheKey, value);
  }

  public Optional<CacheValue> get(String storeName, int version, byte[] key) {
    return get(storeName, version, ByteBuffer.wrap(key));
  }

  public Optional<CacheValue> get(String storeName, int version, ByteBuffer keyBuffer) {
    CacheKey cacheKey = new CacheKey(getStoreId(storeName), version, keyBuffer);
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
