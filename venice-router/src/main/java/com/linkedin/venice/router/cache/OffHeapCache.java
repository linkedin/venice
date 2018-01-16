package com.linkedin.venice.router.cache;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.util.Optional;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;


/**
 * Current off-heap implementation is based on {@link OHCache}.
 * https://github.com/snazy/ohc
 *
 * @param <K>
 * @param <V>
 */
public class OffHeapCache<K, V> implements Cache<K, V> {
  private final OHCache<K, Optional<V>> ohCache;

  public OffHeapCache(long capacityInBytes,
      int concurrency,
      int hashTableSize,
      CacheSerializer<K> keySerializer,
      CacheSerializer<Optional<V>> valueSerializer,
      CacheEviction cacheEviction) {
    if (!checkPowerOfTwo(concurrency)) {
      throw new VeniceException("Concurrency should be power of 2, but was: " + concurrency);
    }
    if (!checkPowerOfTwo(hashTableSize)) {
      throw new VeniceException("Hash table size should be power of 2, but was: " + hashTableSize);
    }
    OHCacheBuilder<K, Optional<V>> cacheBuilder = OHCacheBuilder.newBuilder();
    cacheBuilder.keySerializer(keySerializer)
        .valueSerializer(valueSerializer)
        .eviction(convertEviction(cacheEviction))
        .capacity(capacityInBytes)
        .segmentCount(concurrency)
        .hashTableSize(hashTableSize)
        .throwOOME(true);
    ohCache = cacheBuilder.build();
  }

  private boolean checkPowerOfTwo(int i) {
    return 0 == (i & (i - 1));
  }

  private Eviction convertEviction(CacheEviction cacheEviction) {
    if (cacheEviction == CacheEviction.LRU) {
      return Eviction.LRU;
    }
    if (cacheEviction == CacheEviction.W_TINY_LFU) {
      return Eviction.W_TINY_LFU;
    }
    throw new VeniceException("Unknown cache eviction: " + cacheEviction);
  }

  @Override
  public boolean put(K key, Optional<V> value) {
    return ohCache.put(key, value);
  }

  @Override
  public Optional<V> get(K key) {
    return ohCache.get(key);
  }

  @Override
  public boolean remove(K key) {
    return ohCache.remove(key);
  }

  @Override
  public void clear() {
    ohCache.clear();
  }

  @Override
  public long getEntryNum() {
    return ohCache.size();
  }

  @Override
  public long getCacheSize() {
    return ohCache.memUsed();
  }

  @Override
  public long getEntryNumMaxDiffBetweenBuckets() {
    long[] perSegmentSizes = ohCache.perSegmentSizes();
    long maxEntrySize = 0;
    long minEntrySize = Long.MAX_VALUE;
    for (int i = 0; i < perSegmentSizes.length; ++i) {
      maxEntrySize = Math.max(maxEntrySize, perSegmentSizes[i]);
      minEntrySize = Math.min(minEntrySize, perSegmentSizes[i]);
    }
    return maxEntrySize - minEntrySize;

  }

  @Override
  public long getCacheSizeMaxDiffBetweenBuckets() {
    /**
     * Not available based on current API of {@link OHCache}.
     */
    return -1;
  }

  @Override
  public void close() throws IOException {
    ohCache.close();
  }
}
