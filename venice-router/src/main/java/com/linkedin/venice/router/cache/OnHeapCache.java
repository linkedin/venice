package com.linkedin.venice.router.cache;

import com.linkedin.venice.common.Measurable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;


/**
 * Concurrent LRU Cache on heap
 */
public class OnHeapCache<K extends Measurable, V extends Measurable> implements Cache<K, V> {
  /**
   * Map node overhead estimation for each node in {@link LinkedHashMap}.
   */
  private static int MAP_NODE_OVERHEAD = 50;

  private ArrayList<InternalLRUCache<K, V>> caches;

  public OnHeapCache(long capacityInBytes, int concurrency) {
    long perCacheCapacityInBytes = capacityInBytes / concurrency;

    caches = new ArrayList<>(concurrency);
    for (int i = 0; i < concurrency; ++i) {
      caches.add(new InternalLRUCache<>(perCacheCapacityInBytes));
    }
  }

  private InternalLRUCache getCache(K key) {
    // To avoid Math.abs overflow through dividing hash code by 2
    return caches.get(Math.abs(key.hashCode() / 2) % caches.size());
  }

  @Override
  public boolean put(K key, Optional<V> value) {
    return getCache(key).put(key, value);
  }

  @Override
  public Optional<V> get(K key) {
    return getCache(key).get(key);
  }

  @Override
  public boolean remove(K key) {
    return getCache(key).remove(key);
  }

  @Override
  public synchronized void clear() {
    caches.forEach( cache -> cache.clear() );
  }

  @Override
  public long getEntryNum() {
    long entryNum = 0;
    for (Cache cache : caches) {
      entryNum += cache.getEntryNum();
    }
    return entryNum;
  }

  @Override
  public long getCacheSize() {
    long cacheSize = 0;
    for (Cache cache : caches) {
      cacheSize += cache.getCacheSize();
    }
    return cacheSize;
  }

  @Override
  public long getEntryNumMaxDiffBetweenBuckets() {
    long maxEntryNum = 0;
    long minEntryNum = Long.MAX_VALUE;
    for (Cache cache : caches) {
      maxEntryNum = Math.max(maxEntryNum, cache.getEntryNum());
      minEntryNum = Math.min(minEntryNum, cache.getEntryNum());
    }

    return maxEntryNum - minEntryNum;
  }

  @Override
  public long getCacheSizeMaxDiffBetweenBuckets() {
    long maxCacheSize = 0;
    long minCacheSize = Long.MAX_VALUE;
    for (Cache cache : caches) {
      maxCacheSize = Math.max(maxCacheSize, cache.getCacheSize());
      minCacheSize = Math.min(minCacheSize, cache.getCacheSize());
    }

    return maxCacheSize - minCacheSize;
  }
  public void close() throws IOException {
    clear();
  }

  /**
   * Thread-safe OnHeapCache
   */
  private static class InternalLRUCache<K extends Measurable, V extends Measurable> implements Cache<K, V> {
    private LinkedHashMap<K, Optional<V>> map = new LinkedHashMap<>(100, 0.75f, true);
    private final long capacityInBytes;
    private long spaceLeft;

    public static<K extends Measurable, V extends Measurable> int getMapNodeSize(K key, Optional<V> value) {
      return key.getSize() + (value.isPresent() ? value.get().getSize() : 0) + MAP_NODE_OVERHEAD;
    }

    public InternalLRUCache(long capacityInBytes) {
      this.capacityInBytes = capacityInBytes;
      this.spaceLeft = this.capacityInBytes;
    }

    public synchronized boolean put(K key, Optional<V> value) {
      int nodeSize = getMapNodeSize(key, value);
      if (nodeSize > capacityInBytes) {
        throw new RuntimeException(
            "Couldn't put key/value pair since the internal node size: " + nodeSize +
                " is greater than capacity: " + capacityInBytes);
      }
      if (spaceLeft < nodeSize) {
        // Doesn't have enough space
        Iterator<Map.Entry<K, Optional<V>>> it = map.entrySet().iterator();
        while (spaceLeft < nodeSize && it.hasNext()) {
          Map.Entry<K, Optional<V>> current = it.next();
          spaceLeft += getMapNodeSize(current.getKey(), current.getValue());
          it.remove();
        }
      }
      spaceLeft -= nodeSize;
      map.put(key, value);
      return true;
    }

    public synchronized Optional<V> get(K key) {
      return map.get(key);
    }

    public synchronized boolean remove(K key) {
      Optional<V> value = map.remove(key);
      if (null != value) {
        int nodeSize = getMapNodeSize(key, value);
        spaceLeft += nodeSize;
        return true;
      }

      return false;
    }

    @Override
    public synchronized void clear() {
      map.clear();
      spaceLeft = capacityInBytes;
    }

    @Override
    public synchronized long getEntryNum() {
      return map.size();
    }

    @Override
    public synchronized long getCacheSize() {
      return capacityInBytes - spaceLeft;
    }

    @Override
    public long getEntryNumMaxDiffBetweenBuckets() {
      return 0;
    }

    @Override
    public long getCacheSizeMaxDiffBetweenBuckets() {
      return 0;
    }
    public void close() throws IOException {
    }
  }
}
