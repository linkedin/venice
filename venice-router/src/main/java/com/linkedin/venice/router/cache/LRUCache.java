package com.linkedin.venice.router.cache;

import com.linkedin.venice.common.Measurable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;


/**
 * Concurrent LRU Cache on heap
 */
public class LRUCache<K extends Measurable, V extends Measurable> implements Cache<K, V> {
  /**
   * Map node overhead estimation for each node in {@link LinkedHashMap}.
   */
  private static int MAP_NODE_OVERHEAD = 50;

  private ArrayList<InternalLRUCache<K, V>> caches;

  public LRUCache(long capacityInBytes, int concurrency) {
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
  public Optional<V> put(K key, V value) {
    return getCache(key).put(key, value);
  }

  @Override
  public Optional<V> get(K key) {
    return getCache(key).get(key);
  }

  @Override
  public Optional<V> remove(K key) {
    return getCache(key).remove(key);
  }

  @Override
  public synchronized void clear() {
    caches.forEach( cache -> cache.clear() );
  }

  /**
   * Thread-safe LRUCache
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

    public synchronized Optional<V> put(K key, V value) {
      if (null == value) {
        return putInternal(key, Optional.empty());
      }
      return putInternal(key, Optional.of(value));
    }

    private Optional<V> putInternal(K key, Optional<V> value) {
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
      return map.put(key, value);
    }

    public synchronized Optional<V> get(K key) {
      return map.get(key);
    }

    public synchronized Optional<V> remove(K key) {
      Optional<V> value = map.remove(key);
      if (null != value) {
        int nodeSize = getMapNodeSize(key, value);
        spaceLeft += nodeSize;
      }

      return value;
    }

    @Override
    public synchronized void clear() {
      map.clear();
      spaceLeft = capacityInBytes;
    }
  }
}
