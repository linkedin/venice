package com.linkedin.davinci.store.cache;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


/**
 * Interface for a cache on a venice store.  This interface is meant to decouple venice code from different cache implementations
 * (for which, there are many).
 *
 * @param <K> The key type that should be returned from the cache.  Should match the type that is preferred to be read.
 * @param <V>
 */
public interface VeniceStoreCache {
  /**
   * Returns the value associated with the {@code key} in this cache, or {@code null} if there is no
   * cached value for the {@code key}.
   *
   * @param key the key associated to the desired value
   * @return
   */
  <K, V> V getIfPresent(K key);

  /**
   * Returns a map of the values associated with the {@code keys} in this cache. The returned map
   * will only contain entries which are already present in the cache.  Duplicate keys may be ignored (depends on
   * the implementation)
   * <p>
   *
   * @param keys the keys whose associated values are to be returned
   * @return the unmodifiable mapping of keys to values for the specified keys found in this cache
   * @throws NullPointerException if the specified collection is null or contains a null element
   */
  public <K, V> Map<K, V> getAllPresent(Iterable<K> keys);

  /**
   * Returns the value associated with the {@code key} in this cache, obtaining that value from the
   * {@code mappingFunction} when a value is not already associated to the key.
   *
   * @param key the key with which the specified value is to be associated
   * @param mappingFunction the function to compute a value
   * @return the current (existing or computed) value associated with the specified key, or null if
   *         the computed value is null
   * @throws NullPointerException if the specified key or mappingFunction is null
   * @throws IllegalStateException if the computation is recursive (and might never finish)
   * @throws RuntimeException if the mapping function completes exceptionally
   */
  public <K, V> CompletableFuture<V> get(K key, Function<K, V> mappingFunction);

  public <K, V> CompletableFuture<V> get(K key);

  /**
   * Returns a map of the values associated with the {@code keys}, creating or retrieving those
   * values if necessary. The returned map contains entries that were already cached, combined with
   * the newly loaded entries; it will never contain null keys or values.
   *
   * @param keys the keys whose associated values are to be returned
   * @param mappingFunction the function to compute the values
   * @return an unmodifiable mapping of keys to values for the specified keys in this cache
   * @throws NullPointerException if the specified collection is null or contains a null element, or
   *         if the map returned by the mappingFunction is null
   * @throws RuntimeException or Error if the mappingFunction does so
   */
  public <K, V> CompletableFuture<Map<K, V>> getAll(Iterable<K> keys, Function<Iterable<K>, Map<K, V>> mappingFunction);

  /**
   * Associates the {@code value} with the {@code key} in this cache. If the cache previously
   * contained a value associated with the {@code key}, the old value is replaced by the new
   * {@code value}.
   *
   * @param key
   * @param value
   */
  <K, V> void insert(K key, V value);

  /**
   * Discards any cached value for the {@code key}. The behavior of this operation is undefined for
   * an entry that is being loaded (or reloaded) and is otherwise not present.
   *
   * @param key the key whose mapping is to be removed from the cache
   * @throws NullPointerException if the specified key is null
   */
  <K> void invalidate(K key);

  /**
   * Discards all entries in the cache. The behavior of this operation is undefined for an entry
   * that is being loaded (or reloaded) and is otherwise not present.
   */
  void clear();

  /**
   * Performs any pending maintenance operations needed by the cache.  What gets done is implementation
   * dependant.
   */
  void close();

  /**
   * Returns the approximate number of entries in this cache.
   */
  long size();

  /**
   * Returns the ratio of cache requests which were hits. This is defined as
   * {@code hitCount / requestCount}, or {@code 1.0} when {@code requestCount == 0}. Note that
   * {@code hitRate + missRate =~ 1.0}.
   *
   * @return the ratio of cache requests which were hits
   */
  double hitRate();

  /**
   * Returns the approximate number of cache requests which were hits
   * @return the hit count
   */
  long hitCount();

  /**
   * Returns the approximate number of cache requests which were miss
   * @return the miss count
   */
  long missCount();
}
