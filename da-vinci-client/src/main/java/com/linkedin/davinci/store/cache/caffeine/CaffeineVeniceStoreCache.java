package com.linkedin.davinci.store.cache.caffeine;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.davinci.store.cache.VeniceStoreCache;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.checkerframework.checker.nullness.qual.NonNull;


public class CaffeineVeniceStoreCache implements VeniceStoreCache {
  private final @NonNull AsyncLoadingCache caffeineCache;

  public CaffeineVeniceStoreCache(ObjectCacheConfig cacheConfig, AsyncCacheLoader loadingFunction) {
    Caffeine builder = Caffeine.newBuilder();
    cacheConfig.getTtlInMilliseconds().ifPresent(aLong -> builder.expireAfterWrite(aLong, TimeUnit.MILLISECONDS));
    cacheConfig.getMaxCacheSize().ifPresent(builder::maximumSize);
    this.caffeineCache = builder.buildAsync(loadingFunction);
  }

  @Override
  public <K, V> V getIfPresent(K key) {
    return (V) caffeineCache.synchronous().getIfPresent(key);
  }

  @Override
  public <K, V> Map<K, V> getAllPresent(Iterable<K> keys) {
    return caffeineCache.synchronous().getAllPresent(keys);
  }

  @Override
  public <K, V> CompletableFuture<V> get(K key) {
    return caffeineCache.get(key);
  }

  @Override
  public <K, V> CompletableFuture<V> get(K key, Function<K, V> mappingFunction) {
    return caffeineCache.get(key, mappingFunction);
  }

  @Override
  public <K, V> CompletableFuture<Map<K, V>> getAll(
      Iterable<K> keys,
      Function<Iterable<K>, Map<K, V>> mappingFunction) {
    return caffeineCache.getAll(keys, mappingFunction);
  }

  @Override
  public <K, V> void insert(K key, V value) {
    caffeineCache.put(key, CompletableFuture.completedFuture(value));
  }

  @Override
  public <K> void invalidate(K key) {
    caffeineCache.synchronous().invalidate(key);
  }

  @Override
  public void clear() {
    caffeineCache.synchronous().invalidateAll();
  }

  @Override
  public void close() {
    caffeineCache.synchronous().cleanUp();
  }

  @Override
  public long size() {
    return caffeineCache.synchronous().estimatedSize();
  }

  @Override
  public double hitRate() {
    return caffeineCache.synchronous().stats().hitRate();
  }

  @Override
  public long hitCount() {
    return caffeineCache.synchronous().stats().hitCount();
  }

  @Override
  public long missCount() {
    return caffeineCache.synchronous().stats().missCount();
  }
}
