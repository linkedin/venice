package com.linkedin.davinci.store.cache.backend;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.cache.VeniceStoreCacheStorageEngine;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is in charge of managing access and maintenance of a VeniceStoreCache.  It makes available simple CRUD
 * operations that can be performed on the cache
 */
public class ObjectCacheBackend {
  private static final Logger LOGGER = LogManager.getLogger(ObjectCacheBackend.class);

  private final VeniceConcurrentHashMap<String, VeniceStoreCacheStorageEngine> versionTopicToCacheEngineMap =
      new VeniceConcurrentHashMap<>();
  private final ObjectCacheConfig storeCacheConfig;
  private final StoreCacheStats storeCacheStats;
  private final ReadOnlySchemaRepository schemaRepository;

  /**
   * @param clientConfig configurations that enabled the VeniceStoreCacheBackend to interact with a venice cluster (to set up ingestion) as well as
   *                     store information (such as the store name that is being cached).
   * @param cacheConfig configurations that configure the cache policy (TTL, size, etc.)
   */
  public ObjectCacheBackend(
      ClientConfig clientConfig,
      ObjectCacheConfig cacheConfig,
      ReadOnlySchemaRepository schemaRepository) {
    this.storeCacheConfig = cacheConfig;
    this.schemaRepository = schemaRepository;
    MetricsRepository metricsRepository = Optional.ofNullable(clientConfig.getMetricsRepository())
        .orElse(TehutiUtils.getMetricsRepository(String.format("venice-store-cache-%s", clientConfig.getStoreName())));
    storeCacheStats = new StoreCacheStats(metricsRepository, clientConfig.getStoreName());
  }

  public synchronized void close() {
    // iterate through any version engines and clean up.
    versionTopicToCacheEngineMap.forEach((k, v) -> {
      LOGGER.info("Closing VeniceStoreCacheBackend for store vers: " + k);
      if (v != null) {
        v.drop();
        v.close();
      }
    });
    versionTopicToCacheEngineMap.clear();
    // Clears out metrics
    storeCacheStats.registerServingCache(null);
  }

  public void clearCachedPartitions(Version version) {
    versionTopicToCacheEngineMap.get(version.kafkaTopicName()).drop();
  }

  // TODO: This is a very confusing function for general use. One would expect the cacheLoader function passed to this
  // method would be honored every call (kind of like the get with a mapping function). But it's only conditionally
  // honored if it's the first get called. The best way to remedy this is to define an AsyncCacheLoader factory which
  // is parameterized by the serving version and key. With that we could have a more sane interface.
  public <K, V> CompletableFuture<V> get(K key, Version version, AsyncCacheLoader<K, V> cacheLoader) {
    VeniceStoreCacheStorageEngine engine = versionTopicToCacheEngineMap
        .computeIfAbsent(version.kafkaTopicName(), (k) -> buildCacheEngine(version, cacheLoader));
    if (engine != null) {
      return engine.getCache().get(key);
    }
    return CompletableFuture.completedFuture(null);
  }

  public <K, V> CompletableFuture<Map<K, V>> getAll(
      Iterable<K> keys,
      Version version,
      Function<Iterable<K>, Map<K, V>> mappingFunction,
      AsyncCacheLoader<K, V> cacheLoader) {
    VeniceStoreCacheStorageEngine engine = versionTopicToCacheEngineMap
        .computeIfAbsent(version.kafkaTopicName(), (k) -> buildCacheEngine(version, cacheLoader));
    if (engine != null) {
      return engine.getCache().getAll(keys, mappingFunction);
    }
    return CompletableFuture.completedFuture(new HashMap<K, V>());
  }

  public AbstractStorageEngine getStorageEngine(String topicName) {
    AbstractStorageEngine engine = versionTopicToCacheEngineMap.get(topicName);
    if (engine != null) {
      return versionTopicToCacheEngineMap.get(topicName);
    }
    return null;
  }

  private VeniceStoreCacheStorageEngine buildCacheEngine(Version version, AsyncCacheLoader cacheLoader) {
    VeniceStoreCacheStorageEngine cacheStorageEngine = new VeniceStoreCacheStorageEngine(
        version.kafkaTopicName(),
        storeCacheConfig,
        schemaRepository.getKeySchema(version.getStoreName()).getSchema(),
        cacheLoader);
    // register the stats for this engine as it's now serving traffic
    storeCacheStats.registerServingCache(cacheStorageEngine.getCache());
    return cacheStorageEngine;
  }

  public <K, V> void update(K key, V val, Version version, AsyncCacheLoader<K, V> cacheLoader) {
    VeniceStoreCacheStorageEngine engine = versionTopicToCacheEngineMap
        .computeIfAbsent(version.kafkaTopicName(), k -> buildCacheEngine(version, cacheLoader));
    if (engine != null) {
      engine.putDeserializedValue(key, val);
    }
  }

  private final StoreDataChangedListener cacheInvalidatingStoreChangeListener = new StoreDataChangedListener() {
    @Override
    public void handleStoreChanged(Store store) {
      // new updates will register new cache engines, but we are on the hook to clean up any old versions
      String storeName = store.getName();
      Set<String> upToDateVersionsSet =
          store.getVersions().stream().map(Version::kafkaTopicName).collect(Collectors.toSet());
      for (String key: versionTopicToCacheEngineMap.keySet()) {
        if (!upToDateVersionsSet.contains(key)) {
          // This is no longer in the version list, so clean it out
          LOGGER.info(String.format("Closing VeniceStoreCacheBackend for store:%s version:%s", storeName, key));
          VeniceStoreCacheStorageEngine cache = versionTopicToCacheEngineMap.remove(key);
          cache.drop();
          cache.close();
        }
      }
    }

    @Override
    public void handleStoreDeleted(Store store) {
      synchronized (versionTopicToCacheEngineMap) {
        for (VeniceStoreCacheStorageEngine cache: versionTopicToCacheEngineMap.values()) {
          cache.drop();
          cache.close();
        }
        versionTopicToCacheEngineMap.clear();
      }
    }
  };

  public StoreDataChangedListener getCacheInvalidatingStoreChangeListener() {
    return cacheInvalidatingStoreChangeListener;
  }

  public ObjectCacheConfig getStoreCacheConfig() {
    return this.storeCacheConfig;
  }
}
