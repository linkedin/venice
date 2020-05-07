package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;

import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.linkedin.venice.common.VeniceSystemStoreUtils.*;


public class CachedReadOnlyStoreRepository implements ReadOnlyStoreRepository {
  private static final Logger logger = Logger.getLogger(CachedReadOnlyStoreRepository.class);

  public static final String STORE_REPOSITORY_PATH = "/Stores";

  protected final String clusterName;
  protected final String clusterStoreRepositoryPath;

  protected final ZkClient zkClient;
  protected final ZkBaseDataAccessor<Store> zkDataAccessor;

  // A lock to make sure that all updates are serialized and events are delivered in the correct order
  protected final ReentrantLock updateLock = new ReentrantLock();
  protected final Map<String, Store> storeMap = new VeniceConcurrentHashMap<>();
  private final AtomicLong totalStoreReadQuota = new AtomicLong();
  private final Set<StoreDataChangedListener> listeners = new CopyOnWriteArraySet<>();


  public CachedReadOnlyStoreRepository(ZkClient zkClient, String clusterName, HelixAdapterSerializer compositeSerializer) {
    this.zkClient = zkClient;
    this.zkDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    this.clusterName = clusterName;
    this.clusterStoreRepositoryPath = Paths.get(HelixUtils.getHelixClusterZkPath(clusterName), STORE_REPOSITORY_PATH).toString();
    compositeSerializer.registerSerializer(clusterStoreRepositoryPath, new VeniceJsonSerializer<>(Integer.TYPE));
    compositeSerializer.registerSerializer(getStoreZkPath(PathResourceRegistry.WILDCARD_MATCH_ANY),  new StoreJSONSerializer());
    zkClient.setZkSerializer(compositeSerializer);
  }

  @Override
  public Store getStore(String storeName) {
    // TODO: refactor calls to this method to avoid unnecessary copying
    Store store = storeMap.get(getZkStoreName(storeName));
    if (store != null) {
      return store.cloneStore();
    }
    return null;
  }

  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    Store store = storeMap.get(getZkStoreName(storeName));
    if (store != null) {
      return store;
    }
    throw new VeniceNoStoreException(storeName, clusterName);
  }

  @Override
  public boolean hasStore(String storeName) {
    return storeMap.containsKey(getZkStoreName(storeName));
  }

  @Override
  public List<Store> getAllStores() {
    // TODO: refactor calls to this method to avoid unnecessary copying
    return new ArrayList<>(storeMap.values());
  }

  @Override
  public long getTotalStoreReadQuota() {
    return totalStoreReadQuota.get();
  }

  @Override
  public int getBatchGetLimit(String storeName) {
    return getStoreOrThrow(storeName).getBatchGetLimit();
  }

  @Override
  public boolean isReadComputationEnabled(String storeName) {
    return getStoreOrThrow(storeName).isReadComputationEnabled();
  }

  @Override
  public boolean isSingleGetRouterCacheEnabled(String storeName) {
    return getStoreOrThrow(storeName).isSingleGetRouterCacheEnabled();
  }

  @Override
  public boolean isBatchGetRouterCacheEnabled(String storeName) {
    return getStoreOrThrow(storeName).isBatchGetRouterCacheEnabled();
  }

  @Override
  public void refresh() {
    logger.info("Refresh started for cluster " + clusterName + "'s " + getClass().getSimpleName());
    updateLock.lock();
    try {
      List<Store> newStores = getStoresFromZk();
      Set<String> deletedStoreNames = storeMap.values().stream().map(Store::getName).collect(Collectors.toSet());
      for (Store newStore : newStores) {
        putStore(newStore);
        deletedStoreNames.remove(newStore.getName());
      }

      for (String storeName : deletedStoreNames) {
        removeStore(storeName);
      }
      logger.info("Refresh finished for cluster " + clusterName + "'s " + getClass().getSimpleName());
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public Store refreshOneStore(String storeName) {
    updateLock.lock();
    try {
      Store newStore = getStoreFromZk(storeName);
      if (newStore != null) {
        putStore(newStore);
      } else {
        removeStore(storeName);
      }
      return newStore;
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public void clear() {
    updateLock.lock();
    try {
      storeMap.clear();
      totalStoreReadQuota.set(0);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public void registerStoreDataChangedListener(StoreDataChangedListener listener) {
    listeners.add(listener);
  }

  @Override
  public void unregisterStoreDataChangedListener(StoreDataChangedListener listener) {
    listeners.remove(listener);
  }

  protected Store putStore(Store newStore) {
    updateLock.lock();
    try {
      Store oldStore = storeMap.put(getZkStoreName(newStore.getName()), newStore);
      if (oldStore == null) {
        totalStoreReadQuota.addAndGet(newStore.getReadQuotaInCU());
        notifyStoreCreated(newStore);
      } else if (!oldStore.equals(newStore)) {
        totalStoreReadQuota.addAndGet(newStore.getReadQuotaInCU() - oldStore.getReadQuotaInCU());
        notifyStoreChanged(newStore);
      }
      return oldStore;
    } finally {
      updateLock.unlock();
    }
  }

  protected Store removeStore(String storeName) {
    updateLock.lock();
    try {
      Store oldStore = storeMap.remove(getZkStoreName(storeName));
      if (oldStore != null) {
        totalStoreReadQuota.addAndGet(-oldStore.getReadQuotaInCU());
        notifyStoreDeleted(storeName);
      }
      return oldStore;
    } finally {
      updateLock.unlock();
    }
  }

  protected final String getStoreZkPath(String storeName) {
    return Paths.get(clusterStoreRepositoryPath, getZkStoreName(storeName)).toString();
  }

  protected Store getStoreFromZk(String storeName) {
    return zkDataAccessor.get(getStoreZkPath(storeName), null, AccessOption.PERSISTENT);
  }

  protected List<Store> getStoresFromZk() {
    List<Store> stores = zkDataAccessor.getChildren(clusterStoreRepositoryPath, null, AccessOption.PERSISTENT);
    stores.removeIf(Objects::isNull);
    return stores;
  }

  protected List<Store> getStoresFromZk(Collection<String> storeNames) {
    List<String> paths = storeNames.stream().map(this::getStoreZkPath).collect(Collectors.toList());
    List<Store> stores = zkDataAccessor.get(paths, null, AccessOption.PERSISTENT);
    stores.removeIf(Objects::isNull);
    return stores;
  }

  protected void notifyStoreCreated(Store store) {
    for (StoreDataChangedListener listener : listeners) {
      try {
        listener.handleStoreCreated(store);
      } catch (Throwable e) {
        logger.error("Could not handle store creation event for store: " + store.getName(), e);
      }
    }
  }

  protected void notifyStoreDeleted(String storeName) {
    for (StoreDataChangedListener listener : listeners) {
      try {
        listener.handleStoreDeleted(storeName);
      } catch (Throwable e) {
        logger.error("Could not handle store deletion event for store: " + storeName, e);
      }
    }
  }

  protected void notifyStoreChanged(Store store) {
    for (StoreDataChangedListener listener : listeners) {
      try {
        listener.handleStoreChanged(store);
      } catch (Throwable e) {
        logger.error("Could not handle store updating event for store: " + store.getName(), e);
      }
    }
  }
}
