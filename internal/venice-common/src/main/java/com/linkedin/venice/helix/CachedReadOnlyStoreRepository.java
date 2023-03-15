package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CachedReadOnlyStoreRepository implements ReadOnlyStoreRepository {
  private static final Logger LOGGER = LogManager.getLogger(CachedReadOnlyStoreRepository.class);

  public static final String STORE_REPOSITORY_PATH = "/Stores";

  protected final String clusterName;
  protected final String clusterStoreRepositoryPath;

  protected final ZkClient zkClient;
  protected final ZkBaseDataAccessor<Store> zkDataAccessor;

  protected final ClusterLockManager clusterLockManager;
  protected final Map<String, Store> storeMap = new VeniceConcurrentHashMap<>();
  private final AtomicLong totalStoreReadQuota = new AtomicLong();
  private final Set<StoreDataChangedListener> listeners = new CopyOnWriteArraySet<>();

  public CachedReadOnlyStoreRepository(
      ZkClient zkClient,
      String clusterName,
      HelixAdapterSerializer compositeSerializer,
      ClusterLockManager clusterLockManager) {
    this.zkClient = zkClient;
    this.zkDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    this.clusterName = clusterName;
    this.clusterStoreRepositoryPath =
        Paths.get(HelixUtils.getHelixClusterZkPath(clusterName), STORE_REPOSITORY_PATH).toString();
    compositeSerializer.registerSerializer(clusterStoreRepositoryPath, new VeniceJsonSerializer<>(Integer.TYPE));
    compositeSerializer
        .registerSerializer(getStoreZkPath(PathResourceRegistry.WILDCARD_MATCH_ANY), new StoreJSONSerializer());
    zkClient.setZkSerializer(compositeSerializer);
    this.clusterLockManager = clusterLockManager;
  }

  @Override
  public Store getStore(String storeName) {
    Store store = storeMap.get(storeName);
    if (store != null) {
      return new ReadOnlyStore(store);
    }
    return refreshOneStore(storeName);
  }

  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    Store store = storeMap.get(storeName);
    if (store != null) {
      return new ReadOnlyStore(store);
    }
    throw new VeniceNoStoreException(storeName, clusterName);
  }

  @Override
  public boolean hasStore(String storeName) {
    return storeMap.containsKey(storeName);
  }

  @Override
  public List<Store> getAllStores() {
    return storeMap.values().stream().map(s -> new ReadOnlyStore(s)).collect(Collectors.toList());
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
  public void refresh() {
    LOGGER.info("Refresh started for cluster {}'s ", clusterName, getClass().getSimpleName());
    try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
      List<Store> newStores = getStoresFromZk();
      LOGGER.info(
          "Got {} stores from cluster {} during refresh in repo: {}",
          newStores.size(),
          clusterName,
          getClass().getSimpleName());
      Set<String> deletedStoreNames = storeMap.values().stream().map(Store::getName).collect(Collectors.toSet());
      for (Store newStore: newStores) {
        putStore(newStore);
        deletedStoreNames.remove(newStore.getName());
      }

      for (String storeName: deletedStoreNames) {
        removeStore(storeName);
      }
      LOGGER.info("Refresh finished for cluster {}'s {}", clusterName, getClass().getSimpleName());
    }
  }

  @Override
  public Store refreshOneStore(String storeName) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      Store newStore = getStoreFromZk(storeName);
      if (newStore != null) {
        putStore(newStore);
      } else {
        removeStore(storeName);
      }
      return newStore;
    }
  }

  @Override
  public void clear() {
    try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
      storeMap.clear();
      totalStoreReadQuota.set(0);
      clusterLockManager.clear();
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
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(newStore.getName())) {
      // Workaround to make old metadata compatible with new fields
      newStore.fixMissingFields();

      Store oldStore = storeMap.put(newStore.getName(), newStore);
      if (oldStore == null) {
        totalStoreReadQuota.addAndGet(newStore.getReadQuotaInCU());
        notifyStoreCreated(newStore);
      } else if (!oldStore.equals(newStore)) {
        totalStoreReadQuota.addAndGet(newStore.getReadQuotaInCU() - oldStore.getReadQuotaInCU());
        notifyStoreChanged(newStore);
      }
      return oldStore;
    }
  }

  protected Store removeStore(String storeName) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      Store oldStore = storeMap.remove(storeName);
      if (oldStore != null) {
        totalStoreReadQuota.addAndGet(-oldStore.getReadQuotaInCU());
        notifyStoreDeleted(oldStore);
      }
      return oldStore;
    }
  }

  protected final String getStoreZkPath(String storeName) {
    return Paths.get(clusterStoreRepositoryPath, storeName).toString();
  }

  protected Store getStoreFromZk(String storeName) {
    return zkDataAccessor.get(getStoreZkPath(storeName), null, AccessOption.PERSISTENT);
  }

  /**
   * {@link HelixReadOnlyZKSharedSystemStoreRepository} is overriding this function to filter out
   * stores, which are not necessary to put a watch against during {@link #refresh()}, and if this logic to refresh the zk
   * store repository gets changed in the future, we need to update {@link HelixReadOnlyZKSharedSystemStoreRepository}
   * accordingly.
   */
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
    for (StoreDataChangedListener listener: listeners) {
      try {
        listener.handleStoreCreated(store);
      } catch (Throwable e) {
        LOGGER.error("Could not handle store creation event for store: {}", store.getName(), e);
      }
    }
  }

  protected void notifyStoreDeleted(Store store) {
    for (StoreDataChangedListener listener: listeners) {
      try {
        listener.handleStoreDeleted(store);
      } catch (Throwable e) {
        LOGGER.error("Could not handle store deletion event for store: {}", store.getName(), e);
      }
    }
  }

  protected void notifyStoreChanged(Store store) {
    for (StoreDataChangedListener listener: listeners) {
      try {
        listener.handleStoreChanged(store);
      } catch (Throwable e) {
        LOGGER.error("Could not handle store updating event for store: {}", store.getName(), e);
      }
    }
  }
}
