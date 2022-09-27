package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HelixReadOnlyStoreRepository extends CachedReadOnlyStoreRepository {
  private static final Logger LOGGER = LogManager.getLogger(HelixReadOnlyStoreRepository.class);

  public HelixReadOnlyStoreRepository(
      ZkClient zkClient,
      HelixAdapterSerializer compositeSerializer,
      String clusterName,
      int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs) {
    /**
     * HelixReadOnlyStoreRepository is used in router, server, fast-client, da-vinci and system store.
     * Its centralized locking should NOT be shared with other classes. Create a new instance.
     */
    super(zkClient, clusterName, compositeSerializer, new ClusterLockManager(clusterName));
  }

  @Override
  public void refresh() {
    try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
      zkClient.subscribeStateChanges(zkStateListener);
      zkDataAccessor.subscribeChildChanges(clusterStoreRepositoryPath, zkStoreRepositoryListener);
      super.refresh();
    }
  }

  @Override
  public void clear() {
    try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
      zkClient.unsubscribeStateChanges(zkStateListener);
      zkDataAccessor.unsubscribeChildChanges(clusterStoreRepositoryPath, zkStoreRepositoryListener);
      for (String storeName: storeMap.values().stream().map(Store::getName).collect(Collectors.toSet())) {
        zkDataAccessor.unsubscribeDataChanges(getStoreZkPath(storeName), zkStoreListener);
      }
      super.clear();
    }
  }

  @Override
  protected Store putStore(Store newStore) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(newStore.getName())) {
      Store oldStore = super.putStore(newStore);
      if (oldStore == null) {
        zkDataAccessor.subscribeDataChanges(getStoreZkPath(newStore.getName()), zkStoreListener);
        // Refresh the store after subscription to prevent missed updates. Here is the exact scenario:
        // 1. Put store in read repo.
        // 2. Store updated in read write repo.
        // 3. Subscribe to data change in read repo.
        // Updates in step 2 will not be reflected until the next update.
        refreshOneStore(newStore.getName());
      }
      return oldStore;
    }
  }

  @Override
  protected Store removeStore(String storeName) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      Store oldStore = super.removeStore(storeName);
      if (oldStore != null) {
        zkDataAccessor.unsubscribeDataChanges(getStoreZkPath(storeName), zkStoreListener);
      }
      return oldStore;
    }
  }

  protected void onStoreChanged(Store newStore) {
    Store oldStore = putStore(newStore);
    if (oldStore == null) {
      LOGGER.warn("Out of order store change notification, storeName={}.", newStore.getName());
    }
  }

  /**
   * {@link HelixReadOnlyZKSharedSystemStoreRepository} is overriding this function to filter out
   * stores, which are not necessary to put a watch against, and if this logic to monitor the zk
   * store repository gets changed in the future, we need to update {@link HelixReadOnlyZKSharedSystemStoreRepository}
   * accordingly.
   * @param newZkStoreNames
   */
  protected void onRepositoryChanged(Collection<String> newZkStoreNames) {
    try (AutoCloseableLock ignore = clusterLockManager.createClusterWriteLock()) {
      Set<String> addedZkStoreNames = new HashSet<>(newZkStoreNames);
      List<String> deletedZkStoreNames = new ArrayList<>();
      for (String zkStoreName: storeMap.keySet()) {
        if (!addedZkStoreNames.remove(zkStoreName)) {
          deletedZkStoreNames.add(zkStoreName);
        }
      }

      List<Store> addedStores = getStoresFromZk(addedZkStoreNames);
      for (Store newStore: addedStores) {
        putStore(newStore);
      }

      for (String zkStoreName: deletedZkStoreNames) {
        removeStore(storeMap.get(zkStoreName).getName());
      }
    }
  }

  private final CachedResourceZkStateListener zkStateListener = new CachedResourceZkStateListener(this);

  private final IZkChildListener zkStoreRepositoryListener = new IZkChildListener() {
    @Override
    public void handleChildChange(String path, List<String> children) {
      if (!path.equals(clusterStoreRepositoryPath)) {
        LOGGER.warn("Notification path mismatch, path={}, expected={}.", path, clusterStoreRepositoryPath);
        return;
      }
      onRepositoryChanged(children);
    }
  };

  private final IZkDataListener zkStoreListener = new IZkDataListener() {
    @Override
    public void handleDataChange(String path, Object data) {
      if (!(data instanceof Store)) {
        LOGGER.warn("Notification data is not a Store, path={}, data={}.", path, data);
        return;
      }

      Store store = (Store) data;
      String storePath = getStoreZkPath(store.getName());
      if (!path.equals(storePath)) {
        LOGGER.warn("Notification path mismatch, path={}, expected={}.", path, storePath);
        return;
      }
      onStoreChanged(store);
    }

    @Override
    public void handleDataDeleted(String path) {
    }
  };
}
