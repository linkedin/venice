package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;

import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HelixReadOnlyStoreRepository extends CachedReadOnlyStoreRepository {
  private static final Logger logger = Logger.getLogger(HelixReadOnlyStoreRepository.class);

  public HelixReadOnlyStoreRepository(
      ZkClient zkClient,
      HelixAdapterSerializer compositeSerializer,
      String clusterName,
      int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs) {
    super(zkClient, clusterName, compositeSerializer);
  }

  @Override
  public void refresh() {
    updateLock.lock();
    try {
      zkClient.subscribeStateChanges(zkStateListener);
      zkDataAccessor.subscribeChildChanges(clusterStoreRepositoryPath, zkStoreRepositoryListener);
      super.refresh();
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public void clear() {
    updateLock.lock();
    try {
      zkClient.unsubscribeStateChanges(zkStateListener);
      zkDataAccessor.unsubscribeChildChanges(clusterStoreRepositoryPath, zkStoreRepositoryListener);
      for (String storeName : storeMap.values().stream().map(Store::getName).collect(Collectors.toSet())) {
        zkDataAccessor.unsubscribeDataChanges(getStoreZkPath(storeName), zkStoreListener);
      }
      super.clear();
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  protected Store putStore(Store newStore) {
    updateLock.lock();
    try {
      Store oldStore = super.putStore(newStore);
      if (oldStore == null) {
        zkDataAccessor.subscribeDataChanges(getStoreZkPath(newStore.getName()), zkStoreListener);
      }
      return oldStore;
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  protected Store removeStore(String storeName) {
    updateLock.lock();
    try {
      Store oldStore = super.removeStore(storeName);
      if (oldStore != null) {
        zkDataAccessor.unsubscribeDataChanges(getStoreZkPath(storeName), zkStoreListener);
      }
      return oldStore;
    } finally {
      updateLock.unlock();
    }
  }

  protected void onStoreChanged(Store newStore) {
    Store oldStore = putStore(newStore);
    if (oldStore == null) {
      logger.warn("Out of order store change notification, storeName=" + newStore.getName());
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
    updateLock.lock();
    try {
      Set<String> addedZkStoreNames = new HashSet<>(newZkStoreNames);
      List<String> deletedZkStoreNames = new ArrayList<>();
      for (String zkStoreName : storeMap.keySet()) {
        if (!addedZkStoreNames.remove(zkStoreName)) {
          deletedZkStoreNames.add(zkStoreName);
        }
      }

      List<Store> addedStores = getStoresFromZk(addedZkStoreNames);
      for (Store newStore : addedStores) {
        putStore(newStore);
      }

      for (String zkStoreName : deletedZkStoreNames) {
        removeStore(storeMap.get(zkStoreName).getName());
      }
    } finally {
      updateLock.unlock();
    }
  }

  private final CachedResourceZkStateListener zkStateListener = new CachedResourceZkStateListener(this);

  private final IZkChildListener zkStoreRepositoryListener = new IZkChildListener() {
    @Override
    public void handleChildChange(String path, List<String> children) {
      if (!path.equals(clusterStoreRepositoryPath)) {
        logger.warn("Notification path mismatch, path=" + path + ", expected=" + clusterStoreRepositoryPath);
        return;
      }
      onRepositoryChanged(children);
    }
  };

  private final IZkDataListener zkStoreListener = new IZkDataListener() {
    @Override
    public void handleDataChange(String path, Object data) {
      if (!(data instanceof Store)) {
        logger.warn("Notification data is not a Store, path=" + path + ", data=" + data);
        return;
      }

      Store store = (Store) data;
      String storePath = getStoreZkPath(store.getName());
      if (!path.equals(storePath)) {
        logger.warn("Notification path mismatch, path=" + path + ", expected=" + storePath);
        return;
      }
      onStoreChanged(store);
    }

    @Override
    public void handleDataDeleted(String path) {
    }
  };
}
