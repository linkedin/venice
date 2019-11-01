package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.manager.zk.ZkClient;
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
      for (String storeName : storeMap.keySet()) {
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

  protected void onRepositoryChanged(Collection<String> newStoreNames) {
    updateLock.lock();
    try {
      Set<String> addedStoreNames = new HashSet<>(newStoreNames);
      List<String> deletedStoreNames = new ArrayList<>();
      for (String storeName : storeMap.keySet()) {
        if (!addedStoreNames.remove(storeName)) {
          deletedStoreNames.add(storeName);
        }
      }

      List<Store> addedStores = getStoresFromZk(addedStoreNames);
      for (Store newStore : addedStores) {
        putStore(newStore);
      }

      for (String storeName : deletedStoreNames) {
        removeStore(storeName);
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
