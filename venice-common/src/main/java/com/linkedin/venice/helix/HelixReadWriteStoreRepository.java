package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.HelixUtils;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * Read write store repository which uses Helix as storage.
 * <p>
 * This repository do NOT listen the change of store from ZK. Because in Venice, this is the only once place to modify
 * stores.
 */
public class HelixReadWriteStoreRepository extends CachedReadOnlyStoreRepository implements ReadWriteStoreRepository {
  public HelixReadWriteStoreRepository(
      ZkClient zkClient,
      HelixAdapterSerializer compositeSerializer,
      String clusterName,
      int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs) {
    super(zkClient, clusterName, compositeSerializer);
  }

  @Override
  public void addStore(Store store) {
    updateLock.lock();
    try {
      if (hasStore(store.getName())) {
        throw new VeniceStoreAlreadyExistsException(store.getName(), clusterName);
      }
      HelixUtils.update(zkDataAccessor, getStoreZkPath(store.getName()), store);
      putStore(store);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public void updateStore(Store store) {
    updateLock.lock();
    try {
      if (!hasStore(store.getName())) {
        throw new VeniceNoStoreException(store.getName(), clusterName);
      }
      HelixUtils.update(zkDataAccessor, getStoreZkPath(store.getName()), store);
      putStore(store);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public void deleteStore(String storeName) {
    updateLock.lock();
    try {
      if (!hasStore(storeName)) {
        throw new VeniceNoStoreException(storeName, clusterName);
      }
      HelixUtils.remove(zkDataAccessor, getStoreZkPath(storeName));
      removeStore(storeName);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public Store refreshOneStore(String storeName) {
    return getStore(storeName);
  }

  @Override
  public void lock() {
    updateLock.lock();
  }

  @Override
  public void unLock() {
    updateLock.unlock();
  }
}
