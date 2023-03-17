package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * Read write store repository which uses Helix as storage.
 * <p>
 * This repository does NOT listen the change of store from ZK. Because in Venice, this is the only once place to modify
 * stores.
 */
public class HelixReadWriteStoreRepository extends CachedReadOnlyStoreRepository implements ReadWriteStoreRepository {
  private final Optional<MetaStoreWriter> metaStoreWriter;
  private final String clusterName;

  public HelixReadWriteStoreRepository(
      ZkClient zkClient,
      HelixAdapterSerializer compositeSerializer,
      String clusterName,
      Optional<MetaStoreWriter> metaStoreWriter,
      ClusterLockManager storeLock) {
    super(zkClient, clusterName, compositeSerializer, storeLock);
    this.clusterName = clusterName;
    this.metaStoreWriter = metaStoreWriter;
  }

  @Override
  public void addStore(Store store) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(store.getName())) {
      if (hasStore(store.getName())) {
        throw new VeniceStoreAlreadyExistsException(store.getName(), clusterName);
      }
      HelixUtils.update(zkDataAccessor, getStoreZkPath(store.getName()), store);
      putStore(store);
    }
  }

  @Override
  public void updateStore(Store store) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(store.getName())) {
      if (!hasStore(store.getName())) {
        throw new VeniceNoStoreException(store.getName(), clusterName);
      }
      HelixUtils.update(zkDataAccessor, getStoreZkPath(store.getName()), store);
      putStore(store);
      if (store.isStoreMetaSystemStoreEnabled() && metaStoreWriter.isPresent()) {
        /**
         * Write the update to the meta system store RT topic.
         */
        metaStoreWriter.get().writeStoreProperties(clusterName, store);
      }
    }
  }

  @Override
  public void deleteStore(String storeName) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      if (!hasStore(storeName)) {
        throw new VeniceNoStoreException(storeName, clusterName);
      }
      HelixUtils.remove(zkDataAccessor, getStoreZkPath(storeName));
      removeStore(storeName);
    }
  }

  @Override
  public Store getStore(String storeName) {
    Store store = storeMap.get(storeName);
    if (store != null) {
      return store.cloneStore();
    }
    return null;
  }

  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    Store store = getStore(storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    return store;
  }

  @Override
  public List<Store> getAllStores() {
    return storeMap.values().stream().map(s -> s.cloneStore()).collect(Collectors.toList());
  }

  @Override
  public Store refreshOneStore(String storeName) {
    return getStore(storeName);
  }
}
