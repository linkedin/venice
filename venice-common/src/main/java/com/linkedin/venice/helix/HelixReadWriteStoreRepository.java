package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.HelixUtils;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;

import static com.linkedin.venice.common.VeniceSystemStoreUtils.*;


/**
 * Read write store repository which uses Helix as storage.
 * <p>
 * This repository do NOT listen the change of store from ZK. Because in Venice, this is the only once place to modify
 * stores.
 */
public class HelixReadWriteStoreRepository extends CachedReadOnlyStoreRepository implements ReadWriteStoreRepository {
  private final Optional<MetaStoreWriter> metaStoreWriter;
  private final String clusterName;

  public HelixReadWriteStoreRepository(
      ZkClient zkClient,
      HelixAdapterSerializer compositeSerializer,
      String clusterName,
      int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs,
      Optional<MetaStoreWriter> metaStoreWriter) {
    super(zkClient, clusterName, compositeSerializer);
    this.clusterName = clusterName;
    this.metaStoreWriter = metaStoreWriter;
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
      if (store.isStoreMetaSystemStoreEnabled() && metaStoreWriter.isPresent()) {
        /**
         * Write the update to the meta system store RT topic.
         */
        metaStoreWriter.get().writeStoreProperties(clusterName, store);
      }
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
  public Store getStore(String storeName) {
    Store store = storeMap.get(getZkStoreName(storeName));
    if (store != null) {
      return store.cloneStore();
    }
    return null;
  }

  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    Store store = getStore(storeName);
    if (null == store) {
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

  @Override
  public void lock() {
    updateLock.lock();
  }

  @Override
  public void unLock() {
    updateLock.unlock();
  }
}
