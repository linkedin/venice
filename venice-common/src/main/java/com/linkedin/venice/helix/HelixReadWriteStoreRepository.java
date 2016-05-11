package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceSerializer;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * Read write store repository which is use Helix as storage.
 * <p>
 * This repository do NOT listen the change of store from ZK. Because in Venice, this is the only once place to modify
 * stores.
 */
public class HelixReadWriteStoreRepository extends HelixReadonlyStoreRepository implements ReadWriteStoreRepository {
  private static final Logger logger = Logger.getLogger(HelixReadWriteStoreRepository.class.getName());

  public HelixReadWriteStoreRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adaper,
      @NotNull String clusterName) {
    super(zkClient, adaper, clusterName);
  }

  public HelixReadWriteStoreRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
      @NotNull String clusterName, @NotNull VeniceSerializer<Store> serializer) {
    super(zkClient, adapter, clusterName, serializer);
  }

  @Override
  public void updateStore(Store store) {
    lock();
    try {
      if (!storeMap.containsKey(store.getName())) {
        throw new VeniceException("Store:" + store.getName() + " dose not exist.");
      }
      Store originalStore = storeMap.get(store.getName());
      if (!originalStore.equals(store)) {
        dataAccessor.set(composeStorePath(store.getName()), store, AccessOption.PERSISTENT);
        storeMap.put(store.getName(), store);
      } else {
        logger.warn(
            "Given store:" + store.getName() + " is same as current one in local copy. Will NOT update it to ZK.");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public void deleteStore(String name) {
    lock();
    try {
      dataAccessor.remove(composeStorePath(name), AccessOption.PERSISTENT);
      storeMap.remove(name);
    } finally {
      unLock();
    }
  }

  @Override
  public void addStore(Store store) {
    lock();
    try {
      if (storeMap.containsKey(store.getName())) {
        throw new VeniceException("Store:" + store.getName() + " already exists.");
      }
      dataAccessor.set(composeStorePath(store.getName()), store, AccessOption.PERSISTENT);
      storeMap.put(store.getName(), store);
    } finally {
      unLock();
    }
  }

  @Override
  public void refresh() {
    lock();
    clear();
    try {
      List<Store> stores = dataAccessor.getChildren(rootPath, null, AccessOption.PERSISTENT);
      logger.info("Load " + stores.size() + " stores from Helix");
      for (Store s : stores) {
        storeMap.put(s.getName(), s);
      }
      logger.info("Put " + stores.size() + " stores to local copy.");
    } finally {
      unLock();
    }
  }

  @Override
  public void clear() {
    lock();
    try {
      storeMap.clear();
      logger.info("Clear stores from local copy.");
    } finally {
      unLock();
    }
  }

  /**
   * Lock the repository to do some operations atomically.
   */
  public void lock() {
    metadataLock.writeLock().lock();
  }

  /**
   * Unlock the repository.
   */
  public void unLock() {
    metadataLock.writeLock().unlock();
  }
}
