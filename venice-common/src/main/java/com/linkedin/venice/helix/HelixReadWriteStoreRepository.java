package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.utils.HelixUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import org.apache.commons.collections.map.HashedMap;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * Read write store repository which uses Helix as storage.
 * <p>
 * This repository do NOT listen the change of store from ZK. Because in Venice, this is the only once place to modify
 * stores.
 */
public class HelixReadWriteStoreRepository extends HelixReadonlyStoreRepository implements ReadWriteStoreRepository {
  private static final Logger logger = Logger.getLogger(HelixReadWriteStoreRepository.class);

  //TODO get retry count from configuration.
  private int retryCount = 2;

  public HelixReadWriteStoreRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
      @NotNull String clusterName) {
    super(zkClient, adapter, clusterName);
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
        throw new VeniceNoStoreException("Store:" + store.getName() + " does not exist.");
      }
      HelixUtils.update(dataAccessor,composeStorePath(store.getName()),store,retryCount);
      storeMap.put(store.getName(), store);
    } finally {
      unLock();
    }
  }

  @Override
  public void deleteStore(String name) {
    lock();
    try {
      if (storeMap.containsKey(name)) {
        HelixUtils.remove(dataAccessor,composeStorePath(name),retryCount);
        storeMap.remove(name);
        triggerStoreDeletionListener(name);
        logger.info("Store:" + name + " is deleted.");
      }
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
      HelixUtils.update(dataAccessor,composeStorePath(store.getName()),store,retryCount);
      storeMap.put(store.getName(), store);
      triggerStoreCreationListener(store);
      logger.info("Store:" + store.getName() + " is added.");
    } finally {
      unLock();
    }
  }

  public List<Store> listStores() {
    List<Store> storeList = new ArrayList<>();
    lock();
    try{
      Iterator<Store> storeIter = storeMap.values().iterator();
      while (storeIter.hasNext()){
        storeList.add(storeIter.next().cloneStore());
      }
    } finally {
      unLock();
    }
    return storeList;
  }

  @Override
  public void refresh() {
    lock();
    try {
      Map<String, Store> newStoreMap = new HashedMap();
      List<Store> stores = dataAccessor.getChildren(rootPath, null, AccessOption.PERSISTENT);
      logger.info("Load " + stores.size() + " stores from Helix");
      for (Store s : stores) {
        newStoreMap.put(s.getName(), s);
      }
      clear(); // clear local copy only if loading from ZK successfully.
      storeMap = newStoreMap;
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
