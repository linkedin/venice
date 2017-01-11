package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.utils.HelixUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.validation.constraints.NotNull;

import com.linkedin.venice.utils.PathResourceRegistry;
import org.apache.log4j.Logger;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.collections.map.HashedMap;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;


/**
 * Use Helix as storage for stores. Cached all of stores in local copy and provide read operations. Once stores are
 * changed in ZK, this repository will get notification and update local copy to keep consistent with ZK copy.
 */
public class HelixReadOnlyStoreRepository implements ReadOnlyStoreRepository {
  private static final Logger logger = Logger.getLogger(HelixReadOnlyStoreRepository.class);

  public static final String STORES_PATH = "/Stores";

  /**
   * Interface defined readonly operations to access stores.
   */
  protected Map<String, Store> storeMap = new HashMap<>();

  /**
   * Listener used when there is any store be created or deleted.
   */
  private final StoreCreatedDeleteListener storeCreateDeleteListener = new StoreCreatedDeleteListener();
  /**
   * Listener used when the data of one store is changed.
   */
  private final StoreUpdateListener storeUpdateListener = new StoreUpdateListener();
  /**
   * Lock to control the concurrency requests to stores.
   */
  protected final ReadWriteLock metadataLock = new ReentrantReadWriteLock();

  /**
   * Data accessor of Zookeeper
   */
  protected ZkBaseDataAccessor<Store> dataAccessor;

  private final ZkClient zkClient;

  /**
   * A set of listeners which will be triggered when store created/deleted
   */
  protected Set<StoreDataChangedListener> dataChangedListenerSet = new HashSet<>();

  private final CachedResourceZkStateListener zkStateListener;
  /**
   * Root path of stores in Zookeeper.
   */
  protected final String rootPath;

  public HelixReadOnlyStoreRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
                                      @NotNull String clusterName) {
    this(zkClient, adapter, clusterName, new StoreJSONSerializer());
  }

  public HelixReadOnlyStoreRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
                                      @NotNull String clusterName, @NotNull VeniceSerializer<Store> serializer) {
    this.rootPath = HelixUtils.getHelixClusterZkPath(clusterName) + STORES_PATH;
    // TODO: Considering serializer should be thread-safe, we can share serializer across multiple
    // clusters, which means we can register the following paths:
    // Store serializer: /*/Stores/*
    String storesPath = rootPath + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY;
    adapter.registerSerializer(storesPath, serializer);
    zkClient.setZkSerializer(adapter);
    this.zkClient = zkClient;
    dataAccessor = new ZkBaseDataAccessor<>(zkClient);
    zkStateListener = new CachedResourceZkStateListener(this);
  }

  @Override
  public Store getStore(String name) {
    metadataLock.readLock().lock();
    try {
      if (storeMap.containsKey(name)) {
        return storeMap.get(name).cloneStore();
      } else {
        return null;
      }
    } finally {
      metadataLock.readLock().unlock();
    }
  }

  @Override
  public boolean hasStore(String name) {
    metadataLock.readLock().lock();
    try {
      return storeMap.containsKey(name);
    } finally {
      metadataLock.readLock().unlock();
    }
  }

  @Override
  public void refresh() {
    metadataLock.writeLock().lock();
    try {
      Map<String, Store> newStoreMap = new HashedMap();
      List<Store> stores = dataAccessor.getChildren(rootPath, null, AccessOption.PERSISTENT);
      logger.info("Load " + stores.size() + " stores from Helix");
      // Add stores to local copy.
      for (Store s : stores) {
        newStoreMap.put(s.getName(), s);
      }
      clear(); // clear local copy only if loading from ZK successfully.
      // replace the original map
      storeMap = newStoreMap;
      // add listeners.
      dataAccessor.subscribeChildChanges(rootPath, storeCreateDeleteListener);
      for (Store s : storeMap.values()) {
        dataAccessor.subscribeDataChanges(composeStorePath(s.getName()), storeUpdateListener);
      }
      logger.info("Put " + stores.size() + " stores to local copy.");
    } finally {
      metadataLock.writeLock().unlock();
    }
    // subscribe is the thread safe method
    zkClient.subscribeStateChanges(zkStateListener);
  }

  @Override
  public void clear() {
    // un-subscribe is the thread safe method
    zkClient.unsubscribeStateChanges(zkStateListener);
    metadataLock.writeLock().lock();
    try {
      dataAccessor.unsubscribeChildChanges(rootPath, storeCreateDeleteListener);
      for (String storeName : storeMap.keySet()) {
        dataAccessor.unsubscribeDataChanges(composeStorePath(storeName), storeUpdateListener);
      }
      storeMap.clear();
      logger.info("Clear stores from local copy.");
    } finally {
      metadataLock.writeLock().unlock();
    }
  }

  protected String composeStorePath(String name) {
    return this.rootPath + "/" + name;
  }

  protected String parseStoreNameFromPath(String path) {
    if (path.startsWith(rootPath + "/") && path.lastIndexOf('/') == rootPath.length()
        && path.lastIndexOf('/') < path.length() - 1) {
      return path.substring(path.lastIndexOf('/') + 1);
    } else {
      throw new VeniceException("Data path is invalid, expected:" + rootPath + "/${storename}");
    }
  }

  @Override
  public void registerStoreDataChangedListener(StoreDataChangedListener listener) {
    metadataLock.writeLock().lock();
    try {
      dataChangedListenerSet.add(listener);
    } finally {
      metadataLock.writeLock().unlock();
    }
  }

  @Override
  public void unregisterStoreDataChangedListener(StoreDataChangedListener listener) {
    metadataLock.writeLock().lock();
    try {
      dataChangedListenerSet.remove(listener);
    } finally {
      metadataLock.writeLock().unlock();
    }
  }

  protected void triggerStoreCreationListener(Store store) {
    for (StoreDataChangedListener listener : dataChangedListenerSet) {
      listener.handleStoreCreated(store);
    }
  }

  protected void triggerStoreDeletionListener(String storeName) {
    for (StoreDataChangedListener listener : dataChangedListenerSet) {
      listener.handleStoreDeleted(storeName);
    }
  }

  private class StoreCreatedDeleteListener implements IZkChildListener {

    @Override
    public void handleChildChange(String parentPath, List<String> storeNameList)
        throws Exception {
      logger.info("Received a store children change notification from ZK.");
      if (!parentPath.equals(rootPath)) {
        throw new VeniceException("The path of event is mismatched. Expected:" + rootPath + " Actual:" + parentPath);
      }
      metadataLock.writeLock().lock();
      try {
        List<String> addedChildren = new ArrayList<>();
        Set<String> deletedChildren = new HashSet<>(storeMap.keySet());

        // Find new stores and useless stores.
        for (String storeName : storeNameList) {
          if (!storeMap.containsKey(storeName)) {
            addedChildren.add(composeStorePath(storeName));
          } else {
            deletedChildren.remove(storeName);
          }
        }

        // Add new stores to local copy and add listeners.
        if (!addedChildren.isEmpty()) {
          // Get new stores from ZK.
          List<Store> addedStores = dataAccessor.get(addedChildren, null, AccessOption.PERSISTENT);
          for (Store store : addedStores) {
            storeMap.put(store.getName(), store);
            dataAccessor.subscribeDataChanges(composeStorePath(store.getName()), storeUpdateListener);
            triggerStoreCreationListener(store);
            logger.info("Store:" + store.getName() + " is added. Current version:" + store.getCurrentVersion());
          }
        }

        // Delete useless stores from local copy
        for (String storeName : deletedChildren) {
          dataAccessor.unsubscribeDataChanges(composeStorePath(storeName), storeUpdateListener);
          storeMap.remove(storeName);
          triggerStoreDeletionListener(storeName);
          logger.info("Store:" + storeName + " is deleted.");
        }
      } finally {
        metadataLock.writeLock().unlock();
      }
      //TODO invoke all of venice listeners here to notify the changes of stores.
    }
  }

  private class StoreUpdateListener implements IZkDataListener {

    @Override
    public void handleDataChange(String dataPath, Object data)
        throws Exception {
      logger.info("Received a store data change notification from ZK.");
      if (!(data instanceof Store)) {
        throw new VeniceException("Invalid notification, changed data is not a store.");
      }
      Store store = (Store) data;
      if (!dataPath.equals(composeStorePath(store.getName()))) {
        throw new VeniceException(
            "The path of event is mismatched. Expected:" + composeStorePath(store.getName()) + "Actual:" + dataPath);
      }
      metadataLock.writeLock().lock();
      try {
        storeMap.put(store.getName(), store);
        logger.info("Store:" + store.getName() + " is updated. Current version:" + store.getCurrentVersion());
      } finally {
        metadataLock.writeLock().unlock();
      }
    }

    @Override
    public void handleDataDeleted(String dataPath)
        throws Exception {
      logger.info("Received a store deleted notification from ZK.");
      String storeName = parseStoreNameFromPath(dataPath);
      metadataLock.writeLock().lock();
      try {
        dataAccessor.unsubscribeDataChanges(composeStorePath(storeName), storeUpdateListener);
        storeMap.remove(storeName);
        triggerStoreDeletionListener(storeName);
        logger.info("Store:" + storeName + " is deleted.");
      } finally {
        metadataLock.writeLock().unlock();
      }
    }
  }

}
