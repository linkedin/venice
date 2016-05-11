package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadonlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceSerializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import javax.validation.constraints.NotNull;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;


/**
 * Use Helix as storage for stores. Cached all of stores in local copy and provide read operations. Once stores are
 * changed in ZK, this repository will get notification and update local copy to keep consistent with ZK copy.
 */
public class HelixReadonlyStoreRepository implements ReadonlyStoreRepository {
  private static final Logger logger = Logger.getLogger(HelixReadonlyStoreRepository.class.getName());

  public static final String STORES_PATH = "/Stores";
  /**
   * Interface defined readonly operations to access stores.
   */
  protected Map<String, Store> storeMap = new HashMap<>();

  /**
   * Listener used when there is any store be created or deleted.
   */
  private final CachedStoresChangedListener storesChangedListener = new CachedStoresChangedListener();
  /**
   * Listener used when the data of one store is changed.
   */
  private final CachedStoreDataChangedListener storeDataChangedListener = new CachedStoreDataChangedListener();
  /**
   * Lock to control the concurrency requests to stores.
   */
  protected final ReadWriteLock metadataLock = new ReentrantReadWriteLock();

  /**
   * Data accessor of Zookeeper
   */
  protected ZkBaseDataAccessor<Store> dataAccessor;

  /**
   * Root path of stores in Zookeeper.
   */
  protected final String rootPath;

  public HelixReadonlyStoreRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adaper,
      @NotNull String clusterName) {
    this(zkClient, adaper, clusterName, new StoreJSONSerializer());
  }

  public HelixReadonlyStoreRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
      @NotNull String clusterName, @NotNull VeniceSerializer<Store> serializer) {
    this.rootPath = "/" + clusterName + STORES_PATH;
    adapter.registerSerializer(rootPath, serializer);
    zkClient.setZkSerializer(adapter);
    dataAccessor = new ZkBaseDataAccessor<>(zkClient);
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
  public void refresh() {
    metadataLock.writeLock().lock();
    clear();
    try {
      List<Store> stores = dataAccessor.getChildren(rootPath, null, AccessOption.PERSISTENT);
      logger.info("Load " + stores.size() + " stores from Helix");
      dataAccessor.subscribeChildChanges(rootPath, storesChangedListener);

      // Add stores to local copy and add listeners.
      for (Store s : stores) {
        storeMap.put(s.getName(), s);
        dataAccessor.subscribeDataChanges(composeStorePath(s.getName()), storeDataChangedListener);
      }
      logger.info("Put " + stores.size() + " stores to local copy.");
    } finally {
      metadataLock.writeLock().unlock();
    }
  }

  @Override
  public void clear() {
    metadataLock.writeLock().lock();
    try {
      dataAccessor.unsubscribeChildChanges(rootPath, storesChangedListener);
      for (String storeName : storeMap.keySet()) {
        dataAccessor.unsubscribeDataChanges(composeStorePath(storeName), storeDataChangedListener);
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

  private class CachedStoresChangedListener implements IZkChildListener {

    @Override
    public void handleChildChange(String parentPath, List<String> storeNameList)
        throws Exception {
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
        if (addedChildren.size() > 0) {
          // Get new stores from ZK.
          List<Store> addedStores = dataAccessor.get(addedChildren, null, AccessOption.PERSISTENT);
          for (Store store : addedStores) {
            storeMap.put(store.getName(), store);
            dataAccessor.subscribeDataChanges(composeStorePath(store.getName()), storeDataChangedListener);
          }
        }

        // Delete useless stores from local copy
        if (deletedChildren.size() > 0) {
          for (String storeName : deletedChildren) {
            dataAccessor.unsubscribeDataChanges(composeStorePath(storeName), storeDataChangedListener);
            storeMap.remove(storeName);
          }
        }
      } finally {
        metadataLock.writeLock().unlock();
      }

      // Notify venice lisener
    }
  }

  private class CachedStoreDataChangedListener implements IZkDataListener {

    @Override
    public void handleDataChange(String dataPath, Object data)
        throws Exception {
      Store store = (Store) data;
      if (!dataPath.equals(composeStorePath(store.getName()))) {
        throw new VeniceException(
            "The path of event is mismatched. Expected:" + composeStorePath(store.getName()) + "Actual:" + dataPath);
      }
      metadataLock.writeLock().lock();
      try {
        storeMap.put(store.getName(), store);
      } finally {
        metadataLock.writeLock().unlock();
      }
    }

    @Override
    public void handleDataDeleted(String dataPath)
        throws Exception {
      String storeName = dataPath.substring(dataPath.lastIndexOf('/') + 1);
      if (!dataPath.startsWith(rootPath) || storeName.isEmpty()) {
        throw new VeniceException(
            "The path of event is mismatched. Expected:" + rootPath + "/$StoreName Actual:" + dataPath);
      }
      metadataLock.writeLock().lock();
      try {
        storeMap.remove(storeName);
      } finally {
        metadataLock.writeLock().unlock();
      }
    }
  }
}
