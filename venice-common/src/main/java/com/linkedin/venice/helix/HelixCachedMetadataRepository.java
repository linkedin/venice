package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.StoreListChangedListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.validation.constraints.NotNull;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * Metadata repository  Helix.
 * <p>
 * To ensure doing read and update atomically, use the lock() and unlock() method to lock the repository at first then
 * do operations, at last release the lock of repository.
 */
public class HelixCachedMetadataRepository extends HelixMetadataRepository {

    private static final Logger logger = Logger.getLogger(HelixCachedMetadataRepository.class.getName());

    /**
     * Local map of all stores read from Zookeeper.
     */
    private Map<String, Store> storeMap;
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
    private final ReadWriteLock metadataLock = new ReentrantReadWriteLock();

    public HelixCachedMetadataRepository(@NotNull ZkClient zkClient, @NotNull String rootPath) {
        super(zkClient, rootPath);
    }

    public void init() {
        metadataLock.writeLock().lock();
        try {
            storeMap = new HashMap<>();

            List<Store> stores = dataAccessor.getChildren(rootPath, null, AccessOption.PERSISTENT);
            logger.info("Load " + stores.size() + " stores from Helix");
            this.subscribeStoreListChanged(storesChangedListener);

            internalAddStores(stores);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Add store to local map and register data chaned listener for each of them.
     *
     * @param stores
     */
    private void internalAddStores(List<Store> stores) {
        for (Store s : stores) {
            if (storeMap.containsKey(s.getName())) {
                continue;
            }
            storeMap.put(s.getName(), s);
            this.subscribeStoreDataChanged(s.getName(), storeDataChangedListener);
        }
    }

    @Override
    public Store getStore(@NotNull String name) {
        metadataLock.readLock().lock();
        try {
            if (storeMap.containsKey(name)) {
                return storeMap.get(name).cloneStore();
            }
            return null;
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public void deleteStore(@NotNull String name) {
        metadataLock.writeLock().lock();
        try {
            dataAccessor.remove(composeStorePath(name), AccessOption.PERSISTENT);
            storeMap.remove(name);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    public void addStore(@NotNull Store store) {
        metadataLock.writeLock().lock();
        try {
            if (storeMap.containsKey(store.getName())) {
                throw new IllegalArgumentException("Store" + store.getName() + " already exists.");
            }
            dataAccessor.set(composeStorePath(store.getName()), store, AccessOption.PERSISTENT);
            this.subscribeStoreDataChanged(store.getName(), storeDataChangedListener);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    public void updateStore(@NotNull Store store) {
        metadataLock.writeLock().lock();
        try {
            if (!storeMap.containsKey(store.getName())) {
                throw new IllegalArgumentException("Store" + store.getName() + " dose not exist.");
            }
            Store originalStore = storeMap.get(store.getName());
            if (!originalStore.equals(store)) {
                dataAccessor.set(composeStorePath(store.getName()), store, AccessOption.PERSISTENT);
                storeMap.put(store.getName(), store);
            }
        } finally {
            metadataLock.writeLock().unlock();
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

    private class CachedStoresChangedListener implements StoreListChangedListener {

        @Override
        public void handleStoreListChanged(List<String> storeNameList) {
            metadataLock.writeLock().lock();
            try {
                List<String> addedChildren = new ArrayList<>();
                Set<String> deletedChildren = new HashSet<>(storeMap.keySet());

                for (String child : storeNameList) {
                    if (!storeMap.containsKey(child)) {
                        addedChildren.add(composeStorePath(child));
                    } else {
                        deletedChildren.remove(child);
                    }
                }

                if (addedChildren.size() > 0) {
                    List<Store> addedStores = dataAccessor.get(addedChildren, null, AccessOption.PERSISTENT);
                    internalAddStores(addedStores);
                }

                if (deletedChildren.size() > 0) {
                    //Delete from ZK at first then loccal cache.
                    dataAccessor.remove(new ArrayList<>(deletedChildren), AccessOption.PERSISTENT);
                    for (String deletedChild : deletedChildren) {
                        storeMap.remove(deletedChild);
                    }
                }
            } finally {
                metadataLock.writeLock().unlock();
            }
        }
    }

    private class CachedStoreDataChangedListener implements StoreDataChangedListener {

        @Override
        public void handleStoreUpdated(Store store) {
            metadataLock.writeLock().lock();
            try {
                storeMap.put(store.getName(), store);
            } finally {
                metadataLock.writeLock().unlock();
            }
        }

        @Override
        public void handleStoreDeleted(String storeName) {
            metadataLock.writeLock().lock();
            try {
                storeMap.remove(storeName);
            } finally {
                metadataLock.writeLock().unlock();
            }
        }
    }
}
