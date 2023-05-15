package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SystemStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This repository provides an read only interface to access both system store and regular venice store.
 */
public class HelixReadOnlyStoreRepositoryAdapter implements ReadOnlyStoreRepository {
  private static final Logger LOGGER = LogManager.getLogger(HelixReadOnlyStoreRepositoryAdapter.class);

  private final HelixReadOnlyZKSharedSystemStoreRepository systemStoreRepository;
  private final ReadOnlyStoreRepository regularStoreRepository;
  private final StoreDataChangedListener zkSharedStoreDataChangedListener;
  private final StoreDataChangedListener regularStoreDataChangedListener;
  private final Set<StoreDataChangedListener> listeners = new CopyOnWriteArraySet<>();
  private final String clusterName;

  public HelixReadOnlyStoreRepositoryAdapter(
      HelixReadOnlyZKSharedSystemStoreRepository systemStoreRepository,
      ReadOnlyStoreRepository regularStoreRepository,
      String clusterName) {
    this.systemStoreRepository = systemStoreRepository;
    this.regularStoreRepository = regularStoreRepository;
    this.zkSharedStoreDataChangedListener = new ZKSharedStoreDataChangedListener();
    this.systemStoreRepository.registerStoreDataChangedListener(this.zkSharedStoreDataChangedListener);
    this.regularStoreDataChangedListener = new VeniceStoreDataChangedListener();
    this.regularStoreRepository.registerStoreDataChangedListener(this.regularStoreDataChangedListener);
    this.clusterName = clusterName;
  }

  /**
   * Function to decide whether we should forward the request to the regular repositories.
   * @param systemStoreType
   * @return
   */
  static boolean forwardToRegularRepository(VeniceSystemStoreType systemStoreType) {
    /**
     * This is a regular Venice store or the existing system stores, which hasn't adopted the new repositories yet.
     * Check {@link VeniceSystemStoreType} to find more details.
     */
    return systemStoreType == null || !systemStoreType.isNewMedataRepositoryAdopted();
  }

  @Override
  public Store getStore(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (forwardToRegularRepository(systemStoreType)) {
      return regularStoreRepository.getStore(storeName);
    }
    // Get the regular store name
    String regularStoreName = systemStoreType.extractRegularStoreName(storeName);
    Store regularStore = regularStoreRepository.getStore(regularStoreName);
    if (regularStore == null) {
      return null;
    }
    // Get ZK shared system store name
    String zkSharedStoreName = systemStoreType.getZkSharedStoreName();
    Store zkSharedStore = systemStoreRepository.getStore(zkSharedStoreName);
    if (zkSharedStore == null) {
      return null;
    }

    return new SystemStore(zkSharedStore, systemStoreType, regularStore);
  }

  @Override
  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    Store store = getStore(storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName);
    }
    return store;
  }

  // test only
  Set<StoreDataChangedListener> getListeners() {
    return Collections.unmodifiableSet(listeners);
  }

  @Override
  public boolean hasStore(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (forwardToRegularRepository(systemStoreType)) {
      return regularStoreRepository.hasStore(storeName);
    }
    String zkSharedStoreName = systemStoreType.getZkSharedStoreName();
    String regularStoreName = systemStoreType.extractRegularStoreName(storeName);
    return systemStoreRepository.hasStore(zkSharedStoreName) && regularStoreRepository.hasStore(regularStoreName);
  }

  @Override
  public Store refreshOneStore(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (forwardToRegularRepository(systemStoreType)) {
      return regularStoreRepository.refreshOneStore(storeName);
    }
    String zkSharedStoreName = systemStoreType.getZkSharedStoreName();
    String regularStoreName = systemStoreType.extractRegularStoreName(storeName);
    systemStoreRepository.refreshOneStore(zkSharedStoreName);
    regularStoreRepository.refreshOneStore(regularStoreName);

    return getStore(storeName);
  }

  /**
   * So far, this function will return the regular Venice stores + the corresponding meta system store.
   *
   * TODO: if we want to support more system store types in this repo, we need to modify this function accordingly.
   * @return
   */
  @Override
  public List<Store> getAllStores() {
    // Get all the regular Venice stores
    List<Store> regularVeniceStores = regularStoreRepository.getAllStores();
    List<Store> allStores = new ArrayList<>(regularVeniceStores);
    // So far, only consider meta system store.
    Store zkSharedStoreForMetaSystemStore =
        systemStoreRepository.getStore(VeniceSystemStoreType.META_STORE.getZkSharedStoreName());
    Store zkSharedStoreForDaVinciPushStatusSystemStore =
        systemStoreRepository.getStore(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getZkSharedStoreName());
    // Populate all the systems stores for the regular Venice stores if they are enabled.
    regularVeniceStores.forEach(store -> {
      if (store.isStoreMetaSystemStoreEnabled()) {
        allStores.add(new SystemStore(zkSharedStoreForMetaSystemStore, VeniceSystemStoreType.META_STORE, store));
      }
      if (store.isDaVinciPushStatusStoreEnabled()) {
        allStores.add(
            new SystemStore(
                zkSharedStoreForDaVinciPushStatusSystemStore,
                VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE,
                store));
      }
    });
    return allStores;
  }

  @Override
  public long getTotalStoreReadQuota() {
    return regularStoreRepository.getTotalStoreReadQuota();
  }

  @Override
  public void registerStoreDataChangedListener(StoreDataChangedListener listener) {
    listeners.add(listener);
  }

  @Override
  public void unregisterStoreDataChangedListener(StoreDataChangedListener listener) {
    listeners.remove(listener);
  }

  @Override
  public int getBatchGetLimit(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (forwardToRegularRepository(systemStoreType)) {
      return regularStoreRepository.getBatchGetLimit(storeName);
    }

    // Then get the batch limit from the zk shared store
    return systemStoreRepository.getBatchGetLimit(systemStoreType.getZkSharedStoreName());
  }

  @Override
  public boolean isReadComputationEnabled(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (forwardToRegularRepository(systemStoreType)) {
      return regularStoreRepository.isReadComputationEnabled(storeName);
    }

    // Then get the batch limit from the zk shared store
    return systemStoreRepository.isReadComputationEnabled(systemStoreType.getZkSharedStoreName());
  }

  @Override
  public void refresh() {
    systemStoreRepository.refresh();
    regularStoreRepository.refresh();
  }

  @Override
  public void clear() {
    systemStoreRepository.clear();
    regularStoreRepository.clear();
  }

  /**
   * {@link StoreDataChangedListener} to handle all the events from {@link #systemStoreRepository}.
   */
  private class ZKSharedStoreDataChangedListener implements StoreDataChangedListener {
    /**
     * No need to handle zk shared store creation since the system store will be populated on the fly.
     */
    public void handleStoreCreated(Store store) {
      LOGGER.info(
          "Received a new zk shared store creation: {}, and current repository will do nothing.",
          store.getName());
    }

    /**
     * No need to handle zk shared store deletion since the system store will be populated on the fly.
     */
    public void handleStoreDeleted(String storeName) {
      LOGGER.info("Received a zk shared store deletion: {}, and current repository will do nothing", storeName);
    }

    /**
     * This function will iterate all the regular Venice stores and notify the changes to the corresponding system stores.
     * TODO: so far, this function only supports {@link VeniceSystemStoreType#META_STORE}, and if you plan to support
     * more system store types, this function needs to be changed accordingly.
     */
    public void handleStoreChanged(Store store) {
      /**
       * This function will iterate all the regular Venice stores and notify the changes to the corresponding system stores.
       */
      String zkSharedStoreName = store.getName();
      if (zkSharedStoreName.equals(VeniceSystemStoreType.META_STORE.getZkSharedStoreName())) {
        // Get all the affected system stores
        List<Store> regularStores = regularStoreRepository.getAllStores();
        for (Store regularStore: regularStores) {
          if (regularStore.isStoreMetaSystemStoreEnabled()) {
            SystemStore metaSystemStore = new SystemStore(store, VeniceSystemStoreType.META_STORE, regularStore);
            // Notify the change of meta system store.
            listeners.forEach(listener -> {
              try {
                listener.handleStoreChanged(metaSystemStore);
              } catch (Throwable t) {
                LOGGER.error(
                    "Received exception while invoking `handleStoreChanged` of listener: {} with system store: {}.",
                    listener.getClass(),
                    metaSystemStore.getName(),
                    t);
              }
            });
          }
        }
      } else if (zkSharedStoreName.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getZkSharedStoreName())) {
        // Get all the affected system stores
        List<Store> regularStores = regularStoreRepository.getAllStores();
        for (Store regularStore: regularStores) {
          if (regularStore.isDaVinciPushStatusStoreEnabled()) {
            SystemStore daVinciPushStatusSystemStore =
                new SystemStore(store, VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, regularStore);
            // Notify the change of da vinci push status system store.
            listeners.forEach(listener -> {
              try {
                listener.handleStoreChanged(daVinciPushStatusSystemStore);
              } catch (Throwable t) {
                LOGGER.error(
                    "Received exception while invoking `handleStoreChanged` of listener: {} with system store: {}.",
                    listener.getClass(),
                    daVinciPushStatusSystemStore.getName(),
                    t);
              }
            });
          }
        }
      } else {
        LOGGER.info("Received zk shared store change for store: {}, will be ignored.", zkSharedStoreName);
      }
    }
  }

  /**
   * {@link StoreDataChangedListener} to handle all the events from {@link #regularStoreDataChangedListener}.
   */
  public class VeniceStoreDataChangedListener implements StoreDataChangedListener {
    /**
     * Notify the store creation and maybe the corresponding system store creation.
     * TODO: so far, this function only supports {@link VeniceSystemStoreType#META_STORE}, and if you plan to support
     * more system store types, this function needs to be changed accordingly.
     * @param store
     */
    public void handleStoreCreated(Store store) {
      // Notify the regular store change
      listeners.forEach(listener -> {
        try {
          listener.handleStoreCreated(store);
        } catch (Throwable t) {
          LOGGER.error(
              "Received exception while invoking `handleStoreCreated` of listener: {} with store: {}.",
              listener.getClass(),
              store.getName(),
              t);
        }
      });
      String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(store.getName());
      if (store.isStoreMetaSystemStoreEnabled()) {
        // Notify the creation of meta system store.
        listeners.forEach(listener -> {
          try {
            SystemStore metaSystemStore = new SystemStore(
                systemStoreRepository.getStoreOrThrow(VeniceSystemStoreType.META_STORE.getZkSharedStoreName()),
                VeniceSystemStoreType.META_STORE,
                store);
            listener.handleStoreCreated(metaSystemStore);
          } catch (Throwable t) {
            LOGGER.error(
                "Received exception while invoking `handleStoreCreated` of listener: {} with system store: {}.",
                listener.getClass(),
                metaSystemStoreName,
                t);
          }
        });
      }
      String daVinciPushStatusSystemStoreName =
          VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(store.getName());
      if (store.isDaVinciPushStatusStoreEnabled()) {
        listeners.forEach(listener -> {
          try {
            SystemStore daVinciPushStatusSystemStore = new SystemStore(
                systemStoreRepository
                    .getStoreOrThrow(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getZkSharedStoreName()),
                VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE,
                store);
            listener.handleStoreCreated(daVinciPushStatusSystemStore);
          } catch (Throwable t) {
            LOGGER.error(
                "Received exception while invoking `handleStoreCreated` of listener: {} with system store: {}",
                listener.getClass(),
                daVinciPushStatusSystemStoreName,
                t);
          }
        });
      }
    }

    /**
     * Notify the store deletion and maybe the corresponding system store deletion.
     * TODO: so far, this function only supports {@link VeniceSystemStoreType#META_STORE}, and if you plan to support
     * more system store types, this function needs to be changed accordingly.
     * @param store
     */
    public void handleStoreDeleted(Store store) {
      String storeName = store.getName();
      getListeners().forEach(listener -> {
        // Notify the regular store deletion
        try {
          listener.handleStoreDeleted(store);
        } catch (Throwable t) {
          LOGGER.error(
              "Received exception while invoking `handleStoreDeleted` of listener: {} with store: {}.",
              listener.getClass(),
              storeName,
              t);
        }

        String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
        try {
          // Notify the meta system store deletion
          Store metaStore = systemStoreRepository.getStore(VeniceSystemStoreType.META_STORE.getZkSharedStoreName());
          if (metaStore != null) {
            SystemStore metaSystemStore = new SystemStore(metaStore, VeniceSystemStoreType.META_STORE, store);
            listener.handleStoreDeleted(metaSystemStore);
          }
        } catch (Throwable t) {
          LOGGER.error(
              "Received exception while invoking `handleStoreDeleted` of listener: {} with system store: {}.",
              listener.getClass(),
              metaSystemStoreName,
              t);
        }
        if (store.isDaVinciPushStatusStoreEnabled()) {
          String daVinciPushStatusSystemStoreName =
              VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
          try {
            // Notify the da vinci push status system store deletion
            listener.handleStoreDeleted(daVinciPushStatusSystemStoreName);
          } catch (Throwable t) {
            LOGGER.error(
                "Received exception while invoking `handleStoreDeleted` of listener: {} with system store: {}.",
                listener.getClass(),
                daVinciPushStatusSystemStoreName,
                t);
          }
        }
      });
    }

    /**
     * Notify the store change and maybe the corresponding system store change.
     * TODO: so far, this function only supports {@link VeniceSystemStoreType#META_STORE}, and if you plan to support
     * more system store types, this function needs to be changed accordingly.
     * @param store
     */
    public void handleStoreChanged(Store store) {
      listeners.forEach(listener -> {
        // Notify the regular store change
        try {
          listener.handleStoreChanged(store);
        } catch (Throwable t) {
          LOGGER.error(
              "Received exception while invoking `handleStoreChanged` of listener: {} with store: {}.",
              listener.getClass(),
              store.getName(),
              t);
        }
        // Notify the meta system store change
        String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(store.getName());
        if (store.isStoreMetaSystemStoreEnabled()) {
          try {
            /**
             * So far, this repo doesn't notify the system store creation in the following situation:
             * 1. The regular Venice store gets created.
             * 2. The meta system store feature is enabled later.
             *
             * In this situation, there is no store creation event for the system store, but just store change event.
             * If it is not acceptable, we could maintain a list here for all the available system stores, and if it
             * is not found, this function could notify the store creation event.
             */
            SystemStore metaSystemStore = new SystemStore(
                systemStoreRepository.getStoreOrThrow(VeniceSystemStoreType.META_STORE.getZkSharedStoreName()),
                VeniceSystemStoreType.META_STORE,
                store);
            listener.handleStoreChanged(metaSystemStore);
          } catch (Throwable t) {
            LOGGER.error(
                "Received exception while invoking `handleStoreDeleted` of listener: {}  with system store: {}.",
                listener.getClass(),
                metaSystemStoreName,
                t);
          }
        }
        if (store.isDaVinciPushStatusStoreEnabled()) {
          String daVinciPushStatusSystemStoreName =
              VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(store.getName());
          try {
            SystemStore daVinciPushStatusSystemStore = new SystemStore(
                systemStoreRepository
                    .getStoreOrThrow(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getZkSharedStoreName()),
                VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE,
                store);
            listener.handleStoreChanged(daVinciPushStatusSystemStore);
          } catch (Throwable t) {
            LOGGER.error(
                "Received exception while invoking `handleStoreDeleted` of listener: {} with system store: {}.",
                listener.getClass(),
                daVinciPushStatusSystemStoreName,
                t);
          }
        }
      });
    }
  }
}
