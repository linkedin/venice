package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SystemStore;


/**
 * This repository provides an read-write interface to access both system store and regular venice store.
 */
public class HelixReadWriteStoreRepositoryAdapter extends HelixReadOnlyStoreRepositoryAdapter
    implements ReadWriteStoreRepository {
  private final ReadWriteStoreRepository regularStoreRepository;

  public HelixReadWriteStoreRepositoryAdapter(
      HelixReadOnlyZKSharedSystemStoreRepository systemStoreRepository,
      ReadWriteStoreRepository regularStoreRepository,
      String clusterName) {
    super(systemStoreRepository, regularStoreRepository, clusterName);
    this.regularStoreRepository = regularStoreRepository;
  }

  @Override
  public Store getStore(String storeName) {
    Store store = super.getStore(storeName);
    /**
     * Since the returned store from {@link HelixReadOnlyStoreRepositoryAdapter#getStore} will be referring to
     * {@link ReadOnlyStore} internally, here will clone the store to make it mutable.
     */
    if (store != null) {
      return store.cloneStore();
    }
    return null;
  }

  @Override
  public void updateStore(Store store) {
    String storeName = store.getName();
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (forwardToRegularRepository(systemStoreType)) {
      regularStoreRepository.updateStore(store);
    } else {
      if (!(store instanceof SystemStore)) {
        throw new VeniceException("SystemStore type expected here for store: " + storeName);
      }
      /**
       * For system store, we only update the info stored in the corresponding Venice regular store,
       * since shared properties are not mutable in {@link SystemStore} api.
       */
      SystemStore systemStore = (SystemStore) store;
      regularStoreRepository.updateStore(systemStore.getVeniceStore());
    }
  }

  /**
   * This function only be used to delete regular venice store, and the system store is bundled with regular venice store.
   */
  @Override
  public void deleteStore(String name) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(name);
    if (forwardToRegularRepository(systemStoreType)) {
      regularStoreRepository.deleteStore(name);
    } else {
      throw new VeniceException("System store: " + name + " can't be deleted directly");
    }
  }

  /**
   * This function only be used to add regular venice store, and the system store is bundled with regular venice store.
   */
  @Override
  public void addStore(Store store) {
    String storeName = store.getName();
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (forwardToRegularRepository(systemStoreType)) {
      regularStoreRepository.addStore(store);
    } else {
      throw new VeniceException("System store: " + storeName + " can't be added directly");
    }
  }
}
