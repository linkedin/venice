package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SystemStore;
import java.util.List;


/**
 * This repository provides an read only interface to access both system store and regular venice store.
 */
public class HelixReadOnlyStoreRepositoryAdapter implements ReadOnlyStoreRepository {
  private final HelixReadOnlyZKSharedSystemStoreRepository systemStoreRepository;
  private final ReadOnlyStoreRepository regularStoreRepository;

  public HelixReadOnlyStoreRepositoryAdapter(HelixReadOnlyZKSharedSystemStoreRepository systemStoreRepository,
      ReadOnlyStoreRepository regularStoreRepository) {
    this.systemStoreRepository = systemStoreRepository;
    this.regularStoreRepository = regularStoreRepository;
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
    return null == systemStoreType || !systemStoreType.isNewMedataRepositoryAdopted();
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
    if (null == regularStore) {
      return null;
    }
    // Get ZK shared system store name
    String zkSharedStoreName = systemStoreType.getZkSharedStoreName();
    Store zkSharedStore = systemStoreRepository.getStore(zkSharedStoreName);
    if (null == zkSharedStore) {
      return null;
    }

    return new SystemStore(zkSharedStore, systemStoreType, regularStore);
  }

  @Override
  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    Store store = getStore(storeName);
    if (null == store) {
      throw new VeniceNoStoreException(storeName);
    }
    return store;
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

  @Override
  public List<Store> getAllStores() {
    // So far, it only return the regular store list
    return regularStoreRepository.getAllStores();
  }

  @Override
  public long getTotalStoreReadQuota() {
    return regularStoreRepository.getTotalStoreReadQuota();
  }

  @Override
  public void registerStoreDataChangedListener(StoreDataChangedListener listener) {
    regularStoreRepository.registerStoreDataChangedListener(listener);
  }

  @Override
  public void unregisterStoreDataChangedListener(StoreDataChangedListener listener) {
    regularStoreRepository.unregisterStoreDataChangedListener(listener);
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
}
