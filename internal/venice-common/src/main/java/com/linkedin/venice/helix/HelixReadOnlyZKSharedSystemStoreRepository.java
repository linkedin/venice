package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * This repository provides an interface to access zk shared system stores only.
 * With this implementation, we will minimize the zwatches required to the system store cluster.
 * But there is a shortcoming with current implementation, since all the system store related operations
 * will be constrained by the healthiness of system store cluster.
 */
public class HelixReadOnlyZKSharedSystemStoreRepository extends HelixReadOnlyStoreRepository {
  /**
   * This set is used to keep all the zk shared stores the current repo will monitor.
   */
  private final Set<String> zkSharedSystemStoreSet = new HashSet<>();

  public HelixReadOnlyZKSharedSystemStoreRepository(
      ZkClient zkClient,
      HelixAdapterSerializer compositeSerializer,
      String systemStoreClusterName) {
    super(zkClient, compositeSerializer, systemStoreClusterName);
    // Initialize the necessary zk shared system stores
    for (VeniceSystemStoreType type: VeniceSystemStoreType.values()) {
      if (type.isNewMedataRepositoryAdopted()) {
        zkSharedSystemStoreSet.add(type.getZkSharedStoreName());
      }
    }
  }

  /**
   * Only the zk shared system store can be returned, otherwise this function will return null.
   */
  @Override
  public Store getStore(String storeName) {
    if (zkSharedSystemStoreSet.contains(storeName)) {
      return super.getStore(storeName);
    }
    return null;
  }

  /**
   * Only the zk shared system store can be returned, otherwise this function will throw {@link VeniceNoStoreException}.
   */
  @Override
  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    if (zkSharedSystemStoreSet.contains(storeName)) {
      return super.getStoreOrThrow(storeName);
    }
    throw new VeniceNoStoreException(storeName, clusterName);
  }

  /**
   * Only return true for the existing zk shared system store.
   */
  @Override
  public boolean hasStore(String storeName) {
    if (zkSharedSystemStoreSet.contains(storeName)) {
      return super.hasStore(storeName);
    }
    return false;
  }

  /**
   * Only zk shared system store can be refreshed here, otherwise {@link VeniceException} will be thrown.
   */
  @Override
  public Store refreshOneStore(String storeName) {
    if (zkSharedSystemStoreSet.contains(storeName)) {
      return super.refreshOneStore(storeName);
    }
    throw new VeniceException("Only system store is expected, but got: " + storeName);
  }

  /**
   * This function is used to filter out non system stores from the system store cluster, and
   * this is very important since {@link CachedReadOnlyStoreRepository#refresh()} is relying on
   * this function to retrieve all the stores in this cluster and put a watch against each znode,
   * and we need to filter out non-system stores to avoid unnecessary zk watches.
   * @return
   */
  @Override
  protected List<Store> getStoresFromZk() {
    return super.getStoresFromZk().stream()
        .filter(s -> zkSharedSystemStoreSet.contains(s.getName()))
        .collect(Collectors.toList());
  }

  /**
   * This function is used to filter out non system stores to avoid unnecessary zk watches against
   * the newly added regular stores.
   * @param newZkStoreNames
   */
  @Override
  protected void onRepositoryChanged(Collection<String> newZkStoreNames) {
    // Only monitor system stores
    List<String> systemStores =
        newZkStoreNames.stream().filter(s -> zkSharedSystemStoreSet.contains(s)).collect(Collectors.toList());
    super.onRepositoryChanged(systemStores);
  }

  @Override
  public long getTotalStoreReadQuota() {
    throw new VeniceException("Unsupported operation: 'getTotalStoreReadQuota'");
  }

  /**
   * Only return zk shared system store related info, otherwise {@link VeniceException} will be thrown.
   */
  @Override
  public int getBatchGetLimit(String storeName) {
    if (zkSharedSystemStoreSet.contains(storeName)) {
      return super.getBatchGetLimit(storeName);
    }
    throw new VeniceException("Only system store is expected, but got: " + storeName);
  }

  /**
   * Only return zk shared system store related info, otherwise {@link VeniceException} will be thrown.
   */
  @Override
  public boolean isReadComputationEnabled(String storeName) {
    if (zkSharedSystemStoreSet.contains(storeName)) {
      return super.isReadComputationEnabled(storeName);
    }
    throw new VeniceException("Only system store is expected, but got: " + storeName);
  }

  @Override
  public void refresh() {
    super.refresh();
  }

  @Override
  public void clear() {
    super.clear();
  }
}
