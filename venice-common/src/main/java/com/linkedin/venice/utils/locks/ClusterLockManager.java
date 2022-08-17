package com.linkedin.venice.utils.locks;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * A centralized place to control the locking behavior, such as lock order and lock granularity. There is 1-1 mapping
 * between ClusterLockManager instances and clusters. One instance only manages locks for one cluster.
 *
 * Each instance has a pair of cluster-level read/write locks and multiple store-level read/write locks.
 * Every store-level read/write lock will take cluster-level read lock first. Locking on the whole
 * cluster, including whole cluster write operations or controller shutdown, will take the cluster-level write lock.
 */
public class ClusterLockManager {
  private final ReentrantReadWriteLock perClusterLock;
  private final Map<String, ReentrantReadWriteLock> perStoreLockMap = new VeniceConcurrentHashMap<>();
  private final String clusterName;

  public ClusterLockManager(String clusterName) {
    perClusterLock = new ReentrantReadWriteLock();
    this.clusterName = clusterName;
  }

  public AutoCloseableLock createClusterWriteLock() {
    return AutoCloseableLock.of(perClusterLock.writeLock());
  }

  public AutoCloseableLock createClusterReadLock() {
    return AutoCloseableLock.of(perClusterLock.readLock());
  }

  public AutoCloseableLock createStoreReadLock(String storeName) {
    // Take cluster-level read lock first, then store-level read lock
    ReentrantReadWriteLock storeReadWriteLock = prepareStoreLock(storeName);
    return AutoCloseableLock.ofMany(perClusterLock.readLock(), storeReadWriteLock.readLock());
  }

  public AutoCloseableLock createStoreWriteLock(String storeName) {
    // Take cluster-level read lock first, then store-level write lock
    ReentrantReadWriteLock storeReadWriteLock = prepareStoreLock(storeName);
    return AutoCloseableLock.ofMany(perClusterLock.readLock(), storeReadWriteLock.writeLock());
  }

  public AutoCloseableLock createStoreWriteLockOnly(String storeName) {
    ReentrantReadWriteLock storeReadWriteLock = prepareStoreLock(storeName);
    return AutoCloseableLock.of(storeReadWriteLock.writeLock());
  }

  public void clear() {
    perStoreLockMap.clear();
  }

  private ReentrantReadWriteLock prepareStoreLock(String storeName) {
    // For a regular store's system store operation, acquire the regular store's lock.
    VeniceSystemStoreType systemStore = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (systemStore != null && systemStore.isStoreZkShared()) {
      String regularStoreName = systemStore.extractRegularStoreName(storeName);
      if (!regularStoreName.equals(clusterName)) {
        storeName = regularStoreName;
      }
    }

    return perStoreLockMap.computeIfAbsent(storeName, s -> new ReentrantReadWriteLock());
  }
}
