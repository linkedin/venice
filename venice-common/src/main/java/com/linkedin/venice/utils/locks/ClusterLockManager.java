package com.linkedin.venice.utils.locks;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * A centralized place to control the locking behavior, such as lock order and lock granularity. There is 1-1 mapping
 * between ClusterLockManager instances and clusters. One instance only manages locks for one cluster.
 *
 * Each instance has a pair of cluster-level read/write locks and multiple store-level read/write locks.
 * Every store-level read/write lock will take cluster-level read lock first. Locking on the whole
 * cluster, including whole cluster read/write or controller shutdown, will take the cluster-level write lock.
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
    return new AutoCloseableLock(Collections.singletonList(perClusterLock.writeLock()));
  }

  public AutoCloseableLock createStoreReadLock(String storeName) {
    // Take cluster-level read lock first, then store-level read lock
    ReentrantReadWriteLock storeReadWriteLock = prepareStoreLock(storeName);
    return new AutoCloseableLock(Arrays.asList(perClusterLock.readLock(), storeReadWriteLock.readLock()));
  }

  public AutoCloseableLock createStoreWriteLock(String storeName) {
    // Take cluster-level read lock first, then store-level write lock
    ReentrantReadWriteLock storeReadWriteLock = prepareStoreLock(storeName);
    return new AutoCloseableLock(Arrays.asList(perClusterLock.readLock(), storeReadWriteLock.writeLock()));
  }

  public void clear() {
      perStoreLockMap.clear();
  }

  private ReentrantReadWriteLock prepareStoreLock(String storeName) {
    // For a regular store's system store operation, acquire the regular store's lock.
    VeniceSystemStoreType systemStore = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (systemStore != null) {
      String regularStoreName = systemStore.extractRegularStoreName(storeName);
      if (!regularStoreName.equals(clusterName)) {
        storeName = regularStoreName;
      }
    }

    return perStoreLockMap.computeIfAbsent(storeName, s -> new ReentrantReadWriteLock());
  }
}
