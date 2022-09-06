package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A helper class to return the same lock for the same raw key bytes. There is an upper limit of how many locks available.
 *
 * If a lock is already assigned to a key and the lock is being used, and if another thread comes in and asks for a lock
 * for the same key, the same lock will be returned;
 * if there is no lock assigned to the requested key, pick the next available locks from the pool, or create a new lock
 * when the pool is empty.
 *
 * Current use case of this lock manager is inside Active/Active write path:
 * During Active/Active ingestion, the below data flow must be in a critical session for the same key:
 * Read existing value/RMD from transient record cache/disk -> perform DCR and decide incoming value wins
 * -> update transient record cache -> produce to VT (just call send, no need to wait for the produce future in the critical session)
 *
 * Therefore, put the above critical session in key level locking will have the minimum lock contention; to avoid creating
 * too much locks, we can build a pool of locks. Theoretically, the pool size doesn't need to exceed the number of potential
 * real-time topic partitions from different source regions --- let's assume the number of RT source regions is x, the number
 * of topic partitions are y, the Active/Active write-path could at most handle x * y different keys at the same time.
 *
 * If there are more use cases that could leverage this key level lock manager in future, feel free to do so, and extend/update
 * the class if necessary.
 */
public class KeyLevelLocksManager {
  private final String storeVersion;
  private final int initialPoolSize;
  private final int maxPoolSize;
  private final Map<ByteArrayKey, LockWithReferenceCount> keyToLockMap;
  // Free locks pool
  private final Queue<LockWithReferenceCount> locksPool;
  private int currentPoolSize;

  protected KeyLevelLocksManager(String storeVersion, int initialPoolSize, int maxPoolSize) {
    this.storeVersion = storeVersion;
    this.initialPoolSize = initialPoolSize;
    this.currentPoolSize = initialPoolSize;
    this.maxPoolSize = maxPoolSize;
    this.keyToLockMap = new VeniceConcurrentHashMap<>();
    this.locksPool = new ArrayDeque<>(initialPoolSize);
    for (int i = 0; i < initialPoolSize; i++) {
      this.locksPool.offer(LockWithReferenceCount.wrap(new ReentrantLock()));
    }
  }

  synchronized ReentrantLock acquireLockByKey(ByteArrayKey key) {
    LockWithReferenceCount lockWrapper = keyToLockMap.computeIfAbsent(key, k -> {
      LockWithReferenceCount nextAvailableLock = locksPool.poll();
      if (nextAvailableLock == null) {
        if (currentPoolSize < maxPoolSize) {
          currentPoolSize++;
          return LockWithReferenceCount.wrap(new ReentrantLock());
        } else {
          throw new VeniceException(
              "Store version: " + storeVersion + ". Key level locks pool is empty and current pool "
                  + "size is approaching the maximum pool size: " + maxPoolSize + ", which shouldn't happen. "
                  + "Initial pool size = " + initialPoolSize);
        }
      }
      return nextAvailableLock;
    });
    lockWrapper.referenceCount++;
    return lockWrapper.lock;
  }

  /**
   * If no other thread is using the lock, return the lock back to the pool, and remove the key from keyToLock map
   * so that we only keep a very small footprint, instead of caching the whole key space in memory.
   */
  synchronized void releaseLock(ByteArrayKey key) {
    LockWithReferenceCount lockWrapper = keyToLockMap.get(key);
    if (lockWrapper == null) {
      throw new VeniceException("Store version: " + storeVersion + " .Key to lock is not being maintained correctly.");
    }
    lockWrapper.referenceCount--;
    if (lockWrapper.referenceCount == 0) {
      locksPool.offer(lockWrapper);
      keyToLockMap.remove(key);
    }
  }

  // For testing only
  Queue<LockWithReferenceCount> getLocksPool() {
    return locksPool;
  }

  private static class LockWithReferenceCount {
    ReentrantLock lock;
    int referenceCount;

    private LockWithReferenceCount(ReentrantLock lock) {
      this.lock = lock;
      this.referenceCount = 0;
    }

    public static LockWithReferenceCount wrap(ReentrantLock lock) {
      return new LockWithReferenceCount(lock);
    }
  }
}
