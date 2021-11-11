package com.linkedin.venice.utils.locks;

import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import org.apache.log4j.Logger;


/**
 * This class maintains a map from resource of a certain type to its lock. Its purpose is to support fine granular locking
 * @param <T> Type of the resource
 */
public class ResourceAutoClosableLockManager<T> {
  private static final Logger logger = Logger.getLogger(ResourceAutoClosableLockManager.class);

  private final ConcurrentHashMap<T, Lock> resourceToLockMap;
  private final Supplier<Lock> lockCreator; // User defines how and what kind of lock is created

  public ResourceAutoClosableLockManager(Supplier<Lock> lockCreator) {
    this.lockCreator = Utils.notNull(lockCreator);
    this.resourceToLockMap = new VeniceConcurrentHashMap<>();
  }

  public AutoCloseableLock getLockForResource(T resource) {
    return new AutoCloseableLock(resourceToLockMap.computeIfAbsent(Utils.notNull(resource), t -> lockCreator.get()));
  }

  public void removeLockForResource(T resource) {
    if (resourceToLockMap.remove(Utils.notNull(resource)) == null) {
      logger.warn("No lock for resource: " + resource);
    }
  }

  public void removeAllLocks() {
    resourceToLockMap.clear();
  }
}
