package com.linkedin.venice.utils.locks;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class maintains a map from resource of a certain type to its lock. Its purpose is to support fine granular locking
 * @param <T> Type of the resource
 */
public class ResourceAutoClosableLockManager<T> {
  private static final Logger LOGGER = LogManager.getLogger(ResourceAutoClosableLockManager.class);

  private final ConcurrentHashMap<T, Lock> resourceToLockMap;
  private final Function<T, Lock> lockCreator; // User defines how and what kind of lock is created

  public ResourceAutoClosableLockManager(@Nonnull Supplier<Lock> lockCreator) {
    Validate.notNull(lockCreator);
    this.lockCreator = t -> lockCreator.get();
    this.resourceToLockMap = new VeniceConcurrentHashMap<>();
  }

  public AutoCloseableLock getLockForResource(@Nonnull T resource) {
    Validate.notNull(resource);
    return AutoCloseableLock.of(resourceToLockMap.computeIfAbsent(resource, lockCreator));
  }

  public void removeLockForResource(@Nonnull T resource) {
    Validate.notNull(resource);
    if (resourceToLockMap.remove(resource) == null) {
      LOGGER.warn("No lock for resource: {}", resource);
    }
  }

  public void removeAllLocks() {
    resourceToLockMap.clear();
  }
}
