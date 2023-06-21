package com.linkedin.venice.utils;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * A factory class to create shared objects that need to release resources cleanly. This class uses reference counting
 * to ensure that resources are released safely.
 * @param <T> Class whose objects need to release resources cleanly.
 */
public class SharedObjectFactory<T> {
  private final Map<String, ReferenceCounted<T>> OBJECT_MAP = new VeniceConcurrentHashMap<>();
  private final Map<String, ReentrantLock> LOCK_MAP = new VeniceConcurrentHashMap<>();

  private Lock getLock(String identifier) {
    return LOCK_MAP.computeIfAbsent(identifier, id -> new ReentrantLock());
  }

  /**
   * Get a shared object that has the specified {@param identifier}. If an object with the {@param identifier} doesn't
   * exist, a new one is created using the {@param constructor}. When this function is called, the reference count of
   * the shared object is incremented by 1.
   * @param identifier A string that uniquely identifies the object being shared
   * @param constructor A {@link Supplier> to construct a new instance of the object
   * @param destroyer A {@link Consumer} to clean-up any state when the object is no longer needed
   * @return A shared object
   */
  public T get(String identifier, Supplier<T> constructor, Consumer<T> destroyer) {
    getLock(identifier).lock();
    try {
      final AtomicBoolean newObjectCreated = new AtomicBoolean(false);
      ReferenceCounted<T> referenceCounted = OBJECT_MAP.computeIfAbsent(identifier, id -> {
        newObjectCreated.set(true);
        T object = constructor.get();
        return new ReferenceCounted<>(object, destroyer);
      });

      if (!newObjectCreated.get()) {
        referenceCounted.retain();
      }

      return referenceCounted.get();
    } finally {
      getLock(identifier).unlock();
    }
  }

  /**
   * A method to notify to the factory that the user of the object no longer needs it. This method decreases the
   * reference count of the shared object by 1. If the reference count becomes 0, the destroyer is invoked and the
   * object is removed from the shared objects.
   * @param identifier A string that uniquely identifies the object being shared
   * @return {@code true} if the shared object is no longer being used; {@code false} otherwise
   */
  public boolean release(String identifier) {
    getLock(identifier).lock();
    try {
      ReferenceCounted<T> referenceCounted = OBJECT_MAP.get(identifier);
      if (referenceCounted != null) {
        referenceCounted.release(); // Also removes the object from the objectMap
        if (referenceCounted.getReferenceCount() == 0) {
          OBJECT_MAP.remove(identifier);
          return true;
        } else {
          return false;
        }
      }
    } finally {
      getLock(identifier).unlock();
    }
    return true;
  }

  /**
   * @return The number of shared objects that are being managed
   */
  public int size() {
    return OBJECT_MAP.size();
  }

  /**
   * Return the current reference count of the object identified by the {@param identifier}. If the factory isn't
   * managing an object with the identifier, returns 0
   * @param identifier A string that uniquely identifies the object being shared
   * @return the current reference count of the object identified by the {@param identifier}. If the factory isn't
   * managing an object with the identifier, returns 0
   */
  public int getReferenceCount(String identifier) {
    getLock(identifier).lock();
    try {
      if (!OBJECT_MAP.containsKey(identifier)) {
        return 0;
      } else {
        return OBJECT_MAP.get(identifier).getReferenceCount();
      }
    } finally {
      getLock(identifier).unlock();
    }
  }
}
