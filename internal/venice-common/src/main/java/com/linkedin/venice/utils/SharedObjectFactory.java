package com.linkedin.venice.utils;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * A factory class to create shared objects that need to release resources cleanly. This class uses reference counting
 * to ensure that resources are released safely.
 * @param <T> Class whose objects need to release resources cleanly.
 */
public class SharedObjectFactory<T> {
  private final Map<String, ReferenceCounted<T>> objectMap = new VeniceConcurrentHashMap<>();

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
    return objectMap.compute(identifier, (id, referenceCounted) -> {
      if (referenceCounted == null) {
        T object = constructor.get();
        return new ReferenceCounted<>(object, destroyer);
      } else {
        referenceCounted.retain();
        return referenceCounted;
      }
    }).get();
  }

  /**
   * A method to notify to the factory that the user of the object no longer needs it. This method decreases the
   * reference count of the shared object by 1. If the reference count becomes 0, the destroyer is invoked and the
   * object is removed from the shared objects.
   * @param identifier A string that uniquely identifies the object being shared
   * @return {@code true} if the shared object is no longer being used; {@code false} otherwise
   */
  public boolean release(String identifier) {
    return objectMap.compute(identifier, (id, referenceCounted) -> {
      if (referenceCounted != null) {
        referenceCounted.release();
        if (referenceCounted.getReferenceCount() == 0) {
          return null;
        } else {
          return referenceCounted;
        }
      } else {
        return null;
      }
    }) == null;
  }

  /**
   * @return The number of shared objects that are being managed
   */
  public int size() {
    return objectMap.size();
  }

  /**
   * Return the current reference count of the object identified by the {@param identifier}. If the factory isn't
   * managing an object with the identifier, returns 0
   * @param identifier A string that uniquely identifies the object being shared
   * @return the current reference count of the object identified by the {@param identifier}. If the factory isn't
   * managing an object with the identifier, returns 0
   */
  public int getReferenceCount(String identifier) {
    // It is possible that this method is not thread-safe, but since this is only used in tests currently, it is okay
    ReferenceCounted<T> referenceCounted = objectMap.get(identifier);
    if (referenceCounted == null) {
      return 0;
    } else {
      return referenceCounted.getReferenceCount();
    }
  }
}
