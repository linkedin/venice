package com.linkedin.venice.utils.pools;

/**
 * An interface to get and give back objects that are intended to be long-lived and recycled, but where the
 * location in the code for beginning to use the object is far from the location where we stop using it, e.g.
 * across different threads.
 *
 * If the reuse is localized, then consider whether it may be simpler to leverage a {@link ThreadLocal} or
 * a {@link com.linkedin.venice.utils.concurrent.CloseableThreadLocal}.
 *
 * The intent of having an interface is to experiment with different pooling strategies.
 */
public interface ObjectPool<O> {
  /**
   * @return an object, potentially from the pool, or potentially instantiating a new one if necessary.
   */
  O get();

  /**
   * Return an object to the pool, indicating that it will no longer be used and can be recycled.
   *
   * @param object which is no longer used.
   */
  void dispose(O object);
}
