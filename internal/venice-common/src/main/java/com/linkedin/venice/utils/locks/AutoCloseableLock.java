package com.linkedin.venice.utils.locks;

import java.util.concurrent.locks.Lock;


/**
 * Concrete implementations are expected to lock their inner lock(s) in their own constructor.
 */
public abstract class AutoCloseableLock implements AutoCloseable {
  public static AutoCloseableLock of(Lock lock) {
    return new AutoCloseableSingleLock(lock);
  }

  public static AutoCloseableLock ofMany(Lock... locks) {
    return new AutoCloseableMultiLock(locks);
  }

  private boolean closed = false;

  /**
   * N.B. Since concurrent calls to {@link #close()} is considered to be an error, there is no point in trying to
   *      optimize concurrent access via fine-grained locking or some lock-free construct
   *      (e.g. {@link java.util.concurrent.atomic.AtomicBoolean}). The synchronized keyword on close is expected
   *      to be very cheap in the non-contended case (which, as stated above, is expected to always be true unless
   *      we have a bug). Therefore, it is more valuable to minimize the overhead of instances of this class
   *      via a primitive {@link #closed} boolean protected by coarse synchronization than to introduce one more
   *      object to each instance of {@link AutoCloseableLock}.
   */
  @Override
  public synchronized void close() {
    if (closed) {
      throw new IllegalStateException("Duplicate unlock attempt");
    }
    closed = true;
    unlock();
  }

  /**
   * Concrete implementations should do whatever work is required to unlock their inner lock(s).
   */
  protected abstract void unlock();
}
