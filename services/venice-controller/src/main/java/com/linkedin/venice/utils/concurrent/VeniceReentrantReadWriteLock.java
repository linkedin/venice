package com.linkedin.venice.utils.concurrent;

import com.linkedin.venice.utils.ExceptionUtils;
import java.util.Collection;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * This class simply exposes publicly the protected methods of its parent, for debugging purposes.
 */
public class VeniceReentrantReadWriteLock extends ReentrantReadWriteLock {
  private static final long serialVersionUID = 1L;

  /**
   * Returns the thread that currently owns the write lock, or
   * {@code null} if not owned. When this method is called by a
   * thread that is not the owner, the return value reflects a
   * best-effort approximation of current lock status. For example,
   * the owner may be momentarily {@code null} even if there are
   * threads trying to acquire the lock but have not yet done so.
   * This method is designed to facilitate construction of
   * subclasses that provide more extensive lock monitoring
   * facilities.
   *
   * @return the owner, or {@code null} if not owned
   */
  public Thread getOwner() {
    return super.getOwner();
  }

  /**
   * Returns a collection containing threads that may be waiting to
   * acquire the write lock.  Because the actual set of threads may
   * change dynamically while constructing this result, the returned
   * collection is only a best-effort estimate.  The elements of the
   * returned collection are in no particular order.  This method is
   * designed to facilitate construction of subclasses that provide
   * more extensive lock monitoring facilities.
   *
   * @return the collection of threads
   */
  public Collection<Thread> getQueuedWriterThreads() {
    return super.getQueuedWriterThreads();
  }

  /**
   * Returns a collection containing threads that may be waiting to
   * acquire the read lock.  Because the actual set of threads may
   * change dynamically while constructing this result, the returned
   * collection is only a best-effort estimate.  The elements of the
   * returned collection are in no particular order.  This method is
   * designed to facilitate construction of subclasses that provide
   * more extensive lock monitoring facilities.
   *
   * @return the collection of threads
   */
  public Collection<Thread> getQueuedReaderThreads() {
    return super.getQueuedReaderThreads();
  }

  /**
   * Returns a collection containing threads that may be waiting to
   * acquire either the read or write lock.  Because the actual set
   * of threads may change dynamically while constructing this
   * result, the returned collection is only a best-effort estimate.
   * The elements of the returned collection are in no particular
   * order.  This method is designed to facilitate construction of
   * subclasses that provide more extensive monitoring facilities.
   *
   * @return the collection of threads
   */
  public Collection<Thread> getQueuedThreads() {
    return super.getQueuedThreads();
  }

  /**
   * Returns a collection containing those threads that may be
   * waiting on the given condition associated with the write lock.
   * Because the actual set of threads may change dynamically while
   * constructing this result, the returned collection is only a
   * best-effort estimate. The elements of the returned collection
   * are in no particular order.  This method is designed to
   * facilitate construction of subclasses that provide more
   * extensive condition monitoring facilities.
   *
   * @param condition the condition
   * @return the collection of threads
   * @throws IllegalMonitorStateException if this lock is not held
   * @throws IllegalArgumentException if the given condition is
   *         not associated with this lock
   * @throws NullPointerException if the condition is null
   */
  public Collection<Thread> getWaitingThreads(Condition condition) {
    return super.getWaitingThreads(condition);
  }

  public String toString() {
    String ownerStackTrace = ExceptionUtils.threadToThrowableToString(getOwner());
    /** Beginning with the hex hashcode, like in {@link Object#toString()}, to differentiate instances */
    return this.getClass().getSimpleName() + "@" + Integer.toHexString(hashCode()) + " {\n"
        + "getQueuedWriterThreads(): " + getQueuedWriterThreads() + ",\n\t" + "getQueuedReaderThreads(): "
        + getQueuedReaderThreads() + ",\n\t" + "getReadLockCount(): " + getReadLockCount() + ",\n\t"
        + "isWriteLocked(): " + isWriteLocked() + ",\n\t" + "isWriteLockedByCurrentThread(): "
        + isWriteLockedByCurrentThread() + ",\n\t" + "getWriteHoldCount(): " + getWriteHoldCount() + ",\n\t"
        + "getReadHoldCount(): " + getReadHoldCount() + ",\n\t" + "getQueueLength(): " + getQueueLength() + ",\n\t"
        + "getOwner(): " + getOwner() + ",\n\t" + "ownerStackTrace:\n\n" + ownerStackTrace + "\n" + "}";
  }
}
