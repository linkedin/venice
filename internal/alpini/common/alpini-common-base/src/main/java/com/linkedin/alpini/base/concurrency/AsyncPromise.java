package com.linkedin.alpini.base.concurrency;

import javax.annotation.Nonnull;


/**
 * A completable {@link AsyncFuture} interface.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface AsyncPromise<T> extends AsyncFuture<T> {
  /**
   * Adds the specified listener to this future.  The
   * specified listener is notified when this future is
   * {@linkplain #isDone() done}.  If this future is already
   * completed, the specified listener is notified immediately.
   * @return {@code this} to permit chaining of operations.
   */
  @Nonnull
  AsyncPromise<T> addListener(@Nonnull AsyncFutureListener<T> listener);

  /**
   * Adds the specified future as a listener to this future.  The
   * specified future is notified when this future is
   * {@linkplain #isDone() done}.  If this future is already
   * completed, the specified future is notified immediately.
   * @return {@code this} to permit chaining of operations.
   * @param listener
   */
  @Nonnull
  AsyncPromise<T> addListener(@Nonnull AsyncPromise<T> listener);

  /**
   * Marks this future as a success and notifies all
   * listeners.
   *
   * @return {@code true} if and only if successfully marked this future as
   *         a success. Otherwise {@code false} because this future is
   *         already marked as either a success or a failure.
   */
  boolean setSuccess(T result);

  /**
   * Marks this future as a failure and notifies all
   * listeners.
   *
   * @return {@code true} if and only if successfully marked this future as
   *         a failure. Otherwise {@code false} because this future is
   *         already marked as either a success or a failure.
   */
  boolean setFailure(@Nonnull Throwable cause);

  default void setComplete(T value, Throwable ex) {
    if (ex != null) {
      setFailure(ex);
    } else {
      setSuccess(value);
    }
  }
}
