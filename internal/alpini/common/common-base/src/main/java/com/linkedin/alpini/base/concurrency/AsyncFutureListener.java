package com.linkedin.alpini.base.concurrency;

/**
 * An interface (lambda) which is fired upon completion of an {@link AsyncFuture}.
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
@FunctionalInterface
public interface AsyncFutureListener<T> {
  /**
   * Invoked when the operation associated with the {@link AsyncFuture}
   * has been completed.
   *
   * @param future  the source {@link AsyncFuture} which called this
   *                callback
   */
  void operationComplete(AsyncFuture<T> future) throws Exception;
}
