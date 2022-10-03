package com.linkedin.alpini.base.concurrency.impl;

import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.misc.ExceptionUtil;
import java.util.concurrent.CancellationException;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public final class CancelledAsyncFuture<T> extends FailedAsyncFuture<T> {
  @SuppressWarnings("ThrowableInstanceNeverThrown")
  private static final CancellationException EXCEPTION = ExceptionUtil.withoutStackTrace(new CancellationException());

  private static final AsyncPromise INSTANCE = new CancelledAsyncFuture();

  public CancelledAsyncFuture() {
    super(EXCEPTION);
  }

  @SuppressWarnings("unchecked")
  public static <T> AsyncPromise<T> getInstance() {
    return INSTANCE;
  }

  /**
   * Always returns true.
   *
   * @return {@code true} this task is cancelled
   */
  @Override
  public boolean isCancelled() {
    return true;
  }

}
