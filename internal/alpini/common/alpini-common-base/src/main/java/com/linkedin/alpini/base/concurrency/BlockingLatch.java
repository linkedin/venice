package com.linkedin.alpini.base.concurrency;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Created by acurtis on 10/16/17.
 */
public class BlockingLatch {
  private final AtomicReference<AsyncPromise<Void>> _promise = new AtomicReference<>(AsyncFuture.success(null));

  public final void await() throws InterruptedException {
    _promise.get().await();
  }

  public final void awaitUninterruptibly() {
    _promise.get().awaitUninterruptibly();
  }

  public final void await(long time, TimeUnit unit) throws InterruptedException {
    _promise.get().await(time, unit);
  }

  public final void awaitUninterruptibly(long time, TimeUnit unit) {
    _promise.get().awaitUninterruptibly(time, unit);
  }

  public void unblock() {
    _promise.get().setSuccess(null);
  }

  public void block() {
    AsyncPromise<Void> promise = _promise.get();
    if (promise.isSuccess()) {
      _promise.compareAndSet(promise, AsyncFuture.deferred(false));
    }
  }

  public final boolean isBlocking() {
    return !_promise.get().isSuccess();
  }

  public void setBlock(boolean enabled) {
    if (enabled) {
      block();
    } else {
      unblock();
    }
  }
}
