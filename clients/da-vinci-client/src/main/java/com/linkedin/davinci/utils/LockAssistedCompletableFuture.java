package com.linkedin.davinci.utils;

import java.util.concurrent.CompletableFuture;


/*
 * A CompletableFuture that has a lock associated with it. This is useful when the caller wants to synchronize on the
 * lock object associated with the CompletableFuture. It can be extended to add more functionality by overriding the
 * methods that need to be synchronized. For now, only cancel() is synchronized.
 */
public class LockAssistedCompletableFuture<T> extends CompletableFuture<T> {
  private final Object lock;

  public LockAssistedCompletableFuture(Object lock) {
    this.lock = lock;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    synchronized (lock) {
      return super.cancel(mayInterruptIfRunning);
    }
  }

  public Object getLock() {
    return lock;
  }
}
