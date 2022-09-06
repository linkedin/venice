package com.linkedin.venice.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;


/**
 * CachedCallable is useful when referring to expensive resources.
 * Return the same reference if it is fresh (within {@link #ttlMs}
 * or update reference if it is stale.
 */
public class CachedCallable<T> implements Callable<T> {
  private final Callable<T> inner;
  private final long ttlMs;
  private final Time time;
  private final AtomicReference<T> valueRef;

  private volatile long lastCalledMs;

  public CachedCallable(Callable<T> inner, long ttlMs) {
    this(inner, ttlMs, SystemTime.INSTANCE);
  }

  public CachedCallable(Callable<T> inner, long ttlMs, Time time) {
    this.inner = inner;
    this.ttlMs = ttlMs;
    this.time = time;
    valueRef = new AtomicReference<>();
  }

  public T call() throws Exception {
    T value = valueRef.get();
    long now = time.getMilliseconds();
    if (value == null || now - lastCalledMs > ttlMs) {
      T newValue = inner.call();
      if (valueRef.compareAndSet(value, newValue)) {
        lastCalledMs = now;
        return newValue;
      }
    }
    return valueRef.get();
  }
}
