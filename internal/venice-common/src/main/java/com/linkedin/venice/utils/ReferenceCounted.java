package com.linkedin.venice.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


public class ReferenceCounted<T> implements AutoCloseable {
  private final T object;
  private final AtomicInteger refCount;
  private final AtomicReference<Consumer<T>> deleterRef;

  public ReferenceCounted(T object, Consumer<T> deleter) {
    this.object = object;
    this.refCount = new AtomicInteger(1);
    this.deleterRef = new AtomicReference<>(deleter);
  }

  public T get() {
    return object;
  }

  @Override
  public void close() {
    release();
  }

  public void retain() {
    int count = refCount.incrementAndGet();
    if (count <= 0) {
      throw new ArithmeticException("Reference counter overflow, refCount=" + count);
    }
  }

  public void release() {
    int count = refCount.decrementAndGet();
    if (count < 0) {
      throw new ArithmeticException("Reference counter underflow, refCount=" + count);
    }
    if (count == 0) {
      Consumer<T> deleter = deleterRef.getAndSet(null);
      if (deleter != null && object != null) {
        deleter.accept(object);
      }
    }
  }

  public int getReferenceCount() {
    return refCount.get();
  }
}
