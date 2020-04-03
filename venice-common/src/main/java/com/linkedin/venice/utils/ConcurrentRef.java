package com.linkedin.venice.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


public class ConcurrentRef<T> implements AutoCloseable {
  private final AtomicReference<ConcurrentRef> holder;
  private final AtomicReference<T> object;
  private final Consumer<T> deleter;
  private final AtomicInteger refCount = new AtomicInteger(1);

  public ConcurrentRef(Consumer<T> deleter) {
    this(null, deleter);
  }

  public ConcurrentRef(T object, Consumer<T> deleter) {
    this.holder = new AtomicReference(this);
    this.object = new AtomicReference(object);
    this.deleter = deleter;
  }

  public T get() {
    return object.get();
  }

  @Override
  public void close() {
    release();
  }

  public ConcurrentRef acquire() {
    for (;;) {
      ConcurrentRef ref = holder.get();
      ref.refCount.incrementAndGet();
      if (ref == holder.get()) {
        return ref;
      }
      ref.release();
    }
  }

  public void release() {
    if (refCount.decrementAndGet() == 0) {
      T obj = object.getAndSet(null);
      if (obj != null) {
        deleter.accept(obj);
      }
    }
  }

  public void reset(T object) {
    ConcurrentRef ref = holder.getAndSet(new ConcurrentRef(object, deleter));
    ref.release();
  }

  public void clear() {
    holder.set(new ConcurrentRef(null, deleter));
  }
}
