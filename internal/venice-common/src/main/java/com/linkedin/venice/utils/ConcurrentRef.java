package com.linkedin.venice.utils;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


public class ConcurrentRef<T> {
  private final AtomicReference<ReferenceCounted<T>> holder;
  private final Consumer<T> deleter;

  public ConcurrentRef(Consumer<T> deleter) {
    this(null, deleter);
  }

  public ConcurrentRef(T object, Consumer<T> deleter) {
    this.holder = new AtomicReference<>(new ReferenceCounted<>(object, deleter));
    this.deleter = deleter;
  }

  public ReferenceCounted<T> get() {
    for (;;) {
      ReferenceCounted<T> ref = holder.get();
      ref.retain();
      if (ref == holder.get()) {
        return ref;
      }
      ref.release();
    }
  }

  public void set(T object) {
    ReferenceCounted<T> ref = holder.getAndSet(new ReferenceCounted<>(object, deleter));
    ref.release();
  }

  public void clear() {
    holder.set(new ReferenceCounted<>(null, null));
  }
}
