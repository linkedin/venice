package com.linkedin.alpini.base.concurrency;

import java.util.Objects;
import java.util.function.Supplier;


/**
 * Created by acurtis on 3/29/17.
 */
public final class Lazy<T> implements Supplier<T> {
  public static <T> Supplier<T> of(Supplier<T> supplier) {
    return new Lazy<>(Objects.requireNonNull(supplier));
  }

  private Supplier<T> _supplier;

  private Lazy(Supplier<T> supplier) {
    _supplier = new Supplier<T>() {
      @Override
      public T get() {
        synchronized (this) {
          if (this == _supplier) {
            _supplier = resolve(supplier.get());
          }
        }
        return _supplier.get();
      }
    };
  }

  private static <T> Supplier<T> resolve(T value) {
    return () -> value;
  }

  public T get() {
    return _supplier.get();
  }
}
