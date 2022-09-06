package com.linkedin.venice.utils.lazy;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * Fork of the {@link org.apache.commons.lang3.concurrent.LazyInitializer} with some additional APIs.
 */
public class LazyImpl<T> implements Lazy<T> {
  private static final Object NO_INIT = new Object();
  private Supplier<T> supplier;
  @SuppressWarnings("unchecked")
  /** Stores the managed object. */
  private volatile T value = (T) NO_INIT;

  /** Package-private constructor. Use {@link Lazy#of(Supplier)} instead. */
  LazyImpl(Supplier<T> supplier) {
    if (supplier == null) {
      throw new NullPointerException("supplier cannot be null");
    }
    this.supplier = supplier;
  }

  /**
   * Returns the object wrapped by this instance. On first access the object
   * is created. After that it is cached and can be accessed pretty fast.
   *
   * @return the object initialized by this {@code LazyInitializer}
   * the object
   */
  @Override
  public T get() {
    // use a temporary variable to reduce the number of reads of the
    // volatile field
    T result = value;

    if (result == NO_INIT) {
      synchronized (this) {
        result = value;
        if (result == NO_INIT) {
          value = result = supplier.get();

          // No need to hang on to the supplier after the first use, so it can be GC-ed.
          supplier = null;
        }
      }
    }

    return result;
  }

  public void ifPresent(Consumer<? super T> consumer) {
    if (isPresent()) {
      consumer.accept(value);
    }
  }

  @Override
  public boolean isPresent() {
    return value != NO_INIT;
  }

  @Override
  public Optional<T> filter(Predicate<? super T> predicate) {
    Objects.requireNonNull(predicate);
    if (!isPresent())
      return Optional.empty();
    else
      return predicate.test(value) ? Optional.of(value) : Optional.empty();
  }

  @Override
  public <U> Optional<U> map(Function<? super T, ? extends U> mapper) {
    Objects.requireNonNull(mapper);
    if (!isPresent())
      return Optional.empty();
    else {
      return Optional.ofNullable(mapper.apply(value));
    }
  }

  @Override
  public <U> Optional<U> flatMap(Function<? super T, Optional<U>> mapper) {
    Objects.requireNonNull(mapper);
    if (!isPresent())
      return Optional.empty();
    else {
      return Objects.requireNonNull(mapper.apply(value));
    }
  }

  @Override
  public T orElse(T other) {
    return isPresent() ? value : other;
  }

  @Override
  public T orElseGet(Supplier<? extends T> other) {
    return isPresent() ? value : other.get();
  }

  @Override
  public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
    if (isPresent()) {
      return value;
    } else {
      throw exceptionSupplier.get();
    }
  }
}
