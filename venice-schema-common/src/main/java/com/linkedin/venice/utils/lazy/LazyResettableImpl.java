package com.linkedin.venice.utils.lazy;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;


public class LazyResettableImpl<C> implements LazyResettable<C> {
  private final Supplier<C> supplier;
  protected Lazy<C> lazy;

  public LazyResettableImpl(Supplier<C> supplier) {
    this.supplier = supplier;
    this.lazy = Lazy.of(supplier);
  }

  @Override
  public void reset() {
    this.lazy = Lazy.of(supplier);
  }

  @Override
  public C get() {
    return lazy.get();
  }

  @Override
  public void ifPresent(Consumer<? super C> consumer) {
    lazy.ifPresent(consumer);
  }

  @Override
  public boolean isPresent() {
    return lazy.isPresent();
  }

  @Override
  public Optional<C> filter(Predicate<? super C> predicate) {
    return lazy.filter(predicate);
  }

  @Override
  public <U> Optional<U> map(Function<? super C, ? extends U> mapper) {
    return lazy.map(mapper);
  }

  @Override
  public <U> Optional<U> flatMap(Function<? super C, Optional<U>> mapper) {
    return lazy.flatMap(mapper);
  }

  @Override
  public C orElse(C other) {
    return lazy.orElse(other);
  }

  @Override
  public C orElseGet(Supplier<? extends C> other) {
    return lazy.orElseGet(other);
  }

  @Override
  public <X extends Throwable> C orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
    return lazy.orElseThrow(exceptionSupplier);
  }
}
