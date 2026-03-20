package com.linkedin.venice.utils.lazy;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * A pre-resolved {@link Lazy} that wraps an already-computed value.
 * This avoids the overhead of creating a lambda/supplier and the synchronization
 * in {@link LazyImpl} when the value is already known at construction time.
 */
class ResolvedLazy<T> implements Lazy<T> {
  private final T value;

  ResolvedLazy(T value) {
    this.value = value;
  }

  @Override
  public T get() {
    return value;
  }

  @Override
  public void ifPresent(Consumer<? super T> consumer) {
    consumer.accept(value);
  }

  @Override
  public boolean isPresent() {
    return true;
  }

  @Override
  public Optional<T> filter(Predicate<? super T> predicate) {
    Objects.requireNonNull(predicate);
    return predicate.test(value) ? Optional.of(value) : Optional.empty();
  }

  @Override
  public <U> Optional<U> map(Function<? super T, ? extends U> mapper) {
    Objects.requireNonNull(mapper);
    return Optional.ofNullable(mapper.apply(value));
  }

  @Override
  public <U> Optional<U> flatMap(Function<? super T, Optional<U>> mapper) {
    Objects.requireNonNull(mapper);
    return Objects.requireNonNull(mapper.apply(value));
  }

  @Override
  public T orElse(T other) {
    return value;
  }

  @Override
  public T orElseGet(Supplier<? extends T> other) {
    return value;
  }

  @Override
  public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
    return value;
  }
}
