package com.linkedin.venice.utils.lazy;

import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * This, like {@link Lazy}, implements the same API as {@link java.util.Optional} and provides
 * late initialization for its content. Contrarily to {@link Lazy}, however, this interface
 * provides a {@link #reset()} function to return to the original, uninitialized, state, after
 * which the late initialization can occur again, on-demand. This allows the user to define a
 * member variable as final, and thus guarantee that there is only one statement which can
 * define the {@link Supplier} used in the initialization of the content, rather than declare
 * a non-final {@link Lazy} and rely on convention to always reset it to a new reference that
 * happens to carry the same {@link Supplier}.
 */
public interface LazyResettable<C> extends Lazy<C> {
  /**
   * Returns to the uninitialized state.
   */
  void reset();

  /**
   * @param supplier to initialize the wrapped value
   * @return an instance of {@link Lazy} which will execute the {@param supplier} if needed
   */
  static <C> LazyResettable<C> of(Supplier<C> supplier) {
    return new LazyResettableImpl(supplier);
  }

  /**
   * @param supplier to initialize the wrapped value
   * @param tearDown to run on thw wrapped value during reset, if it has already been initialized
   * @return an instance of {@link Lazy} which will execute the {@param supplier} if needed
   */
  static <C> LazyResettable<C> of(Supplier<C> supplier, Consumer<C> tearDown) {
    return new LazyResettableWithTearDown<>(supplier, tearDown);
  }
}
