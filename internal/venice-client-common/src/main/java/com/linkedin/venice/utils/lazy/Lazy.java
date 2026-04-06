package com.linkedin.venice.utils.lazy;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * This utility provides lazy initialization for a wrapped object.
 *
 * API-wise, it can be considered similar to {@link Optional} with the main difference that
 * since it will never be {@link Optional#empty()}, the {@link #get()} function will never
 * fail. Rather, {@link #get()} will result in initializing the object if initialization has
 * not happened yet. All other functions mimic the behavior of their counterpart in
 * {@link Optional} and do NOT trigger initialization if it has not happened yet.
 *
 * Note that {@link Lazy} does allow wrapping a null if that is what the lazy initialization
 * returns. For the use case of lazy initializing to something which may or may not exist,
 * the implementer should consider combining this class with {@link Optional}, thus
 * resulting in a {@link Lazy<Optional<T>>}.
 *
 * It is recommended to use instances of {@link Lazy} exclusively in conjunction with the
 * final keyword, such that it is not allowed to swap the reference to a new {@link Lazy}
 * instance. Following the recommended usage allows making the assumption that there will
 * be no more than one initialization, and that the transition from uninitialized to
 * initialized is one-way and irreversible.
 *
 * This interface promises to be threadsafe if multiple concurrent calls happen before the
 * first initialization finishes, and furthermore guarantees that the initialization
 * routine will happen at most once.
 */
public interface Lazy<T> {
  Lazy<Boolean> FALSE = Lazy.of(() -> false);
  Lazy<Boolean> TRUE = Lazy.of(() -> true);

  /**
   * @param supplier to initialize the wrapped value
   * @return an instance of {@link Lazy} which will execute the {@param supplier} if needed
   */
  static <C> Lazy<C> of(Supplier<C> supplier) {
    return new LazyImpl<>(supplier);
  }

  /**
   * @return the wrapped value, with the side-effect of initializing it has not happened yet
   */
  T get();

  /**
   * @param consumer to pass the wrapped value to, only if it has already been initialized
   */
  void ifPresent(Consumer<? super T> consumer);

  /**
   * @return {@code true} if there is a value present (i.e. initialized), otherwise {@code false}
   */
  boolean isPresent();

  /**
   * If a value is initialized, and the value matches the given predicate,
   * return an {@code Optional} describing the value, otherwise return an
   * empty {@code Optional}.
   *
   * @param predicate a predicate to apply to the value, if initialized
   * @return an {@code Optional} describing the value of this {@code Optional}
   * if a value is present and the value matches the given predicate,
   * otherwise an empty {@code Optional}
   * @throws NullPointerException if the predicate is null
   */
  Optional<T> filter(Predicate<? super T> predicate);

  /**
   * If a value is initialized, apply the provided mapping function to it,
   * and if the result is non-null, return an {@code Optional} describing the
   * result.  Otherwise return an empty {@code Optional}.
   *
   * @apiNote This method supports post-processing on optional values, without
   * the need to explicitly check for a return status.  For example, the
   * following code traverses a stream of file names, selects one that has
   * not yet been processed, and then opens that file, returning an
   * {@code Optional<FileInputStream>}:
   *
   * <pre>{@code
   *     Optional<FileInputStream> fis =
   *         names.stream().filter(name -> !isProcessedYet(name))
   *                       .findFirst()
   *                       .map(name -> new FileInputStream(name));
   * }</pre>
   *
   * Here, {@code findFirst} returns an {@code Optional<String>}, and then
   * {@code map} returns an {@code Optional<FileInputStream>} for the desired
   * file if one exists.
   *
   * @param <U> The type of the result of the mapping function
   * @param mapper a mapping function to apply to the value, if initialized
   * @return an {@code Optional} describing the result of applying a mapping
   * function to the value of this {@code Optional}, if a value is initialized,
   * otherwise an empty {@code Optional}
   * @throws NullPointerException if the mapping function is null
   */
  <U> Optional<U> map(Function<? super T, ? extends U> mapper);

  /**
   * If a value is initialized, apply the provided {@code Optional}-bearing
   * mapping function to it, return that result, otherwise return an empty
   * {@code Optional}.  This method is similar to {@link #map(Function)},
   * but the provided mapper is one whose result is already an {@code Optional},
   * and if invoked, {@code flatMap} does not wrap it with an additional
   * {@code Optional}.
   *
   * @param <U> The type parameter to the {@code Optional} returned by
   *           the mapping function
   * @param mapper a mapping function to apply to the value, if initialized
   * @return the result of applying an {@code Optional}-bearing mapping
   * function to the value of this {@code Optional}, if a value is initialized,
   * otherwise an empty {@code Optional}
   * @throws NullPointerException if the mapping function is null or returns
   * a null result
   */
  <U> Optional<U> flatMap(Function<? super T, Optional<U>> mapper);

  /**
   * Return the value if initialized, otherwise return {@code other}.
   *
   * @param other the value to be returned if there is no value initialized, may
   * be null
   * @return the value, if initialized, otherwise {@code other}
   */
  T orElse(T other);

  /**
   * Return the value if initialized, otherwise invoke {@code other} and return
   * the result of that invocation.
   *
   * @param other a {@code Supplier} whose result is returned if no value
   * is initialized
   * @return the value if initialized otherwise the result of {@code other.get()}
   * @throws NullPointerException if value is not initialized and {@code other} is
   * null
   */
  T orElseGet(Supplier<? extends T> other);

  /**
   * Return the contained value, if initialized, otherwise throw an exception
   * to be created by the provided supplier.
   *
   * @apiNote A method reference to the exception constructor with an empty
   * argument list can be used as the supplier. For example,
   * {@code IllegalStateException::new}
   *
   * @param <X> Type of the exception to be thrown
   * @param exceptionSupplier The supplier which will return the exception to
   * be thrown
   * @return the initialized value
   * @throws X if there is no value initialized
   * @throws NullPointerException if no value is initialized and
   * {@code exceptionSupplier} is null
   */
  <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X;
}
