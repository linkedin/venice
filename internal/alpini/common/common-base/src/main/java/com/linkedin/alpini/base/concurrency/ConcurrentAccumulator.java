/*
 * Copied from LongAdder but modified for generic accumulation.
 */

/*
 *
 *
 *
 *
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package com.linkedin.alpini.base.concurrency;

import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collector;
import javax.annotation.Nonnull;


/**
 * One or more variables that together maintain an accumulation.
 * When updates (method {@link #accept}) are contended
 * across threads, the set of variables may grow dynamically to reduce
 * contention. Method {@link #get} returns the current total combined across the
 * variables maintaining the accumulation.
 *
 * <p>This class is usually preferable to {@link java.util.concurrent.atomic.AtomicReference} when
 * multiple threads update a common object that is used for purposes such
 * as collecting statistics, not for fine-grained synchronization
 * control.  Under low update contention, the two classes have similar
 * characteristics. But under high contention, expected throughput of
 * this class is significantly higher, at the expense of higher space
 * consumption.
 *
 * @since 1.8
 * @author Doug Lea
 *
 * @param <T> the type of input elements to the reduction operation
 * @param <A> the mutable accumulation type of the reduction operation (often
 *            hidden as an implementation detail)
 * @param <R> the result type of the reduction operation
 */
public final class ConcurrentAccumulator<T, A, R> implements Consumer<T>, Supplier<R> {
  public static Mode defaultMode = Mode.THREADED;

  private final Accumulator<T, A, R> _acc;

  public ConcurrentAccumulator(@Nonnull Collector<T, A, R> fn) {
    this(defaultMode, fn);
  }

  public ConcurrentAccumulator(@Nonnull Mode mode, @Nonnull Collector<T, A, R> fn) {
    _acc = mode.construct(fn);
  }

  /**
   * Adds the given value.
   *
   * @param x the value to add
   */
  @Override
  public void accept(T x) {
    _acc.accept(x);
  }

  /**
   * Returns the current sum.  The returned value is <em>NOT</em> an
   * atomic snapshot; invocation in the absence of concurrent
   * updates returns an accurate result, but concurrent updates that
   * occur while the sum is being calculated might not be
   * incorporated.
   *
   * @return the sum
   */
  @Override
  public R get() {
    return _acc.get();
  }

  /**
   * Resets variables maintaining the sum to zero.  This method may
   * be a useful alternative to creating a new adder, but is only
   * effective if there are no concurrent updates.  Because this
   * method is intrinsically racy, it should only be used when it is
   * known that no threads are concurrently updating.
   */
  public void reset() {
    _acc.reset();
  }

  /**
   * Equivalent in effect to {@link #get} followed by {@link
   * #reset}. This method may apply for example during quiescent
   * points between multithreaded computations.  If there are
   * updates concurrent with this method, the returned value is
   * <em>not</em> guaranteed to be the final value occurring before
   * the reset.
   *
   * @return the sum
   */
  public R getThenReset() {
    return _acc.getThenReset();
  }

  public void pack() {
    _acc.pack();
  }

  /**
   * Returns the String representation of the {@link #get}.
   * @return the String representation of the {@link #get}
   */
  public String toString() {
    return String.valueOf(get());
  }

  public enum Mode {
    COMPLEX {
      @Override
      <T, A, R> Accumulator<T, A, R> construct(Collector<T, A, R> fn) {
        return new ComplexAccumulator<>(fn);
      }
    },

    THREADED {
      @Override
      <T, A, R> Accumulator<T, A, R> construct(Collector<T, A, R> fn) {
        return new ThreadedAccumulator<>(fn);
      }
    };

    abstract <T, A, R> Accumulator<T, A, R> construct(Collector<T, A, R> fn);
  }

  interface Accumulator<T, A, R> extends Consumer<T>, Supplier<R> {
    void reset();

    R getThenReset();

    default void pack() {
      get();
    }
  }

  <A extends Accumulator<T, A, R>> A unwrap(@Nonnull Class<A> clazz) {
    return clazz.isAssignableFrom(_acc.getClass()) ? clazz.cast(_acc) : null;
  }
}
