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

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
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
public final class ComplexAccumulator<T, A, R> extends StripedAccumulator<A>
    implements Consumer<T>, Supplier<R>, ConcurrentAccumulator.Accumulator<T, A, R> {
  private final Collector<T, A, R> fn;

  public ComplexAccumulator(@Nonnull Collector<T, A, R> fn) {
    this.fn = fn;
  }

  private static <A, T> A add(
      Supplier<A> supplier,
      BiConsumer<A, T> accumulator,
      BinaryOperator<A> combiner,
      A a,
      T x) {
    A aValue = supplier.get();
    accumulator.accept(aValue, x);
    return a != null ? combiner.apply(a, aValue) : aValue;
  }

  private static <A> A collect(Supplier<A> supplier, BinaryOperator<A> combiner, A a, A x) {
    if (a == null && x == null) {
      return null;
    }
    if (a == null) {
      a = supplier.get();
    }
    if (x != null) {
      a = combiner.apply(a, x);
    }
    return a;
  }

  /**
   * Adds the given value.
   *
   * @param x the value to add
   */
  @Override
  public void accept(T x) {
    Cell<A>[] as;
    A b;
    A v;
    int m;
    Cell<A> a;
    Supplier<A> supplier = fn.supplier();
    BiConsumer<A, T> accumulator = fn.accumulator();
    BinaryOperator<A> combiner = fn.combiner();
    if ((as = cells) != null || !casBase(b = base, add(supplier, accumulator, combiner, b, x))) { // SUPPRESS CHECKSTYLE
                                                                                                  // InnerAssignment
      boolean uncontended = true;
      if (as == null || (m = as.length - 1) < 0 // SUPPRESS CHECKSTYLE InnerAssignment
          || (a = as[getProbe(threadInfo()) & m]) == null // SUPPRESS CHECKSTYLE InnerAssignment
          || !(uncontended = a.cas(v = a.value, add(supplier, accumulator, combiner, v, x)))) { // SUPPRESS CHECKSTYLE
                                                                                                // InnerAssignment
        accumulate(x, fn, uncontended);
      }
    }
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
    Cell<A>[] as = cells;
    Cell<A> a;
    A sum = base;
    if (as != null) {
      if (sum == null) {
        sum = fn.supplier().get();
      }
      Supplier<A> supplier = fn.supplier();
      BinaryOperator<A> combiner = fn.combiner();
      for (int i = 0; i < as.length; ++i) {
        if ((a = as[i]) != null) { // SUPPRESS CHECKSTYLE InnerAssignment
          sum = collect(supplier, combiner, sum, a.value);
        }
      }
    }
    return sum != null ? fn.finisher().apply(sum) : null;
  }

  /**
   * Resets variables maintaining the sum to zero.  This method may
   * be a useful alternative to creating a new adder, but is only
   * effective if there are no concurrent updates.  Because this
   * method is intrinsically racy, it should only be used when it is
   * known that no threads are concurrently updating.
   */
  public void reset() {
    Cell<A>[] as = cells;
    Cell<A> a;
    base = null;
    if (as != null) {
      for (int i = 0; i < as.length; ++i) {
        if ((a = as[i]) != null) { // SUPPRESS CHECKSTYLE InnerAssignment
          a.value = null;
        }
      }
    }
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
    Cell<A>[] as = cells;
    Cell<A> a;
    A sum = base;
    base = null;
    if (as != null) {
      Supplier<A> supplier = fn.supplier();
      if (sum == null) {
        sum = supplier.get();
      }
      BinaryOperator<A> combiner = fn.combiner();
      for (int i = 0; i < as.length; ++i) {
        if ((a = as[i]) != null) { // SUPPRESS CHECKSTYLE InnerAssignment
          sum = collect(supplier, combiner, sum, a.value);
          a.value = null;
        }
      }
    }
    return sum != null ? fn.finisher().apply(sum) : null;
  }

  /**
   * Returns the String representation of the {@link #get}.
   * @return the String representation of the {@link #get}
   */
  public String toString() {
    return String.valueOf(get());
  }
}
