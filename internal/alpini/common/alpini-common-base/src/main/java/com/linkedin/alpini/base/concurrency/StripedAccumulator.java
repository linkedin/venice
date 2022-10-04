package com.linkedin.alpini.base.concurrency;

/*
 * Copied from Striped64 but modified for generic object.
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collector;
import javax.annotation.Nonnull;


public class StripedAccumulator<A> {
  /*
   * This class maintains a lazily-initialized table of atomically
   * updated variables, plus an extra "base" field. The table size
   * is a power of two. Indexing uses masked per-thread hash codes.
   * Nearly all declarations in this class are package-private,
   * accessed directly by subclasses.
   *
   * Table entries are of class Cell; a variant of AtomicLong padded
   * (via @sun.misc.Contended) to reduce cache contention. Padding
   * is overkill for most Atomics because they are usually
   * irregularly scattered in memory and thus don't interfere much
   * with each other. But Atomic objects residing in arrays will
   * tend to be placed adjacent to each other, and so will most
   * often share cache lines (with a huge negative performance
   * impact) without this precaution.
   *
   * In part because Cells are relatively large, we avoid creating
   * them until they are needed.  When there is no contention, all
   * updates are made to the base field.  Upon first contention (a
   * failed CAS on base update), the table is initialized to size 2.
   * The table size is doubled upon further contention until
   * reaching the nearest power of two greater than or equal to the
   * number of CPUS. Table slots remain empty (null) until they are
   * needed.
   *
   * A single spinlock ("cellsBusy") is used for initializing and
   * resizing the table, as well as populating slots with new Cells.
   * There is no need for a blocking lock; when the lock is not
   * available, threads try other slots (or the base).  During these
   * retries, there is increased contention and reduced locality,
   * which is still better than alternatives.
   *
   * The Thread probe fields maintained via ThreadLocalRandom serve
   * as per-thread hash codes. We let them remain uninitialized as
   * zero (if they come in this way) until they contend at slot
   * 0. They are then initialized to values that typically do not
   * often conflict with others.  Contention and/or table collisions
   * are indicated by failed CASes when performing an update
   * operation. Upon a collision, if the table size is less than
   * the capacity, it is doubled in size unless some other thread
   * holds the lock. If a hashed slot is empty, and lock is
   * available, a new Cell is created. Otherwise, if the slot
   * exists, a CAS is tried.  Retries proceed by "double hashing",
   * using a secondary hash (Marsaglia XorShift) to try to find a
   * free slot.
   *
   * The table size is capped because, when there are more threads
   * than CPUs, supposing that each thread were bound to a CPU,
   * there would exist a perfect hash function mapping threads to
   * slots that eliminates collisions. When we reach capacity, we
   * search for this mapping by randomly varying the hash codes of
   * colliding threads.  Because search is random, and collisions
   * only become known via CAS failures, convergence can be slow,
   * and because threads are typically not bound to CPUS forever,
   * may not occur at all. However, despite these limitations,
   * observed contention rates are typically low in these cases.
   *
   * It is possible for a Cell to become unused when threads that
   * once hashed to it terminate, as well as in the case where
   * doubling the table causes no thread to hash to it under
   * expanded mask.  We do not try to detect or remove such cells,
   * under the assumption that for long-running instances, observed
   * contention levels will recur, so the cells will eventually be
   * needed again; and for short-lived ones, it does not matter.
   */
  /**
   * Padded variant of AtomicLong supporting only raw accesses plus CAS.
   *
   * JVM intrinsics note: It would be possible to use a release-only
   * form of CAS here, if it were provided.
   */
  static final class Cell<A> {
    /* The following exists because we don't have access to @jdk.internal.vm.annotation.Contended
       or @sun.misc.Contended to prevent false cache sharing */
    long _a1;
    long _a2;
    long _a3;
    long _a4;
    long _a5;
    long _a6;
    long _a7;
    long _a8;
    long _a9;
    long _a10;
    long _a11;
    long _a12;
    long _a13;
    long _a14;
    long _a15;
    long _a16;

    volatile A value;

    Cell(A x) {
      value = x;
    }

    final boolean cas(A cmp, A val) {
      return VALUE_UPDATER.compareAndSet(this, cmp, val);
    }

    private static final AtomicReferenceFieldUpdater<Cell, Object> VALUE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(Cell.class, Object.class, "value");
  }

  /** Number of CPUS, to place bound on table size */
  static final int NCPU = Runtime.getRuntime().availableProcessors();

  /**
   * Table of cells. When non-null, size is a power of 2.
   */
  transient volatile Cell<A>[] cells;

  /**
   * Base value, used mainly when there is no contention, but also as
   * a fallback during table initialization races. Updated via CAS.
   */
  transient volatile A base;

  /**
   * Spinlock (locked via CAS) used when resizing and/or creating Cells.
   */
  transient volatile int cellsBusy;

  /**
   * Package-private default constructor
   */
  StripedAccumulator() {
  }

  private static final AtomicReferenceFieldUpdater<StripedAccumulator, Object> BASE_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(StripedAccumulator.class, Object.class, "base");

  private static final AtomicIntegerFieldUpdater<StripedAccumulator> CELLS_BUSY_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(StripedAccumulator.class, "cellsBusy");

  /**
   * CASes the base field.
   */
  final boolean casBase(A cmp, A val) {
    return BASE_UPDATER.compareAndSet(this, cmp, val);
  }

  /**
   * CASes the cellsBusy field from 0 to 1 to acquire lock.
   */
  final boolean casCellsBusy() {
    return CELLS_BUSY_UPDATER.compareAndSet(this, 0, 1);
  }

  static final ThreadInfo threadInfo() {
    return INFO_THREAD_LOCAL.get();
  }

  /**
   * Returns the probe value for the current thread.
   * Duplicated from ThreadLocalRandom because of packaging restrictions.
   */
  static final int getProbe(ThreadInfo threadInfo) {
    return threadInfo.threadLocalRandomProbe;
    // return Unsafe.$.get().getInt(Thread.currentThread(), PROBE);
  }

  /**
   * Pseudo-randomly advances and records the given probe value for the
   * given thread.
   * Duplicated from ThreadLocalRandom because of packaging restrictions.
   */
  static final int advanceProbe(ThreadInfo threadInfo, int probe) {
    probe ^= probe << 13; // xorshift
    probe ^= probe >>> 17;
    probe ^= probe << 5;
    threadInfo.threadLocalRandomProbe = probe;
    // Unsafe.$.get().putInt(Thread.currentThread(), PROBE, probe);
    return probe;
  }

  @SuppressWarnings("unchecked")
  private static <A> Cell<A>[] newCellArray(int n) {
    return new Cell[n];
  }

  private static <T, A> Cell<A> newCell(Collector<T, A, ?> fn, T x) {
    A aValue = fn.supplier().get();
    fn.accumulator().accept(aValue, x);
    return new Cell<>(aValue);
  }

  private static <T, A> A apply(
      Supplier<A> supplier,
      BiConsumer<A, T> accumulator,
      BinaryOperator<A> combiner,
      A a,
      T x) {
    A result = supplier.get();
    if (a != null) {
      result = combiner.apply(result, a);
    }
    accumulator.accept(result, x);
    return result;
  }

  /**
   * Same as longAccumulate, but injecting long/double conversions
   * in too many places to sensibly merge with long version, given
   * the low-overhead requirements of this class. So must instead be
   * maintained by copy/paste/adapt.
   */
  final <T, R> void accumulate(T x, @Nonnull Collector<T, A, R> fn, boolean wasUncontended) {
    int h;
    ThreadInfo threadInfo = threadInfo();
    if ((h = getProbe(threadInfo)) == 0) { // SUPPRESS CHECKSTYLE InnerAssignment
      threadInfo.init(); // force initialization
      h = getProbe(threadInfo);
      wasUncontended = true;
    }
    Supplier<A> supplier = fn.supplier();
    BiConsumer<A, T> accumulator = fn.accumulator();
    BinaryOperator<A> combiner = fn.combiner();
    boolean collide = false; // True if last slot nonempty
    for (;;) {
      Cell<A>[] as;
      Cell<A> a;
      int n;
      A v;
      if ((as = cells) != null && (n = as.length) > 0) { // SUPPRESS CHECKSTYLE InnerAssignment
        if ((a = as[(n - 1) & h]) == null) { // SUPPRESS CHECKSTYLE InnerAssignment
          if (cellsBusy == 0) { // Try to attach new Cell
            Cell<A> r = newCell(fn, x);
            if (cellsBusy == 0 && casCellsBusy()) {
              boolean created = false;
              try { // Recheck under lock
                Cell<A>[] rs;
                int m;
                int j;
                if ((rs = cells) != null // SUPPRESS CHECKSTYLE InnerAssignment
                    && (m = rs.length) > 0 // SUPPRESS CHECKSTYLE InnerAssignment
                    && rs[j = (m - 1) & h] == null) { // SUPPRESS CHECKSTYLE InnerAssignment
                  rs[j] = r;
                  created = true;
                }
              } finally {
                cellsBusy = 0;
              }
              if (created) {
                break;
              }
              continue; // Slot is now non-empty
            }
          }
          collide = false;
        } else if (!wasUncontended) { // CAS already known to fail
          wasUncontended = true; // Continue after rehash
        } else if (a.cas(
            v = a.value, // SUPPRESS CHECKSTYLE InnerAssignment
            apply(supplier, accumulator, combiner, v, x))) {
          break;
        } else if (n >= NCPU || cells != as) {
          collide = false; // At max size or stale
        } else if (!collide) {
          collide = true;
        } else if (cellsBusy == 0 && casCellsBusy()) {
          try {
            if (cells == as) { // Expand table unless stale
              Cell<A>[] rs = newCellArray(n << 1);
              for (int i = 0; i < n; ++i) {
                rs[i] = as[i];
              }
              cells = rs;
            }
          } finally {
            cellsBusy = 0;
          }
          collide = false;
          continue; // Retry with expanded table
        }
        h = advanceProbe(threadInfo, h);
      } else if (cellsBusy == 0 && cells == as && casCellsBusy()) {
        boolean init = false;
        try { // Initialize table
          if (cells == as) {
            Cell<A>[] rs = newCellArray(2);
            rs[h & 1] = newCell(fn, x);
            cells = rs;
            init = true;
          }
        } finally {
          cellsBusy = 0;
        }
        if (init) {
          break;
        }
      } else if (casBase(v = base, apply(supplier, accumulator, combiner, v, x))) { // SUPPRESS CHECKSTYLE
                                                                                    // InnerAssignment
        break; // Fall back on using base
      }
    }
  }

  private static final ThreadLocal<ThreadInfo> INFO_THREAD_LOCAL = ThreadLocal.withInitial(ThreadInfo::new);

  private static class ThreadInfo {
    int threadLocalRandomProbe;

    /** Generates per-thread initialization/probe field */
    private static final AtomicInteger PROBE_GENERATOR = new AtomicInteger();

    /**
     * The increment for generating probe values
     */
    private static final int PROBE_INCREMENT = 0x9e3779b9;

    private void init() {
      int p = PROBE_GENERATOR.addAndGet(PROBE_INCREMENT);
      threadLocalRandomProbe = (p == 0) ? 1 : p; // skip 0
    }
  }
}
