package com.linkedin.alpini.base.concurrency;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;


public class ThreadedAccumulator<T, A, R>
    implements Consumer<T>, Supplier<R>, ConcurrentAccumulator.Accumulator<T, A, R> {
  private final Collector<T, A, R> _collector;
  private final Lock _lock;
  private final Pointer _global;

  private final ConcurrentMap<Reference<? extends Pointer>, Local> _threadMap = new ConcurrentHashMap<>();
  private final ThreadLocal<Pointer> _local = ThreadLocal.withInitial(this::newLocal);
  private final ReferenceQueue<Pointer> _queue = new ReferenceQueue<>();

  ThreadedAccumulator(Collector<T, A, R> collector) {
    _collector = Objects.requireNonNull(collector);
    _lock = new ReentrantLock(false);
    _global = newLocal();
  }

  private Pointer newLocal() {
    Pointer pointer = new Pointer();
    _threadMap.put(new PhantomReference<>(pointer, _queue), pointer._local);
    return pointer;
  }

  private Local getLocal() {
    return _local.get()._local;
  }

  final int threadMapSize() {
    return _threadMap.size();
  }

  private void drainOldLocals() {
    Reference<? extends Pointer> ref = _queue.poll();
    if (ref != null) {
      A drain = null;
      BinaryOperator<A> combiner = Objects.requireNonNull(_collector.combiner());
      do {
        Local local = _threadMap.remove(ref);

        if (local != null && local.get() == null) {
          local = null;
        }

        if (local != null && drain == null) {
          drain = local.get();
          local = null;
        }

        if (local == null && drain != null && _lock.tryLock()) {
          accumulateAndUnlock(combiner, drain);
          drain = null;
        } else if (local != null) {
          drain = combiner.apply(drain, local.get());
        }
        ref = _queue.poll();
      } while (ref != null);

      if (drain != null) {
        _lock.lock();
        accumulateAndUnlock(combiner, drain);
      }
    }
  }

  private void accumulateAndUnlock(BinaryOperator<A> combiner, A drain) {
    try {
      A accumulator = _global._local.get();
      _global._local.lazySet(accumulator != null ? combiner.apply(accumulator, drain) : drain);
    } finally {
      _lock.unlock();
    }
  }

  @Override
  public R getThenReset() {
    return get(Local::getAndReset);
  }

  @Override
  public R get() {
    return get(Local::get /*Local::getSample*/);
  }

  private R get(Function<Local, A> fn) {
    return get(fn, Objects.requireNonNull(_collector.supplier().get()), Objects.requireNonNull(_collector.combiner()));
  }

  private R get(Function<Local, A> fn, A initial, BinaryOperator<A> combiner) {
    try {
      return _collector.finisher()
          .apply(
              Stream.concat(_threadMap.values().stream().filter(this::notGlobal), Stream.of(_global._local))
                  .map(fn)
                  .filter(Objects::nonNull)
                  .reduce(initial, combiner));
    } finally {
      drainOldLocals();
    }
  }

  private boolean notGlobal(Local local) {
    return _global._local != local;
  }

  @Override
  public void reset() {
    _threadMap.values().forEach(Local::reset);
    drainOldLocals();
  }

  @Override
  public void accept(T t) {
    getLocal().accept(t);
  }

  @Override
  public void pack() {
    getLocal().pack();
  }

  private final class Pointer {
    final Local _local = new Local();
  }

  private final class Local extends AtomicReference<A> {
    private final Supplier<A> _supplier = Objects.requireNonNull(_collector.supplier());
    private final BiConsumer<A, T> _accumulator = Objects.requireNonNull(_collector.accumulator());

    void accept(T t) {
      A accumulated = get();
      if (accumulated == null) {
        if (this != _global._local && _lock.tryLock()) {
          try {
            _global._local.accept(t);
            return;
          } finally {
            _lock.unlock();
          }
        }

        accumulated = _supplier.get();
      }
      _accumulator.accept(accumulated, t);
      lazySet(accumulated);
    }

    A getSample() {
      A accumulated = get();
      if (accumulated != null && this != _global._local && _lock.tryLock()) {
        accumulated = getAndReset();
        if (accumulated != null) {
          accumulateAndUnlock(_collector.combiner(), accumulated);
          accumulated = null;
        }
      }
      return accumulated;
    }

    void reset() {
      lazySet(null);
    }

    A getAndReset() {
      return getAndSet(null);
    }

    void pack() {
      getSample();
    }
  }
}
