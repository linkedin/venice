package com.linkedin.alpini.base.concurrency;

import com.linkedin.alpini.base.misc.Time;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


/**
 * A {@linkplain BlockingQueue} implementation which uses a collection of concurrent {@linkplain Queue}s per thread
 * in order to reduce the hotspot of a highly contended mutex or atomic reference.
 * @param <E> content type
 */
public class ConcurrentLinkedBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
  private final ThreadLocal<Holder> _inputQueue = ThreadLocal.withInitial(Holder::new);

  /* Set of all single-producer multiple-consumer thread queues */
  private final CopyOnWriteArraySet<Queue<E>> _allQueues = new CopyOnWriteArraySet<>();

  /** Dead queues are queues for which the creating thread has gone away */
  private final Queue<Queue<E>> _deadQueues = new ConcurrentLinkedQueue<>();

  /* Semaphore to park waiting polling threads */
  private final Semaphore _semaphore = new Semaphore(0);

  /** Sleep time in nanos between polling queue */
  /* package */ long _pollSleepNanos;

  /**
   * Default constructor with a pollSleepMillis of 10 milliseconds.
   */
  public ConcurrentLinkedBlockingQueue() {
    this(10L);
  }

  /**
   * Constructor which permits specifying the pollSleepMillis
   * @param pollSleepMillis the number of milliseconds that a poller should sleep while waiting.
   */
  public ConcurrentLinkedBlockingQueue(@Nonnegative long pollSleepMillis) {
    _pollSleepNanos = pollSleepMillis * 1_000_000L;
  }

  /** This class holds thread-local data for the queue */
  private final class Holder {
    /** Queue local to this thread */
    final Queue<E> _queue;

    /** Iterator to round-robin fairly across {@link #_allQueues} */
    Iterator<Queue<E>> _iterator;

    /** The next queue to poll from {@link #_allQueues} */
    Queue<E> _next;

    Holder() {
      _queue = newQueue();
      _iterator = Collections.emptyIterator();
      _allQueues.add(_queue);
      advance();
    }

    boolean offer(E e) {
      return _queue.offer(e);
    }

    E poll() {
      E element = _queue.poll();

      // Nothing in local queue, we need to poll from other queues
      if (element == null) {
        final Queue<E> first = _next;
        int count = _allQueues.size(); // count of how many queues there are. This is to guard against infinite loops.
        do {
          element = _next.poll();
        } while (first != advance() && count-- > 0 && element == null);

        if (element == null) {
          // Since no elements were found, we should be able to drain the (empty) dead queues.
          Queue<E> dead = _deadQueues.poll();
          if (dead != null) {
            _allQueues.remove(dead);
          }
        }
      }
      return element;
    }

    E peek() {
      E element = _queue.peek();

      // Nothing in local queue, we need to peek from other queues
      if (element == null) {
        final Queue<E> first = _next;
        int count = _allQueues.size(); // count of how many queues there are. This is to guard against infinite loops.
        do {
          element = _next.peek();
        } while (element == null && first != advance() && count-- > 0);
      }
      return element;
    }

    boolean remove(Object o) {
      return _queue.remove(o) || otherQueues().anyMatch(queue -> queue.remove(o));
    }

    boolean removeIf(Predicate<? super E> filter) {
      return _queue.removeIf(filter) | otherQueues().filter(queue -> queue.removeIf(filter)).count() > 0;
    }

    Queue<E> advance() {
      if (!_iterator.hasNext()) {
        _iterator = _allQueues.iterator();
      }
      Queue<E> next = _iterator.next();
      _next = next;
      return next;
    }

    boolean notLocal(Queue<E> queue) {
      return _queue != queue;
    }

    Stream<Queue<E>> otherQueues() {
      return _allQueues.stream().filter(this::notLocal);
    }

    @Override
    protected void finalize() throws Throwable {
      _deadQueues.add(_queue);
      super.finalize();
    }
  }

  /**
   * Create a new single-producer multiple-consumer thread-safe queue
   * @return new queue instance.
   */
  protected Queue<E> newQueue() {
    return new ConcurrentLinkedQueue<>();
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public Iterator<E> iterator() {
    if (_allQueues.isEmpty()) {
      return Collections.emptyIterator();
    }
    return new Iterator<E>() {
      // List of iterators of each queue
      final List<Iterator<E>> iterators = _allQueues.stream().map(Collection::iterator).collect(Collectors.toList());

      // Iterator of list iterator
      Iterator<Iterator<E>> it = iterators.iterator();

      // Queue iterator
      Iterator<E> next = it.next();

      private Iterator<E> advance() {
        Iterator<E> first = next;
        if (!it.hasNext()) {
          it = iterators.iterator();
        }
        next = it.next();
        return first;
      }

      @Override
      public boolean hasNext() {
        for (Iterator<E> first = next;;) {
          if (next.hasNext()) {
            return true;
          }
          advance();
          if (first == next) {
            return false;
          }
        }
      }

      @Override
      public E next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return advance().next();
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty() {
    return _allQueues.stream().allMatch(Collection::isEmpty);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size() {
    return _allQueues.stream().mapToInt(Collection::size).sum();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear() {
    _allQueues.forEach(Queue::clear);
    _deadQueues.removeIf(_allQueues::remove);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(@Nonnull E e) throws InterruptedException {
    while (!offer(e)) {
      // empty
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean offer(E e, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
    // Since this is an unbounded queue, we do not care about blocking timeouts.
    return offer(e);
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public E take() throws InterruptedException {
    return Objects.requireNonNull(poll(Long.MAX_VALUE, TimeUnit.NANOSECONDS));
  }

  /**
   * Compute the deadline relative to {@link Time#nanoTime()}
   */
  private static long deadline(long timeout, TimeUnit unit) {
    try {
      long duration = unit.toNanos(timeout);
      long startTime = Time.nanoTime();
      return Math.addExact(startTime, duration);
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    long deadline = deadline(timeout, unit);
    E element = poll();
    long remaining;
    while (element == null && (remaining = deadline - Time.nanoTime()) > 0) { // SUPPRESS CHECKSTYLE InnerAssignment
      _semaphore.tryAcquire(Math.min(remaining, _pollSleepNanos), TimeUnit.NANOSECONDS);
      element = poll();
    }
    return element;
  }

  /**
   * Returns {@linkplain Integer#MAX_VALUE} because this is an unbounded queue.
   */
  @Override
  public int remainingCapacity() {
    return Integer.MAX_VALUE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int drainTo(@Nonnull Collection<? super E> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int drainTo(@Nonnull Collection<? super E> c, final int maxElements) {
    int remaining = maxElements;
    while (remaining > 0) {
      E element = poll();
      if (element == null) {
        break;
      }
      c.add(element);
      remaining--;
    }
    return maxElements - remaining;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean offer(E e) {
    return wakeup(_inputQueue.get().offer(Objects.requireNonNull(e)));
  }

  private boolean wakeup(boolean result) {
    if (result && _semaphore.hasQueuedThreads()) {
      // Since there are waiting threads, wake one up
      _semaphore.release();
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public E poll() {
    return _inputQueue.get().poll();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public E peek() {
    return _inputQueue.get().peek();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(Object o) {
    return _inputQueue.get().remove(o);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean removeIf(Predicate<? super E> filter) {
    return _inputQueue.get().removeIf(filter);
  }
}
