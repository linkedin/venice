package com.linkedin.alpini.base.queuing;

import com.linkedin.alpini.consts.QOS;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import javax.annotation.Nonnull;


/**
 * Implementation of {@link SimpleQueue}.
 * Internally, it maintains separate queues for each QOS level and responds to poll() or peek() requests from any one of the queues
 * (probabilistically).
 * Addition to this queue, causes the element to be queued together with elements of the same QOS type.
 *
 * Each sub-queue can be configured with a "percentage-slice" i.e. the fraction of poll()/peek() requests that must delegate to it.
 * For e.g. the default slices are "HIGH-80%, NORMAL-15%, LOW-5%".
 * This means that LOW QOS requests will be dequeued at most 5% of the time. Thus a large volume of LOW QOS requests cannot affect higher
 * QOS level, and it also means that higher QOS requests cannot starve lower QOS requests.
 *
 * @author aauradka
 *
 * @param <T>
 */
public class QOSBasedQueue<T extends QOSBasedRequestRunnable> extends AbstractQOSBasedQueue<T> {
  final Map<QOS, ConcurrentLinkedQueue<T>> _queue;
  /** Tracks the number of entries in the queue. If we are able to acquire the semaphore then the queue is non-empty. */
  private final Semaphore _active = new Semaphore(0);

  public QOSBasedQueue() {
    this(getDefaultQOSAllocation());
  }

  public QOSBasedQueue(Map<QOS, Integer> qosBasedAllocations) {
    super(qosBasedAllocations);

    final EnumMap<QOS, ConcurrentLinkedQueue<T>> tmpQueue = new EnumMap<>(QOS.class);
    for (QOS qos: QOS.values()) {
      ConcurrentLinkedQueue<T> baseQueue = new ConcurrentLinkedQueue<>();
      tmpQueue.put(qos, baseQueue);
    }
    _queue = Collections.unmodifiableMap(tmpQueue);
  }

  @Override
  public boolean add(@Nonnull T e) {
    QOS qos = getQOS(e);
    boolean wasAdded = _queue.get(qos).add(e);
    if (wasAdded) {
      // Release the semaphore to indicate there is something in the queue.
      _active.release();
      _log.trace("Added {} QOS element to queue {}", qos, e._queueName);
    }
    return wasAdded;
  }

  @Override
  /**
   * Returns or removes an element from one of the specified queues.
   * @param viewOrder order to check the queues in
   * @param remove if true then remove else only view
   * @return
   */
  T getElement(List<QOS> viewOrder) {
    // Check the semaphore to see if there is an item in the queue for us. If not, return null.
    if (!_active.tryAcquire()) {
      return null;
    }

    // Loop around the queue until we find an entry. We are guaranteed to find something eventually because
    // _active.tryAcquire was successful, but it may take multiple trips around the ring if multiple threads
    // are contending on the queue.
    for (;;) {
      for (QOS q: viewOrder) {
        T e = _queue.get(q).poll();
        if (e != null) {
          return e;
        }
      }
    }
  }

  @Override
  public int size() {
    return _active.availablePermits();
  }

  @Override
  public boolean isEmpty() {
    return _active.availablePermits() == 0;
  }
}
