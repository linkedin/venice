package com.linkedin.alpini.base.queuing;

import com.linkedin.alpini.consts.QOS;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;


/**
 * A fair multi-queue based upon a ring topology.
 * For each queue name, a separate queue exists and is inserted into the ring.
 * Reading items from this queue involves examining each queue in order around the ring.
 *
 * The ring only grows in size but that is okay since the number of possible queues is
 * a finite low number.
 *
 * @author acurtis
 */
public class QOSBasedMultiQueue<T extends QOSBasedRequestRunnable> extends AbstractQOSBasedQueue<T> {
  /** Map which translate a {@code queueName} to its entry in the ring */
  private final ConcurrentHashMap<String, RingEntry> _queues;
  /** Tail of the ring. {@code _ringTail->next} is the first entry in the ring */
  private final AtomicReference<RingEntry> _ringTail;
  /** Semaphore keeps track of the number of entries added to this queue */
  private final Semaphore _active;
  /** Constants determining the maximum number of entries permitted per {@code queueName} */
  private final int _maxPerQueue;
  private final int _hardMaxPerQueue;

  public QOSBasedMultiQueue() {
    this(0, getDefaultQOSAllocation());
  }

  public QOSBasedMultiQueue(int maxPerQueue, Map<QOS, Integer> qosBasedAllocations) {
    this(maxPerQueue, (int) Math.min((long) (maxPerQueue * 1.10), Integer.MAX_VALUE), qosBasedAllocations);
  }

  public QOSBasedMultiQueue(int maxPerQueue, int hardMaxPerQueue, Map<QOS, Integer> qosBasedAllocations) {
    super(qosBasedAllocations);
    _queues = new ConcurrentHashMap<>();
    // By creating a Ring entry, we don't have to handle the special case where there is no entries in the ring.
    RingEntry init = new RingEntry("");
    _ringTail = new AtomicReference<>(init);
    _queues.put(init._name, init);
    _maxPerQueue = maxPerQueue < 1 ? Integer.MAX_VALUE : maxPerQueue;
    _hardMaxPerQueue = Math.max(_maxPerQueue, hardMaxPerQueue);
    _active = new Semaphore(0);
  }

  /**
   * Returns a queue limit for a specified {@link QOS}.
   * @param qos QOS.
   * @return limit.
   */
  protected int getMaxPerQueue(QOS qos) {
    return qos != QOS.HIGH ? _maxPerQueue : _hardMaxPerQueue;
  }

  /**
   * Creates a QOS based queue for a ring element. One of these is used per named queue.
   * By default, this instantiates an instance of {@link QOSBasedQueue}.
   * @return {@link AbstractQOSBasedQueue} instance.
   */
  protected AbstractQOSBasedQueue<T> newQOSBasedQueue() {
    return new QOSBasedQueue<>();
  }

  /**
   * Returns the ring entry for {@code queueName} or creates a new ring entry as required.
   * @param queueName name of ring entry
   * @return ring entry.
   */
  protected RingEntry getRingEntry(String queueName) {
    RingEntry entry = _queues.get(queueName);
    if (entry == null) {
      RingEntry newEntry = new RingEntry(queueName);
      entry = _queues.putIfAbsent(queueName, newEntry);
      if (entry == null) {
        entry = newEntry;

        // Insert the new ring element into the ring.
        RingEntry tail;
        do {
          tail = _ringTail.get();
          entry._next = tail._next;
        } while (!_ringTail.compareAndSet(tail, entry));
        tail._next = entry;
      }
    }
    return entry;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean add(@Nonnull T e) {
    String queueName = getQueueName(e);
    QOS qos = getQOS(e);
    RingEntry entry = getRingEntry(queueName);

    int queueSize = entry._queue.size();
    int maxPerQueue = getMaxPerQueue(qos);
    if (queueSize >= maxPerQueue) {
      _log.trace(
          "Unable to add {} QOS element to queue {}" + " : queueSize {} >= {}",
          qos,
          e._queueName,
          queueSize,
          maxPerQueue);
      return false;
    }

    if (entry._queue.add(e)) {
      _active.release();
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size() {
    return _active.availablePermits();
  }

  @Override
  /**
   * {@inheritDoc}
   */
  T getElement(List<QOS> viewOrder) {
    RingEntry tail;
    RingEntry pos;

    if (!_active.tryAcquire()) {
      return null;
    }

    // Get the next ring entry and advance the ring position.
    do {
      tail = _ringTail.get();
      pos = tail._next;
    } while (!_ringTail.compareAndSet(tail, pos));

    final RingEntry first = pos;
    for (;; pos = pos._next) {
      T elem = pos._queue.getElement(viewOrder);

      if (elem != null) {
        // if the ring tail hasn't moved, then set it to our current position
        if (first != pos) {
          _ringTail.compareAndSet(first, pos);
        }
        return elem;
      }
    }
  }

  @Override
  public boolean isEmpty() {
    return _active.availablePermits() == 0;
  }

  /**
   * An element of the ring, encapsulates a QOSBasedQueue.
   */
  private final class RingEntry {
    final String _name;
    final AbstractQOSBasedQueue<T> _queue;
    volatile RingEntry _next;

    private RingEntry(String name) {
      _name = name;
      _queue = newQOSBasedQueue();
      _next = this;
    }
  }
}
