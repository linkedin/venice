package com.linkedin.alpini.base.misc;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A queue implemented with a circular buffer of AtomicReferences
 * Example of using:
 * 1. Initializing a SlidingWindow: RetryCountSlidingWindow w = new RetryCountSlidingWindow(500, _windowLen, scheduler);
 * 2. Inside a thread, usually an io worker to get a queue: CounterQueue<RetryCounter> q = w.getQueue();
 * 3. increase the count: q.increaseCount(isRetry);
 *
 * For more examples, please refer to the class of TestRetryCountSlidingWindow
 *
 * @author solu
 * Date: 4/26/21
  */
public class CounterQueue<T extends RetryCounter> {
  private static final Logger LOG = LogManager.getLogger(CounterQueue.class);

  private final LongAdder _totalCount = new LongAdder();
  private final LongAdder _totalRetryCount = new LongAdder();

  public long getTotalCount() {
    return _totalCount.longValue();
  }

  public long getTotalRetryCount() {
    return _totalRetryCount.longValue();
  }

  private final Supplier<T> _retryCounterSupplier;

  private final AtomicReferenceArray<T> _queue;
  // the _index would be accessed by more than one thread. Although volatile would not work with ++, only increaseCount
  // would change its value. For other thread such as oldCounter cleaning thread, even if it sees an outdated value, it
  // won't be a big deal because in method removeOldCounters:
  // 1. we use another variable to read from the index: i = _index;
  // 2. we use mod again on i to make sure the value is right: i %= _queueLength;
  private volatile int _index = 0;

  /**
   * Non-thread safe method but it would only be called by a single thread. So the increment of _index does not have
   * to be protected by AtomicReference. A volatile suffices.
   * @param isRetry
   */
  public void increaseCount(boolean isRetry) {

    // getting an absolute timestamp ticking from 01/01/1970 in second.
    long currentSecond = RetryCounter.getCurrentSecond();
    // finding the right slot
    // Usually the _index would point to the latest timeslot. However, it may be way behind the current second if the
    // thread has not gotten any chance to either serve a request or get to be scheduled to run.
    T rc = _queue.get(_index);
    int queueLength = _queue.length();
    while (rc != null && rc.getTimestamp() < currentSecond) {
      if (currentSecond - rc.getTimestamp() > queueLength) {
        removeFromTotal(_index, currentSecond, rc);
      }

      _index = (_index + 1) % queueLength;
      rc = _queue.get(_index);
    }

    // if we are out here it is either we got the right Counter or we need to initialize a new one
    // in case there is a recurring thread that may be resetting the retryCounter,
    // we need to re-examine the condition.
    while (rc == null) {
      rc = _retryCounterSupplier.get();
      if (!_queue.compareAndSet(_index, null, rc)) {
        rc = _queue.get(_index);
      }
    }
    rc.increment(isRetry);

    if (isRetry) {
      _totalRetryCount.increment();
    }
    _totalCount.increment();
  }

  private void removeFromTotal(int i, long currentSecond, T rc) {
    // if some one already changed the value to null, meaning that the removal from total has already been done.
    // So we don't need to do anything.
    boolean iWouldbeTheOneToRemoveIt = _queue.compareAndSet(i, rc, null);
    if (iWouldbeTheOneToRemoveIt) {
      LOG.debug(
          "Removing counter {} because current - its timestamp is {} > {}",
          rc,
          currentSecond - rc.getTimestamp(),
          _queue.length());
      _totalCount.add(-rc.getTotalCount());
      _totalRetryCount.add(-rc.getRetryCount());
    }
  }

  void removeOldCounters() {
    long currentSecond = RetryCounter.getCurrentSecond();
    // starting from currentIndex + 1. This is a circular buffer. So the most possible outdated counter is
    // right after the currentIndex.
    int queueLength = _queue.length();
    for (int i = _index + 1; i < _index + queueLength; i++) {
      int actualIndex = i % queueLength;
      T rc = _queue.get(actualIndex);
      if (rc != null && currentSecond - rc.getTimestamp() > queueLength) {
        removeFromTotal(actualIndex, currentSecond, rc);
      } else {
        // we can safely break as we removed all the outdated counters.
        // but even if we dont' break here, it is not big deal.
        break;
      }
    }
  }

  /**
   * Initialize the queue and register itself to the queue map.
   * @param queueLength
   * @param retryCounterSupplier
   */
  public CounterQueue(int queueLength, Map<String, CounterQueue<T>> queueMap, Supplier<T> retryCounterSupplier) {
    _queue = new AtomicReferenceArray<>(queueLength);
    queueMap.put(Thread.currentThread().getName(), this);
    this._retryCounterSupplier = Objects.requireNonNull(retryCounterSupplier, "Null retryCounterSupplier");
  }

}
