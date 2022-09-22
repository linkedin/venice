package com.linkedin.alpini.base.misc;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A Sliding Window implementation that is implemented as followed:
 * 1. Each thread maintains a ThreadLocal queue to count certain metric by removing the data that is out of the time
 * range.
 * 2. A dedicated thread collected the counts from those ThreadLocalQueue and sum them up into a thread-safe counter
 * periodically
 *
 * Since the internal ThreadLocal instance is not a static one. Please do not create more than one
 * RetryCountSlidingWindow. And please refer to the explanation just right above the ThreadLocal instance for why
 * we made it a non-static variable.
 *
 * @author solu
 * Date: 4/26/21
 */
public class RetryCountSlidingWindow {
  private static int stalenessTorlerantSeconds = 5;
  private static final Logger LOG = LogManager.getLogger(RetryCountSlidingWindow.class);

  private int _slidingWindowLengthInSecond = 5;
  final ConcurrentMap<String, CounterQueue<RetryCounter>> _queueMap = new ConcurrentHashMap<>();

  // Normally java recommends static on ThreadLocal variable. However, static variable is hard to unit test.
  // So here we took the trade off by putting it as an instance variable to show that we are willing to take the risk
  // that if someone accidentally created two RetryCountSlidingWindow instances, we would end up with two CounterQueue
  // for a given thread if that thread calls gets on those RetryCounterSlidingWindow for more than once.
  // This is just a trade off between testability and a possible memory leak. We need devs to be careful here for not
  // creating more than on RetryCountSlidingWindow.
  // Here is a simple unit test that proves one thread can have two instances of ThreadLocal if the ThreadLocal is
  // an instance instead of a static one
  /*
  public class TestThreadLocal {
    class Blah {
      ThreadLocal<Object> _threadLocal = ThreadLocal.withInitial(Object::new);
    }
  
    @Test
    public void test() {
      Blah b1 = new Blah();
      Blah b2 = new Blah();
  
      Object o1 = b1._threadLocal.get();
      Object o2 = b2._threadLocal.get();
  
      Assert.assertEquals(o1, o2);
    }
  }
  */
  private final ThreadLocal<CounterQueue> _queues =
      ThreadLocal.withInitial(() -> new CounterQueue<>(_slidingWindowLengthInSecond, _queueMap, RetryCounter::new));

  private volatile double _retryRatio = 0.0;
  private volatile long _totalCount = 0L;
  private volatile long _retryCount = 0L;

  private volatile long _lastUpdateTsByUpdated = 0L;

  // An alternative of Google guava's checkArgument. It would be an overkill to take the whole fruit just for its
  // checkArgument.
  private void examineInput(int v, String vName) {
    if (v <= 0) {
      String msg = "Invalid " + vName + ": " + v;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  public double getRetryRatio() {
    long currentSecond = RetryCounter.getCurrentSecond();
    // if there is no thread to update the queues within last STALENESS_TORLERANT_SECONDS seconds, update the queues.
    // even if there are multiple threads calling updateCount, it is still OK as updateCount is a thread-safe method.
    if (currentSecond - _lastUpdateTsByUpdated > stalenessTorlerantSeconds) {
      updateCount();
    }
    return _retryRatio;
  }

  public long getTotalCount() {
    return _totalCount;
  }

  public long getRetryCount() {
    return _retryCount;
  }

  /**
   * Return a ThreadLocal Queue for I/O workers to keep counting retriy request count and total count.
   * @return
   */
  public CounterQueue<RetryCounter> getQueue() {
    return _queues.get();
  }

  public RetryCountSlidingWindow(
      int updateIntervalMs,
      int slidingWindowLengthInSecond,
      ScheduledExecutorService updateExecutor) {
    examineInput(slidingWindowLengthInSecond, "slidingWindowLengthInSecond");
    examineInput(updateIntervalMs, "updateIntervalMs");

    this._slidingWindowLengthInSecond = slidingWindowLengthInSecond;

    LOG.info(
        "Building a RetrySlidingWindow with updateInterval = {} ms and slidingWindowLength = {} seconds",
        updateIntervalMs,
        slidingWindowLengthInSecond);
    ScheduledExecutorService updater = Objects.requireNonNull(updateExecutor, "Null updateExecutor");
    updater.scheduleWithFixedDelay(this::updateCount, 0, updateIntervalMs, TimeUnit.MILLISECONDS);
  }

  private void updateCount() {
    _lastUpdateTsByUpdated = RetryCounter.getCurrentSecond();
    new SimpleCount().count();
  }

  private class SimpleCount {
    long total;
    long retry;

    public void count() {
      _queueMap.values().forEach(q -> {
        q.removeOldCounters();
        total += q.getTotalCount();
        retry += q.getTotalRetryCount();
      });

      _retryRatio = total > 0 ? (double) retry / (double) total : 0.0;
      _totalCount = total;
      _retryCount = retry;
    }
  }

}
