package com.linkedin.alpini.base.concurrency;

import com.linkedin.alpini.base.misc.DoublyLinkedList;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.base.registry.ShutdownableExecutors;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * More efficient handling of cancellable schedulable events. The default ScheduledExecutorService is
 * designed to handle schedulable tasks where cancellation is rare. However, for the purposes of handling
 * timeouts, we would prefer cancellation to be the common case and the tasks should be rarely run.
 *
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 *
 * @see ScheduledExecutorService
 */
public class TimeoutProcessor implements Executor {
  /** Logger for this class */
  private static final Logger LOG = LogManager.getLogger(TimeoutProcessor.class);

  private static final long TICKING_INTERVAL_MILLISECONDS = 1000;

  /** No operation Runnable */
  private static final Runnable NOP = () -> {
    // Does nothing;
  };

  /** Defunct Future */
  private static final TimeoutInterface DEFUNCT = new TimeoutInterface() {
    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public boolean cancel() {
      return false;
    }
  };

  /** ResourceRegistry iff none specified at construction */
  private final ResourceRegistry _registry;

  /** ExecutorService for this timeout processor */
  private final ScheduledExecutorService _executor;

  /** A stack containing all tasks scheduled on the ExecutorService */
  private final DoublyLinkedList<TimeoutEvent> _nextEvent = new DoublyLinkedList<>();

  // EventStore entity
  private final NavigableMap<Long, TimeoutEvent> _eventsMap;

  private final EventStore _eventStore;

  // lock to guard eventsMap
  private final ReentrantLock _mapLock = new ReentrantLock();

  // lock to guard eventsMap
  private final ReentrantLock _nextEventLock = new ReentrantLock();

  enum EventStore {
    TreeMap {
      private TimeoutEvent nextEvent(
          long now,
          NavigableMap<Long, TimeoutEvent> eventsMap,
          ReentrantLock mapLock,
          ReentrantLock nextEventLock) {
        mapLock.lock();
        try {
          Iterator<Map.Entry<Long, TimeoutEvent>> it = eventsMap.entrySet().iterator();
          if (it.hasNext()) {
            TimeoutEvent ev = it.next().getValue();
            if (ev._time <= now) {
              it.remove();
              nextEventLock.lock();
              try {
                ev.unlink();
              } finally {
                nextEventLock.unlock();
              }
              return ev;
            }
          }
        } finally {
          mapLock.unlock();
        }
        return null;
      }

      @Override
      List<TimeoutEvent> removeTimeoutEvents(
          long nano,
          NavigableMap<Long, TimeoutEvent> eventsMap,
          ReentrantLock mapLock,
          ReentrantLock nextEventLock) {
        List<TimeoutEvent> result = new LinkedList<>();
        for (TimeoutEvent ev = nextEvent(nano, eventsMap, mapLock, nextEventLock); ev != null; ev =
            nextEvent(nano, eventsMap, mapLock, nextEventLock)) {
          result.add(ev);
          ev.executeAll();
        }
        return result;
      }

      @Override
      TimeoutFuture schedule(
          Runnable task,
          TimeoutEvent event,
          Long time,
          DoublyLinkedList<TimeoutEvent> nextEvent,
          ScheduledExecutorService onDemandDrainer,
          Runnable drainingEvents,
          NavigableMap<Long, TimeoutEvent> eventsMap,
          ReentrantLock mapLock,
          ReentrantLock nextEventLock) {
        mapLock.lock();
        try {
          return super.schedule(
              task,
              event,
              time,
              nextEvent,
              onDemandDrainer,
              drainingEvents,
              eventsMap,
              nextEventLock);
        } finally {
          mapLock.unlock();
        }
      }

      @Override
      void scheduleFirstNodeInEvents(
          DoublyLinkedList<TimeoutEvent> nextEvent,
          ScheduledExecutorService onDemandDrainer,
          Runnable drainingEvents,
          NavigableMap<Long, TimeoutEvent> eventsMap,
          ReentrantLock mapLock,
          ReentrantLock nextEventLock) {
        mapLock.lock();
        try {
          super.scheduleFirstNodeInEvents(nextEvent, onDemandDrainer, drainingEvents, eventsMap, nextEventLock);
        } finally {
          mapLock.unlock();
        }
      }

      @Override
      NavigableMap<Long, TimeoutEvent> buildCorrespondentMap() {
        return new TreeMap<>();
      }
    },

    SkipList {
      @Override
      NavigableMap<Long, TimeoutEvent> buildCorrespondentMap() {
        return new ConcurrentSkipListMap<>();
      }

      @Override
      void scheduleFirstNodeInEvents(
          DoublyLinkedList<TimeoutEvent> nextEvent,
          ScheduledExecutorService onDemandDrainer,
          Runnable drainingEvents,
          NavigableMap<Long, TimeoutEvent> eventsMap,
          ReentrantLock mapLock,
          ReentrantLock nextEventLock) {
        super.scheduleFirstNodeInEvents(nextEvent, onDemandDrainer, drainingEvents, eventsMap, nextEventLock);
      }

      @Override
      TimeoutFuture schedule(
          Runnable task,
          TimeoutEvent event,
          Long time,
          DoublyLinkedList<TimeoutEvent> nextEvent,
          ScheduledExecutorService onDemandDrainer,
          Runnable drainingEvents,
          NavigableMap<Long, TimeoutEvent> eventsMap,
          ReentrantLock mapLock,
          ReentrantLock nextEventLock) {
        return super.schedule(task, event, time, nextEvent, onDemandDrainer, drainingEvents, eventsMap, nextEventLock);
      }

      @Override
      List<TimeoutEvent> removeTimeoutEvents(
          long nano,
          NavigableMap<Long, TimeoutEvent> eventsMap,
          ReentrantLock mapLock,
          ReentrantLock nextEventLock) {
        return super.removeTimeoutEvents(nano, eventsMap, nextEventLock);
      }
    };

    private List<TimeoutEvent> removeTimeoutEvents(
        long nano,
        NavigableMap<Long, TimeoutEvent> eventsMap,
        ReentrantLock nextEventLock) {
      return eventsMap.headMap(nano, true)
          .entrySet()
          .stream()
          .map(entry -> eventsMap.remove(entry.getKey()))
          .filter(Objects::nonNull)
          .map(e -> {

            nextEventLock.lock();
            try {
              e.unlink();
            } finally {
              nextEventLock.unlock();
            }
            e.executeAll();
            return e;
          })
          .collect(Collectors.toList());
    }

    private TimeoutFuture schedule(
        Runnable task,
        TimeoutEvent event,
        Long time,
        DoublyLinkedList<TimeoutEvent> nextEvent,
        ScheduledExecutorService onDemandDrainer,
        Runnable drainingEvents,
        NavigableMap<Long, TimeoutEvent> eventsMap,
        ReentrantLock nextEventLock) {

      TimeoutFuture future = new TimeoutFuture();
      eventsMap.computeIfAbsent(time, blah -> event).add(future, RunOnce.make(task));
      scheduleFirstNodeInEvents(nextEvent, onDemandDrainer, drainingEvents, eventsMap, nextEventLock);
      return future;
    }

    private void scheduleFirstNodeInEvents(
        DoublyLinkedList<TimeoutEvent> nextEvent,
        ScheduledExecutorService onDemandDrainer,
        Runnable drainingEvents,
        NavigableMap<Long, TimeoutEvent> eventsMap,
        ReentrantLock nextEventLock) {
      // We always peek the first entry;
      Optional.ofNullable(eventsMap.isEmpty() ? null : eventsMap.firstEntry()).map(Map.Entry::getValue).ifPresent(e -> {
        // TODO: Uncomment after we fix logging
        // LOG.debug("Got an event with {} futures!", e._futures.size());
        boolean act = false;
        nextEventLock.lock();
        try {
          if (nextEvent.isEmpty() || nextEvent.peek()._time > e._time) {
            nextEvent.push(e);
            act = true;
          }
        } finally {
          nextEventLock.unlock();
        }
        if (act) {
          long delay = e._time - Time.nanoTime();
          // Until debug regression is fixed, comment it out.
          // LOG.debug("Got an event and scheduling it in {} ms.", delay / 1000000);
          onDemandDrainer.schedule(drainingEvents, delay, TimeUnit.NANOSECONDS);
        }
      });
    }

    abstract NavigableMap<Long, TimeoutEvent> buildCorrespondentMap();

    abstract void scheduleFirstNodeInEvents(
        DoublyLinkedList<TimeoutEvent> nextEvent,
        ScheduledExecutorService onDemandDrainer,
        Runnable drainingEvents,
        NavigableMap<Long, TimeoutEvent> eventsMap,
        ReentrantLock mapLock,
        ReentrantLock nextEventLock);

    abstract TimeoutFuture schedule(
        Runnable task,
        TimeoutEvent event,
        Long time,
        DoublyLinkedList<TimeoutEvent> nextEvent,
        ScheduledExecutorService onDemandDrainer,
        Runnable drainingEvents,
        NavigableMap<Long, TimeoutEvent> eventsMap,
        ReentrantLock mapLock,
        ReentrantLock nextEventLock);

    abstract List<TimeoutEvent> removeTimeoutEvents(
        long nano,
        NavigableMap<Long, TimeoutEvent> eventsMap,
        ReentrantLock mapLock,
        ReentrantLock nextEventLock);
  }

  /** The task which processes expired events and schedules a next event node */
  private final Runnable _drainingEvents = () -> {
    removeTimeoutNodes();
    scheduleFirstNodeInEvents();
  };

  /** The task which processes timeout events */
  private final Runnable _tick = () -> {
    try {
      _drainingEvents.run();
    } catch (RejectedExecutionException e) {
      // nothing to log here since the executor is shutting down
      LOG.debug("RejectedExecutionException: {}", e.getMessage());
    } catch (Throwable t) {
      LOG.warn("Caught a throwable. But next _tick would continue.", t);
    }
  };

  public TimeoutProcessor() {
    this(null);
  }

  public TimeoutProcessor(ResourceRegistry registry) {
    this(registry, TICKING_INTERVAL_MILLISECONDS);
  }

  TimeoutProcessor(ResourceRegistry registry, long tickingInterval, EventStore eventStore, int executorThreadCount) {
    _registry = registry == null ? (registry = new ResourceRegistry()) : null; // SUPPRESS CHECKSTYLE InnerAssignment
    _executor = registry.factory(ShutdownableExecutors.class)
        .newScheduledThreadPool(executorThreadCount, new NamedThreadFactory("timeout-processor"));

    _eventStore = Objects.requireNonNull(eventStore);
    _eventsMap = _eventStore.buildCorrespondentMap();
    _executor.scheduleAtFixedRate(_tick, 0, tickingInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * For internal unit test only
   * @param registry
   * @param tickingInterval
   */
  TimeoutProcessor(ResourceRegistry registry, long tickingInterval) {
    this(registry, tickingInterval, EventStore.TreeMap, 1);
  }

  public TimeoutProcessor(ResourceRegistry registry, boolean useTreeMap, int executorThreadCount) {

    this(
        registry,
        TICKING_INTERVAL_MILLISECONDS,
        useTreeMap ? EventStore.TreeMap : EventStore.SkipList,
        executorThreadCount);
  }

  public TimeoutProcessor(
      ResourceRegistry registry,
      long tickingInterval,
      boolean useTreeMap,
      int executorThreadCount) {
    this(registry, tickingInterval, useTreeMap ? EventStore.TreeMap : EventStore.SkipList, executorThreadCount);
  }

  @Override
  public String toString() {
    return _eventStore.name();
  }

  private void scheduleFirstNodeInEvents() {
    _eventStore.scheduleFirstNodeInEvents(_nextEvent, _executor, _drainingEvents, _eventsMap, _mapLock, _nextEventLock);
  }

  /**
   * Obtain the next expired TimeoutEvent from the treemap.
   *
   */
  private List<TimeoutEvent> removeTimeoutNodes() {

    Long toNano = Time.nanoTime();
    List<TimeoutEvent> removedEvents = _eventStore.removeTimeoutEvents(toNano, _eventsMap, _mapLock, _nextEventLock);

    // Until debug regression is fixed, comment it out.
    // LOG.debug("After removing {} nodes, there are {} nodes", removedEvents.size(), _eventsBySkipList.size());
    return removedEvents;
  }

  /**
   * Schedule a timeout event to occur after the specified delay
   * @param task Runnable event.
   * @param delay delay time.
   * @param unit unit of time delay.
   * @return Cancellable TimeoutFuture.
   * @see ScheduledExecutorService#schedule(Runnable, long, java.util.concurrent.TimeUnit)
   */
  public TimeoutFuture schedule(Runnable task, long delay, TimeUnit unit) {
    Long time = absoluteTime(delay, unit);
    return _eventStore.schedule(
        task,
        new TimeoutEvent(time),
        time,
        _nextEvent,
        _executor,
        _drainingEvents,
        _eventsMap,
        _mapLock,
        _nextEventLock);
  }

  /**
   * @see java.util.concurrent.ScheduledExecutorService#shutdownNow()
   */
  public void shutdownNow() {
    if (_registry != null) {
      _registry.shutdown();
      try {
        _registry.waitForShutdown();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted", e);
      }
    } else {
      _executor.shutdownNow();
    }
  }

  /**
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @throws InterruptedException if interrupted while waiting
   * @see ScheduledExecutorService#awaitTermination(long, java.util.concurrent.TimeUnit)
   */
  public void awaitTermination(int timeout, TimeUnit unit) throws InterruptedException {
    if (_registry != null) {
      try {
        _registry.waitForShutdown(unit.toMillis(timeout));
      } catch (TimeoutException e) {
        LOG.warn("Timeout", e);
      }
    } else {
      _executor.awaitTermination(timeout, unit);
    }
  }

  @Override
  public void execute(Runnable command) {
    _executor.execute(command);
  }

  /**
   * Internal representation of a timeout event
   */
  private class TimeoutEvent extends DoublyLinkedList.Entry<TimeoutEvent> {
    private final Long _time;
    private boolean _purgePending;
    private final ReentrantLock _lock = new ReentrantLock();
    private final DoublyLinkedList<Future> _futures = new DoublyLinkedList<Future>() {
      @Override
      protected boolean unlink(Entry<Future> future) {
        boolean success = super.unlink(future);
        assert !success || _lock.isHeldByCurrentThread();
        if (success && isEmpty() && !_purgePending) {
          _purgePending = true;
          _executor.execute(() -> {
            try {
              if (isEmpty()) {
                _nextEventLock.lock();
                try {
                  TimeoutEvent.this.unlink();
                } finally {
                  _nextEventLock.unlock();
                }
              }
            } finally {
              _purgePending = false;
            }
          });
        }
        return success;
      }
    };

    public void executeAll() {
      for (TimeoutEvent.Future f = pop(); f != null; f = pop()) {
        f.execute();
      }
    }

    private class Future extends DoublyLinkedList.Entry<Future> implements TimeoutInterface {
      private final AtomicReference<Runnable> _task;
      private TimeoutFuture _public;

      public Future(TimeoutFuture pub, Runnable task) {
        assert pub._future == null;
        _task = new AtomicReference<>(task);
        _public = pub;
        pub._future = Future.this;
      }

      /**
       * Schedules the timeout task to be executed.
       */
      public void execute() {
        TimeoutFuture pub = _public;
        if (pub != null) {
          Runnable task = _task.getAndSet(NOP);
          if (task != NOP) {
            _executor.execute(task);
            pub._future = DEFUNCT;
            _public = null;
          }
        }
      }

      /**
       * Test to see if event is completed.
       * @return true if completed
       */
      @Override
      public boolean isDone() {
        return _task.get() == NOP;
      }

      @Override
      public boolean cancel() {
        TimeoutFuture pub = _public;
        _lock.lock();
        try {
          try {
            if (_task.getAndSet(NOP) != NOP) {
              unlink();
              return true;
            }
            return false;
          } finally {
            if (pub != null) {
              pub._future = DEFUNCT;
              _public = null;
            }
          }
        } finally {
          _lock.unlock();
        }
      }
    }

    private TimeoutEvent(Long time) {
      this._time = time;
    }

    /**
     * Add a future to this TimeoutEvent.
     * @param future to add.
     */
    public void add(TimeoutFuture future, Runnable task) {
      _lock.lock();
      try {
        _futures.add(new Future(future, task));
      } finally {
        _lock.unlock();
      }
    }

    /**
     * Removes a future from the list.
     * @return a future.
     */
    public Future pop() {
      _lock.lock();
      try {
        for (;;) {
          if (_futures.isEmpty()) {
            return null;
          }

          Future future = _futures.pop();

          if (!future.isDone()) {
            return future;
          }
        }
      } finally {
        _lock.unlock();
      }
    }
  }

  interface TimeoutInterface {
    boolean isDone();

    boolean cancel();
  }

  public static class TimeoutFuture extends DoublyLinkedList.Entry<TimeoutFuture> implements TimeoutInterface {
    private TimeoutInterface _future;

    private TimeoutFuture() {
    }

    /**
     * Test if the task has been completed.
     * @return true if the task was run or cancelled.
     */
    @Override
    public boolean isDone() {
      return _future.isDone();
    }

    /**
     * Cancels a pending scheduled runnable
     * @return true if the runnable task was not yet run.
     */
    @Override
    public boolean cancel() {
      return _future.cancel();
    }
  }

  private static long absoluteTime(long delay, TimeUnit unit) {
    // Creates buckets of approximately 1 millisecond.
    long nanosBucket = Time.nanoTime() + unit.toNanos(delay);
    return (nanosBucket + 0x7ffffL) | 0xfffffL;
  }
}
