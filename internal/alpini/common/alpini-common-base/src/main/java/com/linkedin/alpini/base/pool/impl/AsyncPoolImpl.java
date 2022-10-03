package com.linkedin.alpini.base.pool.impl;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.base.pool.AsyncPool;
import com.linkedin.alpini.base.pool.PoolStats;
import com.linkedin.alpini.base.registry.ShutdownableResource;
import com.linkedin.alpini.base.statistics.LongStats;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class AsyncPoolImpl<T> implements AsyncPool<T>, ShutdownableResource {
  private static final Logger LOG = LogManager.getLogger(AsyncPoolImpl.class);

  private final LifeCycle<T> _lifeCycle;
  private final ConcurrentLinkedQueue<IdleEntity> _idleQueue;
  private final ConcurrentLinkedQueue<CompletableFuture<T>> _waiters;
  private final IdentityHashMap<T, CallCompletion> _checkedOut;
  private final AtomicInteger _totalEntities;
  private final AtomicInteger _totalWaiters;
  private final CallTracker _waitersCallTracker;
  private final CallTracker _checkedOutCallTracker;
  private final int _minimumEntities;
  private final int _maximumEntities;
  private final long _maxIdleTime;
  private final int _maxWaiters;
  private final Executor _executor;
  private final Semaphore _concurrentCreate;
  private CompletableFuture<Boolean> _shutdown;
  private CompletableFuture<Void> _shutdownCompleted;
  private ShutdownException _shutdownException;

  public AsyncPoolImpl(
      LifeCycle<T> lifeCycle,
      Executor executor,
      int maxConcurrentCreate,
      int maxWaiters,
      int minimumEntities,
      int maximumEntities,
      long maxIdleTime,
      TimeUnit maxIdleUnit) {
    _lifeCycle = Objects.requireNonNull(lifeCycle, "lifeCycle");
    _executor = Objects.requireNonNull(executor, "executor");
    checkLowerBound(maxConcurrentCreate, 1, "maxConcurrentCreate");
    _maxWaiters = checkLowerBound(maxWaiters, 1, "maxWaiters");
    _maxIdleTime = Objects.requireNonNull(maxIdleUnit, "maxIdleUnit").toNanos(maxIdleTime);
    if (_maxIdleTime < 1) {
      throw new IllegalArgumentException("maxIdleTime");
    }
    _minimumEntities = checkLowerBound(minimumEntities, 0, "minimumEntries");
    _maximumEntities = checkLowerBound(maximumEntities, minimumEntities, "maximumEntries");
    _idleQueue = new ConcurrentLinkedQueue<>();
    _waiters = new ConcurrentLinkedQueue<>();
    _checkedOut = new IdentityHashMap<>();
    _totalEntities = new AtomicInteger();
    _totalWaiters = new AtomicInteger();
    _waitersCallTracker = createCallTracker();
    _checkedOutCallTracker = createCallTracker();
    _concurrentCreate = new Semaphore(maxConcurrentCreate);
  }

  protected CallTracker createCallTracker() {
    return CallTracker.create();
  }

  private static int checkLowerBound(int value, int bound, String message) {
    if (value < bound) {
      throw new IllegalArgumentException(message);
    }
    return value;
  }

  @Override
  public void start() {
    if (_shutdown == null && _totalEntities.get() < _minimumEntities && _concurrentCreate.tryAcquire()) {
      _totalEntities.incrementAndGet();
      create();
    }
  }

  protected final CallCompletion startWaiters() {
    return _waitersCallTracker.startCall();
  }

  protected CompletableFuture<T> checkout(@Nonnull CompletableFuture<T> future, @Nonnull CallCompletion waiter) {
    return future.whenComplete((entity, ex) -> {
      if (entity != null) {
        CallCompletion checkout = _checkedOutCallTracker.startCall();
        CallCompletion cc;
        synchronized (_checkedOut) {
          cc = _checkedOut.put(entity, checkout);
        }
        if (cc != null) {
          LOG.warn("Entry already checked out {}", entity);
          cc.closeWithError();
        }
      }
      waiter.closeCompletion(entity, ex);
    });
  }

  @Override
  public CompletableFuture<T> acquire() {
    return checkout(acquire0(), startWaiters());
  }

  protected final CompletableFuture<T> acquire0() {
    long now = Time.currentTimeMillis();
    IdleEntity idle;
    while ((idle = _idleQueue.poll()) != null) {
      if (idle._idleNanos + _maxIdleTime < now) {
        T entity = idle.get();
        CompletableFuture.supplyAsync(idle, _executor)
            .thenCompose(_lifeCycle::testAfterIdle)
            .thenAccept(success -> afterTest(success, entity));
      } else {
        return CompletableFuture.completedFuture(idle.get());
      }
    }
    CompletableFuture<T> future = new CompletableFuture<>();
    if (_shutdown != null) {
      synchronized (this) {
        future.obtrudeException(_shutdownException);
        return future;
      }
    }
    if (_waiters.add(future)) {
      int totalWaiters = _totalWaiters.incrementAndGet();

      outer: while (!future.isDone() && (idle = _idleQueue.poll()) != null) { // SUPPRESS CHECKSTYLE InnerAssignment
        if (idle._idleNanos + _maxIdleTime < now) {
          T entity = idle.get();
          CompletableFuture.supplyAsync(idle, _executor)
              .thenCompose(_lifeCycle::testAfterIdle)
              .thenAccept(success -> afterTest(success, entity));
        } else {
          CompletableFuture<T> waiter;
          while ((waiter = _waiters.poll()) != future) {
            _totalWaiters.getAndDecrement();
            if (waiter.complete(idle.get())) {
              continue outer;
            }
          }
          future.obtrudeValue(idle.get());
          return future;
        }
      }
      if (_idleQueue.isEmpty() && _totalEntities.get() < _maximumEntities && _concurrentCreate.tryAcquire()) {
        _totalEntities.incrementAndGet();
        create();
      }

      if (totalWaiters > _maxWaiters) {
        CompletableFuture<T> waiter;
        int rejected = 0;
        TooManyWaitersException rejectedException = new TooManyWaitersException();
        while (totalWaiters + rejected > _maximumEntities && (waiter = _waiters.poll()) != null) { // SUPPRESS
                                                                                                   // CHECKSTYLE
                                                                                                   // InnerAssignment
          rejected--;
          waiter.completeExceptionally(rejectedException);
        }
        _totalWaiters.addAndGet(rejected);
      }
    } else {
      future.obtrudeException(new IllegalStateException());
    }
    return future;
  }

  private void afterTest(boolean success, T entity) {
    if (success && _shutdown == null) {
      CompletableFuture<T> future;
      while ((future = _waiters.poll()) != null) {
        _totalWaiters.getAndDecrement();
        if (future.complete(entity)) {
          return;
        }
      }
      _idleQueue.add(new IdleEntity(entity));
    } else {
      dispose0(entity);
    }
  }

  @Override
  public void release(T entity) {
    synchronized (_checkedOut) {
      CallCompletion cc = _checkedOut.remove(entity);
      if (cc == null) {
        throw new IllegalStateException("Cannot release object which was not checked out");
      }
      cc.close();
    }
    release0(entity);
  }

  protected final void release0(T entity) {
    _lifeCycle.testOnRelease(Objects.requireNonNull(entity)).exceptionally(ex -> {
      LOG.warn("error in lifecycle testOnRelease", ex);
      return false;
    }).thenAccept(success -> afterTest(success, entity));
  }

  @Override
  public void dispose(T entity) {
    synchronized (_checkedOut) {
      CallCompletion cc = _checkedOut.remove(entity);
      if (cc == null) {
        throw new IllegalStateException("Cannot dispose object which was not checked out");
      }
      cc.closeWithError();
    }
    release0(entity);
  }

  protected final void dispose0(T entity) {
    _lifeCycle.destroy(Objects.requireNonNull(entity)).exceptionally(ex -> {
      LOG.warn("error in lifecycle destroy", ex);
      return null;
    }).thenRun(() -> {
      if (_shutdown == null && _minimumEntities > _totalEntities.get() - 1 && _concurrentCreate.tryAcquire()) {
        create();
        return;
      }
      int total = _totalEntities.decrementAndGet();
      if (_shutdown != null && total == 0) {
        _shutdown.complete(Boolean.TRUE);
      }
    });
  }

  private void create() {
    if (_shutdown != null) {
      _concurrentCreate.release();
      if (_totalEntities.decrementAndGet() == 0) {
        _shutdown.complete(Boolean.TRUE);
      }
    }
    _lifeCycle.create().whenComplete((newEntity, ex) -> {
      _concurrentCreate.release();
      if (ex != null) {
        LOG.warn("error in lifecycle create", ex);
      }
    }).thenAccept(newEntity -> {
      if (newEntity != null) {
        afterTest(true, newEntity);
      } else {
        int total = _totalEntities.decrementAndGet();
        if (_shutdown != null && total == 0) {
          _shutdown.complete(Boolean.TRUE);
        }
      }
    }).thenRun(this::start);
  }

  @Override
  public int size() {
    return _totalEntities.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isShutdown() {
    return _shutdown != null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isTerminated() {
    return isShutdown() && _shutdown.isDone();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void shutdown() {
    if (_shutdown == null) {
      _shutdownException = new ShutdownException();
      _shutdown = new CompletableFuture<>();
      _shutdownCompleted = _shutdown.thenCompose(ignore -> _lifeCycle.shutdown());

      CompletableFuture<T> future;
      while ((future = _waiters.poll()) != null) {
        _totalWaiters.getAndDecrement();
        future.completeExceptionally(_shutdownException);
      }

      IdleEntity idle;
      while ((idle = _idleQueue.poll()) != null) {
        dispose0(idle.get());
      }

      if (_totalEntities.get() == 0) {
        _shutdown.complete(Boolean.TRUE);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> shutdownPool() {
    shutdown();
    return _shutdownCompleted.thenApply(Function.identity());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void waitForShutdown() throws InterruptedException, IllegalStateException {
    CompletableFuture<Void> shutdown = _shutdownCompleted;
    if (shutdown == null) {
      throw new IllegalStateException();
    } else {
      try {
        shutdown.get();
      } catch (ExecutionException e) {
        throw new InterruptedException(e.getCause().getMessage());
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void waitForShutdown(long timeoutInMs) throws InterruptedException, IllegalStateException, TimeoutException {
    CompletableFuture<Void> shutdown = _shutdownCompleted;
    if (shutdown == null) {
      throw new IllegalStateException();
    } else {
      try {
        shutdown.get(timeoutInMs, TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        throw new InterruptedException(e.getCause().getMessage());
      }
    }
  }

  @Override
  public PoolStats getPoolStats() {
    CallTracker.CallStats createStats;
    CallTracker.CallStats testStats;
    CallTracker.CallStats destroyStats;

    if (_lifeCycle.isWrapperFor(LifeCycleStatsCollector.class)) {
      LifeCycleStatsCollector statsCollector = _lifeCycle.unwrap(LifeCycleStatsCollector.class);
      createStats = statsCollector.getCreateCallStats();
      testStats = statsCollector.getTestCallStats();
      destroyStats = statsCollector.getDestroyCallStats();
    } else {
      createStats = NullStats.getInstance();
      testStats = NullStats.getInstance();
      destroyStats = NullStats.getInstance();
    }

    CallTracker.CallStats waitStats = _waitersCallTracker.getCallStats();
    CallTracker.CallStats checkedOut = _checkedOutCallTracker.getCallStats();

    return new PoolStatsImpl(
        _maximumEntities,
        _minimumEntities,
        _totalEntities.get(),
        _idleQueue.size(),
        createStats,
        testStats,
        destroyStats,
        waitStats,
        checkedOut);
  }

  private static class PoolStatsImpl implements PoolStats {
    private final int _maxPoolSize;
    private final int _minPoolSize;
    private final int _poolSize;
    private final int _idleCount;
    private final CallTracker.CallStats _createStats;
    private final CallTracker.CallStats _testStats;
    private final CallTracker.CallStats _destroyStats;
    private final CallTracker.CallStats _waitStats;
    private final CallTracker.CallStats _checkedOut;

    private PoolStatsImpl(
        int maxPoolSize,
        int minPoolSize,
        int poolSize,
        int idleCount,
        CallTracker.CallStats createStats,
        CallTracker.CallStats testStats,
        CallTracker.CallStats destroyStats,
        CallTracker.CallStats waitStats,
        CallTracker.CallStats checkedOut) {
      _maxPoolSize = maxPoolSize;
      _minPoolSize = minPoolSize;
      _poolSize = poolSize;
      _idleCount = idleCount;
      _createStats = createStats;
      _testStats = testStats;
      _destroyStats = destroyStats;
      _waitStats = waitStats;
      _checkedOut = checkedOut;
    }

    @Override
    public long getTotalCreated() {
      return _createStats.getCallCountTotal() - _createStats.getErrorCountTotal();
    }

    @Override
    public long getTotalDestroyed() {
      return _destroyStats.getCallCountTotal() - _destroyStats.getErrorCountTotal();
    }

    @Override
    public long getTotalCreateErrors() {
      return _createStats.getErrorCountTotal();
    }

    @Override
    public long getTotalDestroyErrors() {
      return _destroyStats.getErrorCountTotal();
    }

    @Override
    public double getCheckedOut1min() {
      return _checkedOut.getAverageConcurrency1min();
    }

    @Override
    public double getCheckedOut5min() {
      return _checkedOut.getAverageConcurrency5min();
    }

    @Override
    public double getCheckedOut15min() {
      return _checkedOut.getAverageConcurrency15min();
    }

    @Override
    public int getMaxCheckedOut1min() {
      return _checkedOut.getMaxConcurrency1min();
    }

    @Override
    public double getCheckedOutTimeAvg() {
      return nanosToMillis(_checkedOut.getCallTimeStats().getAverage());
    }

    @Override
    public double getCheckedOutTime50Pct() {
      return nanosToMillis(_checkedOut.getCallTimeStats().get50Pct());
    }

    @Override
    public double getCheckedOutTime95Pct() {
      return nanosToMillis(_checkedOut.getCallTimeStats().get95Pct());
    }

    @Override
    public double getCheckedOutTime99Pct() {
      return nanosToMillis(_checkedOut.getCallTimeStats().get99Pct());
    }

    @Override
    public int getMaxPoolSize() {
      return _maxPoolSize;
    }

    @Override
    public int getMinPoolSize() {
      return _minPoolSize;
    }

    @Override
    public int getPoolSize() {
      return _poolSize;
    }

    @Override
    public int getIdleCount() {
      return _idleCount;
    }

    @Override
    public double getWaiters1min() {
      return _waitStats.getAverageConcurrency1min();
    }

    @Override
    public double getWaiters5min() {
      return _waitStats.getAverageConcurrency5min();
    }

    @Override
    public double getWaiters15min() {
      return _waitStats.getAverageConcurrency15min();
    }

    @Override
    public double getWaitTimeAvg() {
      return nanosToMillis(_waitStats.getCallTimeStats().getAverage());
    }

    @Override
    public double getWaitTime50Pct() {
      return nanosToMillis(_waitStats.getCallTimeStats().get50Pct());
    }

    @Override
    public double getWaitTime95Pct() {
      return nanosToMillis(_waitStats.getCallTimeStats().get95Pct());
    }

    @Override
    public double getWaitTime99Pct() {
      return nanosToMillis(_waitStats.getCallTimeStats().get99Pct());
    }

    @Override
    public LifeCycleStats getLifecycleStats() {
      return new LifeCycleStatsImpl(
          _createStats.getCallTimeStats(),
          _testStats.getCallTimeStats(),
          _destroyStats.getCallTimeStats());
    }
  }

  private static class LifeCycleStatsImpl implements PoolStats.LifeCycleStats {
    private final LongStats _createCallStats;
    private final LongStats _testCallStats;
    private final LongStats _destroyCallStats;

    private LifeCycleStatsImpl(LongStats createCallStats, LongStats testCallStats, LongStats destroyCallStats) {
      _createCallStats = createCallStats;
      _testCallStats = testCallStats;
      _destroyCallStats = destroyCallStats;
    }

    @Override
    public double getCreateTimeAvg() {
      return nanosToMillis(_createCallStats.getAverage());
    }

    @Override
    public double getCreateTime50Pct() {
      return nanosToMillis(_createCallStats.get50Pct());
    }

    @Override
    public double getCreateTime95Pct() {
      return nanosToMillis(_createCallStats.get95Pct());
    }

    @Override
    public double getCreateTime99Pct() {
      return nanosToMillis(_createCallStats.get99Pct());
    }

    @Override
    public double getTestTimeAvg() {
      return nanosToMillis(_testCallStats.getAverage());
    }

    @Override
    public double getTestTime50Pct() {
      return nanosToMillis(_testCallStats.get50Pct());
    }

    @Override
    public double getTestTime95Pct() {
      return nanosToMillis(_testCallStats.get95Pct());
    }

    @Override
    public double getTestTime99Pct() {
      return nanosToMillis(_testCallStats.get99Pct());
    }

    @Override
    public double getDestroyTimeAvg() {
      return nanosToMillis(_destroyCallStats.getAverage());
    }

    @Override
    public double getDestroyTime50Pct() {
      return nanosToMillis(_destroyCallStats.get50Pct());
    }

    @Override
    public double getDestroyTime95Pct() {
      return nanosToMillis(_destroyCallStats.get95Pct());

    }

    @Override
    public double getDestroyTime99Pct() {
      return nanosToMillis(_destroyCallStats.get99Pct());
    }
  }

  private static double nanosToMillis(double nanos) {
    return Double.isFinite(nanos) ? nanos / 1000000.0 : Double.NaN;
  }

  private static double nanosToMillis(Long nanos) {
    return nanos != null ? nanos / 1000000.0 : Double.NaN;
  }

  private class IdleEntity implements Supplier<T> {
    private final long _idleNanos;
    private final T _idleEntity;

    private IdleEntity(T idleEntity) {
      _idleNanos = Time.nanoTime();
      _idleEntity = idleEntity;
    }

    @Override
    public T get() {
      return _idleEntity;
    }
  }

  public static class TooManyWaitersException extends CancellationException {
  }

  public static class ShutdownException extends CancellationException {
  }
}
