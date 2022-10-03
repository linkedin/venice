package com.linkedin.alpini.base.pool.impl;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.pool.AsyncPool;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class RateLimitedCreateLifeCycle<T> extends LifeCycleFilter<T> {
  private final ScheduledExecutorService _executor;
  private final AtomicLong _lastCreateTime;
  private final long _minimumTimeDelay;
  private final long _maximumTimeDelay;
  private final long _timeIncrement;
  private long _timeDelay;

  public RateLimitedCreateLifeCycle(
      @Nonnull AsyncPool.LifeCycle<T> lifeCycle,
      @Nonnull ScheduledExecutorService executor,
      long minimumTimeDelay,
      long maximumTimeDelay,
      long timeIncrement,
      @Nonnull TimeUnit unit) {
    super(Objects.requireNonNull(lifeCycle, "lifeCycle"));
    _executor = Objects.requireNonNull(executor, "executor");
    Objects.requireNonNull(unit, "unit");
    _minimumTimeDelay = unit.toNanos(positive(minimumTimeDelay));
    _maximumTimeDelay = unit.toNanos(positive(maximumTimeDelay));
    if (_maximumTimeDelay <= _minimumTimeDelay) {
      throw new IllegalArgumentException("maximum time must exceed minimum time");
    }
    _timeIncrement = unit.toNanos(positive(timeIncrement));
    _lastCreateTime = new AtomicLong(Time.nanoTime());
  }

  private static long positive(long number) {
    if (number < 1) {
      throw new IllegalArgumentException("time values must be positive");
    }
    return number;
  }

  @Override
  public CompletableFuture<T> create() {
    long now = Time.nanoTime();

    long lastCreateTime = _lastCreateTime.get();
    if (lastCreateTime + Math.max(_timeDelay, _minimumTimeDelay) < now
        && _lastCreateTime.compareAndSet(lastCreateTime, now)) {
      return super.create();
    }
    CompletableFuture<T> future = new CompletableFuture<>();
    while (true) {
      long timeDelay = Math.max(_timeDelay, _minimumTimeDelay);
      long delta = lastCreateTime + timeDelay - now;
      if (_lastCreateTime.compareAndSet(lastCreateTime, lastCreateTime + timeDelay)) {
        _executor.schedule(() -> super.create().whenComplete((t, ex) -> {
          if (ex != null) {
            _timeDelay = Math.min(_maximumTimeDelay, _timeDelay + _timeIncrement);
            future.completeExceptionally(ex);
          } else {
            _timeDelay = 0;
            if (!future.complete(t)) {
              destroy(t);
            }
          }
        }), delta, TimeUnit.NANOSECONDS);
        return future;
      }
    }
  }

  @Override
  public CompletableFuture<Boolean> testOnRelease(T entity) {
    return super.testOnRelease(entity);
  }

  @Override
  public CompletableFuture<Boolean> testAfterIdle(T entity) {
    return super.testAfterIdle(entity);
  }

  @Override
  public CompletableFuture<Void> destroy(T entity) {
    return super.destroy(entity);
  }
}
