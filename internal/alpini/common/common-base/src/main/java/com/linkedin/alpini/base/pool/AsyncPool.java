package com.linkedin.alpini.base.pool;

import com.linkedin.alpini.base.pool.impl.AsyncPoolImpl;
import com.linkedin.alpini.base.pool.impl.RateLimitedCreateLifeCycle;
import com.linkedin.alpini.base.registry.Shutdownable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface AsyncPool<T> {
  void start();

  int size();

  CompletableFuture<T> acquire();

  void release(T entity);

  void dispose(T entity);

  default CompletableFuture<Void> shutdownPool() {
    if (this instanceof Shutdownable) {
      ((Shutdownable) this).shutdown();
      return CompletableFuture.supplyAsync(() -> {
        try {
          ((Shutdownable) this).waitForShutdown();
          return null;
        } catch (InterruptedException e) {
          throw new CompletionException(e);
        }
      });
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  interface LifeCycle<T> {
    CompletableFuture<T> create();

    CompletableFuture<Boolean> testOnRelease(T entity);

    CompletableFuture<Boolean> testAfterIdle(T entity);

    CompletableFuture<Void> destroy(T entity);

    default CompletableFuture<Void> shutdown() {
      return CompletableFuture.completedFuture(null);
    }

    default <W> W unwrap(Class<W> iface) {
      if (isWrapperFor(iface)) {
        return iface.cast(this);
      } else {
        throw new IllegalArgumentException();
      }
    }

    default boolean isWrapperFor(Class<?> iface) {
      return iface.isAssignableFrom(getClass());
    }
  }

  PoolStats getPoolStats();

  static <T> AsyncPool<T> create(
      @Nonnull LifeCycle<T> lifeCycle,
      @Nonnull Executor executor,
      int maxConcurrentCreate,
      int maxWaiters,
      int minimumEntities,
      int maximumEntities,
      long maxIdleTime,
      @Nonnull TimeUnit maxIdleUnit) {
    return new AsyncPoolImpl<>(
        lifeCycle,
        executor,
        maxConcurrentCreate,
        maxWaiters,
        minimumEntities,
        maximumEntities,
        maxIdleTime,
        maxIdleUnit);
  }

  static <T> LifeCycle<T> rateLimitCreate(
      @Nonnull AsyncPool.LifeCycle<T> lifeCycle,
      @Nonnull ScheduledExecutorService executor,
      long minimumTimeDelay,
      long maximumTimeDelay,
      long timeIncrement,
      @Nonnull TimeUnit unit) {
    return new RateLimitedCreateLifeCycle<>(
        lifeCycle,
        executor,
        minimumTimeDelay,
        maximumTimeDelay,
        timeIncrement,
        unit);
  }

}
