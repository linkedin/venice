package com.linkedin.alpini.base.pool.impl;

import com.linkedin.alpini.base.pool.AsyncQOSPool;
import com.linkedin.alpini.base.queuing.QOSBasedRequestRunnable;
import com.linkedin.alpini.base.queuing.QOSPolicy;
import com.linkedin.alpini.base.queuing.SimpleQueue;
import com.linkedin.alpini.consts.QOS;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class AsyncQOSPoolImpl<T> extends AsyncPoolImpl<T> implements AsyncQOSPool<T> {
  private static final Logger LOG = LogManager.getLogger(AsyncQOSPool.class);
  private final SimpleQueue<Waiter> _pending;

  public AsyncQOSPoolImpl(
      LifeCycle<T> lifeCycle,
      QOSPolicy.StaticConfig qosPolicyConfig,
      Executor executor,
      int maxConcurrentCreate,
      int minimumEntities,
      int maximumEntities,
      long maxIdleTime,
      TimeUnit maxIdleUnit) {
    this(
        lifeCycle,
        QOSPolicy.getQOSPolicy(Objects.requireNonNull(qosPolicyConfig, "qosPolicyConfig")),
        executor,
        maxConcurrentCreate,
        minimumEntities,
        maximumEntities,
        maxIdleTime,
        maxIdleUnit);
  }

  private AsyncQOSPoolImpl(
      LifeCycle<T> lifeCycle,
      SimpleQueue<Waiter> pendingQueue,
      Executor executor,
      int maxConcurrentCreate,
      int minimumEntities,
      int maximumEntities,
      long maxIdleTime,
      TimeUnit maxIdleUnit) {
    super(
        lifeCycle,
        executor,
        maxConcurrentCreate,
        minimumEntities,
        minimumEntities,
        maximumEntities,
        maxIdleTime,
        maxIdleUnit);
    _pending = Objects.requireNonNull(pendingQueue, "pendingQueue");
  }

  @Override
  public CompletableFuture<T> acquire() {
    return checkout(acquire0("", QOS.NORMAL), startWaiters());
  }

  @Override
  public CompletableFuture<T> acquire(String queueName, QOS qos) {
    return checkout(acquire0(Objects.requireNonNull(queueName), Objects.requireNonNull(qos)), startWaiters());
  }

  protected final CompletableFuture<T> acquire0(String queueName, QOS qos) {
    CompletableFuture<T> future = super.acquire0();
    if (future.isCompletedExceptionally()) {
      return future;
    }
    try {
      Waiter w = new Waiter(queueName, qos, new CompletableFuture<>());

      if (_pending.add(w)) {
        return w._future;
      } else {
        w._future.obtrudeException(new IllegalStateException());
        return w._future;
      }
    } finally {
      future.whenComplete(this::issue);
    }
  }

  private void issue(T entity, Throwable ex) {
    Waiter w;
    if (ex == null) {
      while ((w = _pending.poll()) != null) {
        if (w._future.complete(entity)) {
          return;
        }
      }
      release0(entity);
    } else {
      while ((w = _pending.poll()) != null) {
        if (w._future.completeExceptionally(ex)) {
          return;
        }
      }
      LOG.warn("Dropped exception", ex);
    }
  }

  private class Waiter extends QOSBasedRequestRunnable {
    private final CompletableFuture<T> _future;

    public Waiter(String queueName, QOS qos, CompletableFuture<T> future) {
      super(queueName, qos, null);
      _future = future;
    }
  }
}
