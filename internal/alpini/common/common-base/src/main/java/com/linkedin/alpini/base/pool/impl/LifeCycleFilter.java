package com.linkedin.alpini.base.pool.impl;

import com.linkedin.alpini.base.pool.AsyncPool;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class LifeCycleFilter<T> implements AsyncPool.LifeCycle<T> {
  private final AsyncPool.LifeCycle<T> _lifeCycle;

  public LifeCycleFilter(AsyncPool.LifeCycle<T> lifeCycle) {
    _lifeCycle = Objects.requireNonNull(lifeCycle);
  }

  @Override
  public CompletableFuture<T> create() {
    return _lifeCycle.create();
  }

  @Override
  public CompletableFuture<Boolean> testOnRelease(T entity) {
    return _lifeCycle.testOnRelease(entity);
  }

  @Override
  public CompletableFuture<Boolean> testAfterIdle(T entity) {
    return _lifeCycle.testAfterIdle(entity);
  }

  @Override
  public CompletableFuture<Void> destroy(T entity) {
    return _lifeCycle.destroy(entity);
  }

  @Override
  public <W> W unwrap(Class<W> iface) {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    } else {
      return _lifeCycle.unwrap(iface);
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isAssignableFrom(getClass()) || _lifeCycle.isWrapperFor(iface);
  }
}
