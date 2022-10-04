package com.linkedin.alpini.base.pool.impl;

import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.base.pool.AsyncPool;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class LifeCycleStatsCollector<T> extends LifeCycleFilter<T> {
  private final CallTracker _createCallTracker;
  private final CallTracker _testCallTracker;
  private final CallTracker _destroyCallTracker;

  public LifeCycleStatsCollector(@Nonnull AsyncPool.LifeCycle<T> lifeCycle) {
    super(lifeCycle);

    _createCallTracker = createCallTracker();
    _testCallTracker = createCallTracker();
    _destroyCallTracker = createCallTracker();
  }

  protected CallTracker createCallTracker() {
    return CallTracker.create();
  }

  @Override
  public CompletableFuture<T> create() {
    CallCompletion completion = _createCallTracker.startCall();
    return super.create().whenComplete(completion::closeCompletion);
  }

  public CallTracker.CallStats getCreateCallStats() {
    return _createCallTracker.getCallStats();
  }

  @Override
  public CompletableFuture<Boolean> testOnRelease(T entity) {
    CallCompletion completion = _testCallTracker.startCall();
    return super.testOnRelease(entity).whenComplete(completion::closeCompletion);
  }

  @Override
  public CompletableFuture<Boolean> testAfterIdle(T entity) {
    CallCompletion completion = _testCallTracker.startCall();
    return super.testAfterIdle(entity).whenComplete(completion::closeCompletion);
  }

  public CallTracker.CallStats getTestCallStats() {
    return _testCallTracker.getCallStats();
  }

  @Override
  public CompletableFuture<Void> destroy(T entity) {
    CallCompletion completion = _destroyCallTracker.startCall();
    return super.destroy(entity).whenComplete(completion::closeCompletion);
  }

  public CallTracker.CallStats getDestroyCallStats() {
    return _destroyCallTracker.getCallStats();
  }
}
