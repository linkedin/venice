package com.linkedin.alpini.netty4.http2;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.EventExecutor;


/**
 * Provides the ability to associate the outcome of multiple {@link ChannelPromise}
 * objects into a single {@link ChannelPromise} object.
 *
 * This implementation is copied from Http2CodecUtil
 */
final class SimpleChannelPromiseAggregator extends DefaultChannelPromise {
  private final ChannelPromise promise;
  private int expectedCount;
  private int doneCount;
  private Throwable lastFailure;
  private boolean doneAllocating;

  SimpleChannelPromiseAggregator(ChannelPromise promise, Channel c, EventExecutor e) {
    super(c, e);
    assert promise != null && !promise.isDone();
    this.promise = promise;
  }

  /**
   * Allocate a new promise which will be used to aggregate the overall success of this promise aggregator.
   * @return A new promise which will be aggregated.
   * {@code null} if {@link #doneAllocatingPromises()} was previously called.
   */
  public ChannelPromise newPromise() {
    assert !doneAllocating : "Done allocating. No more promises can be allocated.";
    ++expectedCount;
    return this;
  }

  /**
   * Signify that no more {@link #newPromise()} allocations will be made.
   * The aggregation can not be successful until this method is called.
   * @return The promise that is the aggregation of all promises allocated with {@link #newPromise()}.
   */
  public ChannelPromise doneAllocatingPromises() {
    if (!doneAllocating) {
      doneAllocating = true;
      if (doneCount == expectedCount || expectedCount == 0) {
        return setPromise();
      }
    }
    return this;
  }

  @Override
  public boolean tryFailure(Throwable cause) {
    if (allowFailure()) {
      ++doneCount;
      lastFailure = cause;
      if (allPromisesDone()) {
        return tryPromise();
      }
      // TODO: We break the interface a bit here.
      // Multiple failure events can be processed without issue because this is an aggregation.
      return true;
    }
    return false;
  }

  /**
   * Fail this object if it has not already been failed.
   * <p>
   * This method will NOT throw an {@link IllegalStateException} if called multiple times
   * because that may be expected.
   */
  @Override
  public ChannelPromise setFailure(Throwable cause) {
    if (allowFailure()) {
      ++doneCount;
      lastFailure = cause;
      if (allPromisesDone()) {
        return setPromise();
      }
    }
    return this;
  }

  @Override
  public ChannelPromise setSuccess(Void result) {
    if (awaitingPromises()) {
      ++doneCount;
      if (allPromisesDone()) {
        setPromise();
      }
    }
    return this;
  }

  @Override
  public boolean trySuccess(Void result) {
    if (awaitingPromises()) {
      ++doneCount;
      if (allPromisesDone()) {
        return tryPromise();
      }
      // TODO: We break the interface a bit here.
      // Multiple success events can be processed without issue because this is an aggregation.
      return true;
    }
    return false;
  }

  private boolean allowFailure() {
    return awaitingPromises() || expectedCount == 0;
  }

  private boolean awaitingPromises() {
    return doneCount < expectedCount;
  }

  private boolean allPromisesDone() {
    return doneCount == expectedCount && doneAllocating;
  }

  private ChannelPromise setPromise() {
    if (lastFailure == null) {
      promise.setSuccess();
      return super.setSuccess(null);
    } else {
      promise.setFailure(lastFailure);
      return super.setFailure(lastFailure);
    }
  }

  private boolean tryPromise() {
    if (lastFailure == null) {
      promise.trySuccess();
      return super.trySuccess(null);
    } else {
      promise.tryFailure(lastFailure);
      return super.tryFailure(lastFailure);
    }
  }
}
