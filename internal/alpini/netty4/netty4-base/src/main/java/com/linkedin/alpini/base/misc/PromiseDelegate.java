package com.linkedin.alpini.base.misc;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * A simple delegate class which wraps an existing {@link Promise} and permits
 * overriding of some of its methods.
 *
 * @author acurtis
 *
 * @param <V> promise type
 */
public class PromiseDelegate<V> implements Promise<V> {
  private final Promise<V> _promise;

  public PromiseDelegate(Promise<V> promise) {
    _promise = promise;
  }

  @Override
  public final Promise<V> setSuccess(V result) {
    if (trySuccess(result)) {
      return this;
    }
    throw new IllegalStateException("complete already: " + this);
  }

  @Override
  public boolean trySuccess(V result) {
    return _promise.trySuccess(result);
  }

  @Override
  public final Promise<V> setFailure(Throwable cause) {
    if (tryFailure(cause)) {
      return this;
    }
    throw new IllegalStateException("complete already: " + this);
  }

  @Override
  public boolean tryFailure(Throwable cause) {
    return _promise.tryFailure(cause);
  }

  @Override
  public boolean setUncancellable() {
    return _promise.setUncancellable();
  }

  @Override
  public boolean isSuccess() {
    return _promise.isSuccess();
  }

  @Override
  public final boolean isCancellable() {
    return _promise.isCancellable();
  }

  @Override
  public final Throwable cause() {
    return _promise.cause();
  }

  @Override
  public Promise<V> addListener(GenericFutureListener<? extends Future<? super V>> listener) {
    _promise.addListener(listener);
    return this;
  }

  @Override
  public Promise<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners) {
    _promise.addListeners(listeners);
    return this;
  }

  @Override
  public Promise<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener) {
    _promise.removeListener(listener);
    return this;
  }

  @Override
  public Promise<V> removeListeners(GenericFutureListener<? extends Future<? super V>>... listeners) {
    _promise.removeListeners(listeners);
    return this;
  }

  @Override
  public Promise<V> await() throws InterruptedException {
    _promise.await();
    return this;
  }

  @Override
  public Promise<V> awaitUninterruptibly() {
    _promise.awaitUninterruptibly();
    return this;
  }

  @Override
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    return _promise.await(timeout, unit);
  }

  @Override
  public boolean await(long timeoutMillis) throws InterruptedException {
    return _promise.await(timeoutMillis);
  }

  @Override
  public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
    return _promise.awaitUninterruptibly(timeout, unit);
  }

  @Override
  public boolean awaitUninterruptibly(long timeoutMillis) {
    return _promise.awaitUninterruptibly(timeoutMillis);
  }

  @Override
  public V getNow() {
    return _promise.getNow();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return _promise.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return _promise.isCancelled();
  }

  @Override
  public boolean isDone() {
    return _promise.isDone();
  }

  @Override
  public final V get() throws InterruptedException, ExecutionException {
    return _promise.get();
  }

  @Override
  public final V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return _promise.get(timeout, unit);
  }

  @Override
  public final Promise<V> sync() throws InterruptedException {
    _promise.sync();
    return this;
  }

  @Override
  public final Promise<V> syncUninterruptibly() {
    _promise.syncUninterruptibly();
    return this;
  }
}
