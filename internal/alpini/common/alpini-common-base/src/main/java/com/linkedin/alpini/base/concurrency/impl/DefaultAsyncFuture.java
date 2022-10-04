package com.linkedin.alpini.base.concurrency.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncFutureListener;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.misc.Time;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An implementation of {@link AsyncFuture} where much of the implementation is based upon
 * netty's {@code org.jboss.netty.channel.DefaultChannelFuture}.
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class DefaultAsyncFuture<T> extends CompletableFuture<T> implements AsyncPromise<T>, Time.Awaitable {
  private static final Logger LOG = LogManager.getLogger(AsyncFuture.class);

  private final boolean _cancellable;

  /**
   * Creates a new instance.
   *
   * @param cancellable
   *        {@code true} if and only if this future can be canceled
   */
  public DefaultAsyncFuture(boolean cancellable) {
    _cancellable = cancellable;
  }

  /**
   * @see AsyncFuture#isSuccess()
   */
  @Override
  public final boolean isSuccess() {
    return super.isDone() && !super.isCompletedExceptionally();
  }

  /**
   * @see AsyncFuture#getCause()
   */
  @Override
  public final Throwable getCause() {
    if (isDone()) {
      Throwable e = super.handle((ignore, ex) -> ex).join();
      // CompletableFuture likes to wrap exceptions in CompletionExceptions
      // We unwrap them so that the caller gets the thrown exception.
      while (e instanceof CompletionException) {
        e = e.getCause();
      }
      return e;
    }
    return null;
  }

  /**
   * @see AsyncPromise#setSuccess(Object)
   */
  @Override
  public boolean setSuccess(T result) {
    return super.complete(result);
  }

  /**
   * @see AsyncPromise#setFailure(Throwable)
   */
  @Override
  public boolean setFailure(@Nonnull Throwable t) {
    return super.completeExceptionally(t);
  }

  /**
   * @see AsyncFuture#addListener(AsyncFutureListener)
   */
  @Override
  public @Nonnull AsyncPromise<T> addListener(AsyncFutureListener<T> listener) {
    Objects.requireNonNull(listener, "listener");

    whenComplete((v, t) -> notifyListener(listener));

    return this;
  }

  /**
   * Adds the specified future as a listener to this future.  The
   * specified future is notified when this future is
   * {@linkplain #isDone() done}.  If this future is already
   * completed, the specified future is notified immediately.
   * @param listener
   */
  @Override
  public @Nonnull AsyncPromise<T> addListener(final AsyncPromise<T> listener) {
    Objects.requireNonNull(listener, "listener");

    handle((v, t) -> {
      if (t != null) {
        return listener.setFailure(t);
      } else {
        return listener.setSuccess(v);
      }
    });

    return this;
  }

  /**
   * @see AsyncFuture#await()
   */
  @Override
  public final @Nonnull AsyncFuture<T> await() throws InterruptedException {
    if (!isDone()) {
      try {
        get();
      } catch (ExecutionException | CancellationException ignored) {
      }
    }
    return this;
  }

  /**
   * @see AsyncFuture#awaitUninterruptibly()
   */
  @Override
  public final @Nonnull AsyncFuture<T> awaitUninterruptibly() {
    while (!isDone()) {
      try {
        join();
      } catch (RuntimeException ignored) {
      }
    }
    return this;
  }

  /**
   * @see AsyncFuture#await(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public final boolean await(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
    if (!isDone()) {
      try {
        super.get(timeout, unit);
      } catch (TimeoutException ignored) {
        return false;
      } catch (ExecutionException | CancellationException ignored) {
      }
    }
    return true;
  }

  /**
   * @see AsyncFuture#awaitUninterruptibly(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public final boolean awaitUninterruptibly(long timeout, @Nonnull TimeUnit unit) {
    final long deadline = unit.toNanos(timeout) + Time.nanoTime();
    long remaining;
    while (!isDone() && (remaining = deadline - Time.nanoTime()) > 0) { // SUPPRESS CHECKSTYLE InnerAssignment
      try {
        if (await(remaining, TimeUnit.NANOSECONDS)) {
          return true;
        }
      } catch (CancellationException | InterruptedException ignored) {
      }
    }
    return isDone();
  }

  @Override
  public T get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    boolean res = Time.await(this, timeout, unit);
    if (res) {
      return get();
    } else {
      throw new TimeoutException();
    }
  }

  @Override
  public final boolean complete(T value) {
    return setSuccess(value);
  }

  @Override
  public final boolean completeExceptionally(Throwable ex) {
    return setFailure(ex);
  }

  /**
   * @see java.util.concurrent.Future#cancel(boolean)
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return _cancellable && super.cancel(mayInterruptIfRunning);
  }

  @Override
  public final T getNow() {
    return super.getNow(null);
  }

  private Void notifyListener(@Nonnull AsyncFutureListener<T> l) {
    return notifyListener(this, l);
  }

  static <T> Void notifyListener(@Nonnull AsyncFuture<T> future, @Nonnull AsyncFutureListener<T> l) {
    try {
      l.operationComplete(future);
    } catch (Throwable t) {
      LOG.warn("An exception was thrown by {}.", simpleClassName(l), t);
    }
    return null;
  }

  private static String simpleClassName(Object t) {
    return t != null ? t.getClass().getSimpleName() : "null";
  }
}
