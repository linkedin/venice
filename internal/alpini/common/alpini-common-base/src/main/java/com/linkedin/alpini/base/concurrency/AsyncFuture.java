package com.linkedin.alpini.base.concurrency;

import com.linkedin.alpini.base.concurrency.impl.CancelledAsyncFuture;
import com.linkedin.alpini.base.concurrency.impl.DefaultAsyncFuture;
import com.linkedin.alpini.base.concurrency.impl.DefaultCollectingAsyncFuture;
import com.linkedin.alpini.base.concurrency.impl.FailedAsyncFuture;
import com.linkedin.alpini.base.concurrency.impl.SuccessAsyncFuture;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;


/**
 * A {@link Future} which has very netty-style semantics where {@link AsyncFutureListener} can
 * be added to the future to be executed upon condition of Success or Failure has been satisfied.
 *
 * See {@link com.linkedin.alpini.base.concurrency.impl.DefaultAsyncFuture}.
 *
 * See also {@link com.linkedin.alpini.base.concurrency.impl.DefaultAsyncFutureTask}
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public interface AsyncFuture<T> extends Future<T>, CompletionStage<T> {
  /**
   * Non-blocking variant of {@link #get()}
   * @return value or {@code null}
   */
  T getNow();

  /**
   * Returns {@code true} if and only if the I/O operation was completed
   * successfully.
   */
  boolean isSuccess();

  /**
   * Returns the cause of the failed I/O operation if the I/O operation has
   * failed.
   *
   * @return the cause of the failure.
   *         {@code null} if succeeded or this future is not
   *         completed yet.
   */
  Throwable getCause();

  /**
   * Adds the specified listener to this future.  The
   * specified listener is notified when this future is
   * {@linkplain #isDone() done}.  If this future is already
   * completed, the specified listener is notified immediately.
   * @return {@code this} to permit chaining of operations.
   */
  @Nonnull
  AsyncFuture<T> addListener(AsyncFutureListener<T> listener);

  /**
   * Adds the specified future as a listener to this future.  The
   * specified future is notified when this future is
   * {@linkplain #isDone() done}.  If this future is already
   * completed, the specified future is notified immediately.
   * @return {@code this} to permit chaining of operations.
   * @param listener
   */
  @Nonnull
  AsyncFuture<T> addListener(AsyncPromise<T> listener);

  /**
   * Waits for this future to be completed.
   *
   * @throws InterruptedException
   *         if the current thread was interrupted
   */
  @Nonnull
  AsyncFuture<T> await() throws InterruptedException;

  /**
   * Waits for this future to be completed without
   * interruption.  This method catches an {@link InterruptedException} and
   * discards it silently.
   */
  @Nonnull
  AsyncFuture<T> awaitUninterruptibly();

  /**
   * Waits for this future to be completed within the
   * specified time limit.
   *
   * @return {@code true} if and only if the future was completed within
   *         the specified time limit
   *
   * @throws InterruptedException
   *         if the current thread was interrupted
   */
  boolean await(long timeout, TimeUnit unit) throws InterruptedException;

  /**
   * Waits for this future to be completed within the
   * specified time limit without interruption.  This method catches an
   * {@link InterruptedException} and discards it silently.
   *
   * @return {@code true} if and only if the future was completed within
   *         the specified time limit
   */
  boolean awaitUninterruptibly(long timeout, TimeUnit unit);

  static <T> AsyncFuture<T> of(@Nonnull AsyncFuture<T> stage, boolean cancellable) {
    // cancellable argument is ignored because it is already an AsyncFuture.
    return stage;
  }

  static <T> AsyncFuture<T> of(@Nonnull CompletionStage<T> stage, boolean cancellable) {
    if (stage instanceof AsyncFuture) {
      return (AsyncFuture<T>) stage;
    }
    AsyncPromise<T> promise = deferred(cancellable);
    stage.whenComplete(promise::setComplete);
    return promise;
  }

  /**
   * Obtain an instance of an AsyncFuture which is in the Cancelled state.
   * @param <T> any type.
   * @return Cancelled future.
   */
  static <T> AsyncPromise<T> cancelled() {
    return CancelledAsyncFuture.getInstance();
  }

  /**
   * Create a new default deferred future.
   * @param cancellable {@literal true} if cancellable.
   * @param <T> Type of the future.
   * @return new deferred async future.
   */
  static <T> AsyncPromise<T> deferred(boolean cancellable) {
    return new DefaultAsyncFuture<>(cancellable);
  }

  /**
   * Create a new failed future. These futures are already in the Done state.
   * @param exception exception.
   * @param <T> Type of the future.
   * @return new failed async future.
   */
  static <T> AsyncPromise<T> failed(@Nonnull Throwable exception) {
    return new FailedAsyncFuture<>(exception);
  }

  AsyncPromise<?> NULL_SUCCESS = new SuccessAsyncFuture<>(null);

  /**
   * Create a new success future. These futures are already in the Done state.
   * @param value value of future.
   * @param <T> Type of the value.
   * @return new success async future.
   */
  @SuppressWarnings("unchecked")
  static <T, V extends T> AsyncPromise<T> success(V value) {
    return value != null ? new SuccessAsyncFuture<>(value) : (AsyncPromise<T>) NULL_SUCCESS;
  }

  static <T> AsyncFuture<List<T>> collect(List<AsyncFuture<List<T>>> futuresList, boolean cancellable) {
    return new DefaultCollectingAsyncFuture<>(futuresList, cancellable);
  }

  enum Status {
    SUCCESS, // terminal state
    CANCELLATION_EXCEPTION, // terminal state CANCELLED
    OTHER_EXCEPTION, // terminal state
    DONE, // terminal state
    INFLIGHT,
  }

  static Status getStatus(AsyncFuture<?> future) {
    return !future.isDone()
        ? Status.INFLIGHT
        : future.isSuccess()
            ? Status.SUCCESS
            : future.isCancelled() ? Status.CANCELLATION_EXCEPTION : Status.OTHER_EXCEPTION;
  }
}
