package com.linkedin.alpini.base.concurrency.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncFutureListener;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class FailedAsyncFuture<T> extends AbstractAsyncFuture<T> implements AsyncPromise<T> {
  private final Throwable _throwable;

  public FailedAsyncFuture(@Nonnull Throwable throwable) {
    _throwable = throwable;
  }

  /**
   * Returns {@code true} if and only if the I/O operation was completed
   * successfully.
   */
  @Override
  public final boolean isSuccess() {
    return false;
  }

  /**
   * Returns the cause of the failed I/O operation if the I/O operation has
   * failed.
   *
   * @return the cause of the failure.
   * {@code null} if succeeded or this future is not
   * completed yet.
   */
  @Override
  public final Throwable getCause() {
    return _throwable;
  }

  /**
   * Marks this future as a success and notifies all
   * listeners.
   *
   * @param result
   * @return {@code true} if and only if successfully marked this future as
   * a success. Otherwise {@code false} because this future is
   * already marked as either a success or a failure.
   */
  @Override
  public final boolean setSuccess(T result) {
    return false;
  }

  /**
   * Marks this future as a failure and notifies all
   * listeners.
   *
   * @param cause
   * @return {@code true} if and only if successfully marked this future as
   * a failure. Otherwise {@code false} because this future is
   * already marked as either a success or a failure.
   */
  @Override
  public final boolean setFailure(@Nonnull Throwable cause) {
    return false;
  }

  /**
   * Adds the specified listener to this future.  The
   * specified listener is notified when this future is
   * {@linkplain #isDone() done}.  If this future is already
   * completed, the specified listener is notified immediately.
   *
   * @param listener
   * @return {@code this} to permit chaining of operations.
   */
  @Override
  public final @Nonnull AsyncPromise<T> addListener(@Nonnull AsyncFutureListener<T> listener) {
    if (!isCancelled()) {
      DefaultAsyncFuture.notifyListener(this, listener);
    }
    return this;
  }

  /**
   * Adds the specified future as a listener to this future.  The
   * specified future is notified when this future is
   * {@linkplain #isDone() done}.  If this future is already
   * completed, the specified future is notified immediately.
   *
   * @param listener
   * @return {@code this} to permit chaining of operations.
   */
  @Override
  public final @Nonnull AsyncPromise<T> addListener(@Nonnull AsyncPromise<T> listener) {
    if (!isCancelled()) {
      listener.setFailure(getCause());
    }
    return this;
  }

  /**
   * Waits for this future to be completed.
   *
   * @throws InterruptedException if the current thread was interrupted
   */
  @Override
  public final @Nonnull AsyncFuture<T> await() throws InterruptedException {
    return this;
  }

  /**
   * Waits for this future to be completed without
   * interruption.  This method catches an {@link InterruptedException} and
   * discards it silently.
   */
  @Override
  public final @Nonnull AsyncFuture<T> awaitUninterruptibly() {
    return this;
  }

  /**
   * Waits for this future to be completed within the
   * specified time limit.
   *
   * @param timeout
   * @param unit
   * @return {@code true} if and only if the future was completed within
   * the specified time limit
   * @throws InterruptedException if the current thread was interrupted
   */
  @Override
  public final boolean await(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
    return true;
  }

  /**
   * Waits for this future to be completed within the
   * specified time limit without interruption.  This method catches an
   * {@link InterruptedException} and discards it silently.
   *
   * @param timeout
   * @param unit
   * @return {@code true} if and only if the future was completed within
   * the specified time limit
   */
  @Override
  public final boolean awaitUninterruptibly(long timeout, @Nonnull TimeUnit unit) {
    return true;
  }

  /**
   * Attempts to cancel execution of this task.  This attempt will
   * fail if the task has already completed, has already been cancelled,
   * or could not be cancelled for some other reason. If successful,
   * and this task has not started when {@code cancel} is called,
   * this task should never run.  If the task has already started,
   * then the {@code mayInterruptIfRunning} parameter determines
   * whether the thread executing this task should be interrupted in
   * an attempt to stop the task.
   * <p>
   * <p>After this method returns, subsequent calls to {@link #isDone} will
   * always return {@code true}.  Subsequent calls to {@link #isCancelled}
   * will always return {@code true} if this method returned {@code true}.
   *
   * @param mayInterruptIfRunning {@code true} if the thread executing this
   *                              task should be interrupted; otherwise, in-progress tasks are allowed
   *                              to complete
   * @return {@code false} if the task could not be cancelled,
   * typically because it has already completed normally;
   * {@code true} otherwise
   */
  @Override
  public final boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  /**
   * Returns {@code true} if this task was cancelled before it completed
   * normally.
   *
   * @return {@code true} if this task was cancelled before it completed
   */
  @Override
  public boolean isCancelled() {
    return _throwable instanceof CancellationException;
  }

  /**
   * Returns {@code true} if this task completed.
   * <p>
   * Completion may be due to normal termination, an exception, or
   * cancellation -- in all of these cases, this method will return
   * {@code true}.
   *
   * @return {@code true} if this task completed
   */
  @Override
  public final boolean isDone() {
    return true;
  }

  /**
   * Waits if necessary for the computation to complete, and then
   * retrieves its result.
   *
   * @return the computed result
   * @throws CancellationException if the computation was cancelled
   * @throws ExecutionException    if the computation threw an
   *                               exception
   * @throws InterruptedException  if the current thread was interrupted
   *                               while waiting
   */
  @Override
  public final T get() throws InterruptedException, ExecutionException {
    throw new ExecutionException(getCause());
  }

  /**
   * Waits if necessary for at most the given time for the computation
   * to complete, and then retrieves its result, if available.
   *
   * @param timeout the maximum time to wait
   * @param unit    the time unit of the timeout argument
   * @return the computed result
   * @throws CancellationException if the computation was cancelled
   * @throws ExecutionException    if the computation threw an
   *                               exception
   * @throws InterruptedException  if the current thread was interrupted
   *                               while waiting
   * @throws TimeoutException      if the wait timed out
   */
  @Override
  public final T get(long timeout, @Nonnull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return get();
  }

  @Override
  public final T getNow() {
    return null;
  }

  @SuppressWarnings("unchecked")
  private <U> CompletionStage<U> self() {
    return (CompletionStage) this;
  }

  @Override
  public <U> CompletionStage<U> thenApply(Function<? super T, ? extends U> fn) {
    return self();
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
    return self();
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
    return self();
  }

  @Override
  public CompletionStage<Void> thenAccept(Consumer<? super T> action) {
    return self();
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action) {
    return self();
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
    return self();
  }

  @Override
  public CompletionStage<Void> thenRun(Runnable action) {
    return self();
  }

  @Override
  public CompletionStage<Void> thenRunAsync(Runnable action) {
    return self();
  }

  @Override
  public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
    return self();
  }

  @Override
  public <U, V> CompletionStage<V> thenCombine(
      CompletionStage<? extends U> other,
      BiFunction<? super T, ? super U, ? extends V> fn) {
    return self();
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(
      CompletionStage<? extends U> other,
      BiFunction<? super T, ? super U, ? extends V> fn) {
    return self();
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(
      CompletionStage<? extends U> other,
      BiFunction<? super T, ? super U, ? extends V> fn,
      Executor executor) {
    return self();
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBoth(
      CompletionStage<? extends U> other,
      BiConsumer<? super T, ? super U> action) {
    return self();
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(
      CompletionStage<? extends U> other,
      BiConsumer<? super T, ? super U> action) {
    return self();
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(
      CompletionStage<? extends U> other,
      BiConsumer<? super T, ? super U> action,
      Executor executor) {
    return self();
  }

  @Override
  public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
    return self();
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
    return self();
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
    return self();
  }

  @Override
  public <U> CompletionStage<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return self();
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return self();
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(
      CompletionStage<? extends T> other,
      Function<? super T, U> fn,
      Executor executor) {
    return self();
  }

  @Override
  public CompletionStage<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
    return self();
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
    return self();
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(
      CompletionStage<? extends T> other,
      Consumer<? super T> action,
      Executor executor) {
    return self();
  }

  @Override
  public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
    return self();
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
    return self();
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
    return self();
  }

  @Override
  public <U> CompletionStage<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
    return self();
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
    return self();
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      Function<? super T, ? extends CompletionStage<U>> fn,
      Executor executor) {
    return self();
  }

  @Override
  public CompletableFuture<T> toCompletableFuture() {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.obtrudeException(getCause());
    return future;
  }
}
