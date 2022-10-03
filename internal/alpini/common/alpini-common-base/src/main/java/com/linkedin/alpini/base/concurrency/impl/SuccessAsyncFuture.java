package com.linkedin.alpini.base.concurrency.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncFutureListener;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;


/**
 * A simple implementation of {@link AsyncFuture} which behaves as if the Future has already
 * completed successfully.
 *
 * <p/>note: The AsyncFuture classes would be replaced with the Promise classes available in Netty4.
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public final class SuccessAsyncFuture<T> extends AbstractAsyncFuture<T> implements AsyncPromise<T> {
  private final T _value;

  /**
   * Construct an AsyncFuture which is already in success state.
   *
   * @param value Value to be returned by {@linkplain #get()}.
   */
  public SuccessAsyncFuture(T value) {
    _value = value;
  }

  /**
   * Returns {@code true} if and only if the I/O operation was completed
   * successfully.
   */
  @Override
  public boolean isSuccess() {
    return true;
  }

  /**
   * Returns the cause of the failed I/O operation if the I/O operation has
   * failed.
   *
   * @return the cause of the failure.
   *         {@code null} if succeeded or this future is not
   *         completed yet.
   */
  @Override
  public Throwable getCause() {
    return null;
  }

  /**
   * Marks this future as a success and notifies all
   * listeners.
   *
   * @return {@code true} if and only if successfully marked this future as
   *         a success. Otherwise {@code false} because this future is
   *         already marked as either a success or a failure.
   */
  @Override
  public boolean setSuccess(T result) {
    return false;
  }

  /**
   * Marks this future as a failure and notifies all
   * listeners.
   *
   * @return {@code true} if and only if successfully marked this future as
   *         a failure. Otherwise {@code false} because this future is
   *         already marked as either a success or a failure.
   */
  @Override
  public boolean setFailure(@Nonnull Throwable cause) {
    return false;
  }

  /**
   * Adds the specified listener to this future.  The
   * specified listener is notified when this future is
   * {@linkplain #isDone() done}.  If this future is already
   * completed, the specified listener is notified immediately.
   * @return {@code this} to permit chaining of operations.
   */
  @Override
  public @Nonnull AsyncPromise<T> addListener(AsyncFutureListener<T> listener) {
    DefaultAsyncFuture.notifyListener(this, listener);
    return this;
  }

  /**
   * Adds the specified future as a listener to this future.  The
   * specified future is notified when this future is
   * {@linkplain #isDone() done}.  If this future is already
   * completed, the specified future is notified immediately.
   * @return {@code this} to permit chaining of operations.
   * @param listener
   */
  @Override
  public @Nonnull AsyncPromise<T> addListener(AsyncPromise<T> listener) {
    listener.setSuccess(_value);
    return this;
  }

  /**
   * Waits for this future to be completed.
   *
   * @throws InterruptedException if the current thread was interrupted
   */
  @Override
  public @Nonnull AsyncFuture<T> await() throws InterruptedException {
    return this;
  }

  /**
   * Waits for this future to be completed without
   * interruption.  This method catches an {@link InterruptedException} and
   * discards it silently.
   */
  @Override
  public @Nonnull AsyncFuture<T> awaitUninterruptibly() {
    return this;
  }

  /**
   * Waits for this future to be completed within the
   * specified time limit.
   *
   * @return {@code true} if and only if the future was completed within
   *         the specified time limit
   * @throws InterruptedException if the current thread was interrupted
   */
  @Override
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    return true;
  }

  /**
   * Waits for this future to be completed within the
   * specified time limit without interruption.  This method catches an
   * {@link InterruptedException} and discards it silently.
   *
   * @return {@code true} if and only if the future was completed within
   *         the specified time limit
   */
  @Override
  public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
    return true;
  }

  /**
   * Attempts to cancel execution of this task.  This attempt will
   * fail if the task has already completed, has already been cancelled,
   * or could not be cancelled for some other reason. If successful,
   * and this task has not started when <tt>cancel</tt> is called,
   * this task should never run.  If the task has already started,
   * then the <tt>mayInterruptIfRunning</tt> parameter determines
   * whether the thread executing this task should be interrupted in
   * an attempt to stop the task.
   * <p/>
   * <p>After this method returns, subsequent calls to {@link #isDone} will
   * always return <tt>true</tt>.  Subsequent calls to {@link #isCancelled}
   * will always return <tt>true</tt> if this method returned <tt>true</tt>.
   *
   * @param mayInterruptIfRunning <tt>true</tt> if the thread executing this
   *                              task should be interrupted; otherwise, in-progress tasks are allowed
   *                              to complete
   * @return <tt>false</tt> if the task could not be cancelled,
   *         typically because it has already completed normally;
   *         <tt>true</tt> otherwise
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  /**
   * Returns <tt>true</tt> if this task was cancelled before it completed
   * normally.
   *
   * @return <tt>true</tt> if this task was cancelled before it completed
   */
  @Override
  public boolean isCancelled() {
    return false;
  }

  /**
   * Returns <tt>true</tt> if this task completed.
   * <p/>
   * Completion may be due to normal termination, an exception, or
   * cancellation -- in all of these cases, this method will return
   * <tt>true</tt>.
   *
   * @return <tt>true</tt> if this task completed
   */
  @Override
  public boolean isDone() {
    return true;
  }

  /**
   * Waits if necessary for the computation to complete, and then
   * retrieves its result.
   *
   * @return the computed result
   * @throws java.util.concurrent.CancellationException
   *                              if the computation was cancelled
   * @throws java.util.concurrent.ExecutionException
   *                              if the computation threw an
   *                              exception
   * @throws InterruptedException if the current thread was interrupted
   *                              while waiting
   */
  @Override
  public T get() throws InterruptedException, ExecutionException {
    return getNow();
  }

  /**
   * Waits if necessary for at most the given time for the computation
   * to complete, and then retrieves its result, if available.
   *
   * @param timeout the maximum time to wait
   * @param unit    the time unit of the timeout argument
   * @return the computed result
   * @throws java.util.concurrent.CancellationException
   *                              if the computation was cancelled
   * @throws java.util.concurrent.ExecutionException
   *                              if the computation threw an
   *                              exception
   * @throws InterruptedException if the current thread was interrupted
   *                              while waiting
   * @throws java.util.concurrent.TimeoutException
   *                              if the wait timed out
   */
  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return getNow();
  }

  @Override
  public T getNow() {
    return _value;
  }

  @Override
  public CompletableFuture<T> toCompletableFuture() {
    return CompletableFuture.completedFuture(getNow());
  }
}
