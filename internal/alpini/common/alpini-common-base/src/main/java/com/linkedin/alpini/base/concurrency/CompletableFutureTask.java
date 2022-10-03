package com.linkedin.alpini.base.concurrency;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 4/4/17.
 */
public class CompletableFutureTask<V> extends FutureTask<V> implements CompletionStage<V> {
  private final CompletableFuture<V> _completableFuture = new InnerFuture<>(this);

  private static class InnerFuture<V> extends CompletableFuture<V> {
    private final CompletableFutureTask<V> _outer;

    private InnerFuture(CompletableFutureTask<V> outer) {
      _outer = outer;
    }

    @Override
    public boolean complete(V value) {
      if (_outer.isSuperDone() || isDone()) {
        return super.complete(value);
      } else {
        _outer.superSet(value);
        return isDone() && !isCompletedExceptionally() && join() == value;
      }
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
      if (_outer.isSuperDone() || isDone()) {
        return super.completeExceptionally(ex);
      } else {
        _outer.superSetException(ex);
        return isCompletedExceptionally();
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (_outer.isSuperCancelled()) {
        return super.cancel(mayInterruptIfRunning);
      } else {
        return _outer.superCancel(mayInterruptIfRunning);
      }
    }
  }

  private boolean isSuperDone() {
    return super.isDone();
  }

  private boolean isSuperCancelled() {
    return super.isCancelled();
  }

  private boolean superCancel(boolean mayInterruptIfRunning) {
    return super.cancel(mayInterruptIfRunning);
  }

  private void superSet(V value) {
    super.set(value);
  }

  private void superSetException(Throwable throwable) {
    super.setException(throwable);
  }

  /**
   * Creates a {@code CompletableFutureTask} that will, upon running, execute the
   * given {@code Callable}.
   *
   * @param  callable the callable task
   * @throws NullPointerException if the callable is null
   */
  public CompletableFutureTask(Callable<V> callable) {
    super(callable);
  }

  /**
   * Creates a {@code CompletableFutureTask} that will, upon running, execute the
   * given {@code Runnable}, and arrange that {@code get} will return the
   * given result on successful completion.
   *
   * @param runnable the runnable task
   * @param result the result to return on successful completion. If
   * you don't need a particular result, consider using
   * constructions of the form:
   * {@code CompletionStage<?> f = new CompletableFutureTask<Void>(runnable, null)}
   * @throws NullPointerException if the runnable is null
   */
  public CompletableFutureTask(Runnable runnable, V result) {
    super(runnable, result);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isCancelled() {
    return _completableFuture.isCancelled();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isDone() {
    return _completableFuture.isDone();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return _completableFuture.cancel(mayInterruptIfRunning);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get() throws InterruptedException, ExecutionException {
    return _completableFuture.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return _completableFuture.get(timeout, unit);
  }

  /**
   * Use the methods provided by the {@link CompletionStage} interface to perform actions
   * after the completion of the task. This method is {@literal final} and may not be overridden.
   */
  @Override
  protected final void done() {
    boolean interrupted = Thread.interrupted();
    while (!_completableFuture.isDone()) {
      try {
        if (super.isCancelled()) {
          _completableFuture.cancel(false);
        } else {
          _completableFuture.complete(super.get());
        }
      } catch (InterruptedException e) {
        interrupted = true;
      } catch (ExecutionException e) {
        _completableFuture.completeExceptionally(e.getCause());
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void set(V v) {
    if (!_completableFuture.isDone()) {
      super.set(v);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void setException(Throwable t) {
    if (!_completableFuture.isDone()) {
      super.setException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> thenApply(Function<? super V, ? extends U> fn) {
    return _completableFuture.thenApply(fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
    return _completableFuture.thenApplyAsync(fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
    return _completableFuture.thenApplyAsync(fn, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> thenAccept(Consumer<? super V> action) {
    return _completableFuture.thenAccept(action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action) {
    return _completableFuture.thenAcceptAsync(action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
    return _completableFuture.thenAcceptAsync(action, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> thenRun(Runnable action) {
    return _completableFuture.thenRun(action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> thenRunAsync(Runnable action) {
    return _completableFuture.thenRunAsync(action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
    return _completableFuture.thenRunAsync(action, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U, W> CompletionStage<W> thenCombine(
      CompletionStage<? extends U> other,
      BiFunction<? super V, ? super U, ? extends W> fn) {
    return _completableFuture.thenCombine(other, fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U, W> CompletionStage<W> thenCombineAsync(
      CompletionStage<? extends U> other,
      BiFunction<? super V, ? super U, ? extends W> fn) {
    return _completableFuture.thenCombineAsync(other, fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U, W> CompletionStage<W> thenCombineAsync(
      CompletionStage<? extends U> other,
      BiFunction<? super V, ? super U, ? extends W> fn,
      Executor executor) {
    return _completableFuture.thenCombineAsync(other, fn, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<Void> thenAcceptBoth(
      CompletionStage<? extends U> other,
      BiConsumer<? super V, ? super U> action) {
    return _completableFuture.thenAcceptBoth(other, action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(
      CompletionStage<? extends U> other,
      BiConsumer<? super V, ? super U> action) {
    return _completableFuture.thenAcceptBothAsync(other, action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(
      CompletionStage<? extends U> other,
      BiConsumer<? super V, ? super U> action,
      Executor executor) {
    return _completableFuture.thenAcceptBothAsync(other, action, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
    return _completableFuture.runAfterBoth(other, action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
    return _completableFuture.runAfterBothAsync(other, action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
    return _completableFuture.runAfterBothAsync(other, action, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
    return _completableFuture.applyToEither(other, fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
    return _completableFuture.applyToEitherAsync(other, fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> applyToEitherAsync(
      CompletionStage<? extends V> other,
      Function<? super V, U> fn,
      Executor executor) {
    return _completableFuture.applyToEitherAsync(other, fn, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
    return _completableFuture.acceptEither(other, action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
    return _completableFuture.acceptEitherAsync(other, action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> acceptEitherAsync(
      CompletionStage<? extends V> other,
      Consumer<? super V> action,
      Executor executor) {
    return _completableFuture.acceptEitherAsync(other, action, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
    return _completableFuture.runAfterEither(other, action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
    return _completableFuture.runAfterEitherAsync(other, action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
    return _completableFuture.runAfterEitherAsync(other, action, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
    return _completableFuture.thenCompose(fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
    return _completableFuture.thenComposeAsync(fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      Function<? super V, ? extends CompletionStage<U>> fn,
      Executor executor) {
    return _completableFuture.thenComposeAsync(fn, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<V> exceptionally(Function<Throwable, ? extends V> fn) {
    return _completableFuture.exceptionally(fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
    return _completableFuture.whenComplete(action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
    return _completableFuture.whenCompleteAsync(action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
    return _completableFuture.whenCompleteAsync(action, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
    return _completableFuture.handle(fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
    return _completableFuture.handleAsync(fn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
    return _completableFuture.handleAsync(fn, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<V> toCompletableFuture() {
    return _completableFuture.toCompletableFuture();
  }

  /**
   * Returns a string identifying this CompletableFutureTask, as well as
   * its completion state.  The state, in brackets, contains the
   * String {@code "Completed Normally"} or the String {@code
   * "Completed Exceptionally"}, or the String {@code "Not
   * completed"} followed by the number of CompletableFutures
   * dependent upon its completion, if any.
   *
   * @return a string identifying this CompletableFutureTask, as well as its state
   */
  @Override
  public String toString() {
    return _completableFuture.toString();
  }
}
