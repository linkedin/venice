package com.linkedin.alpini.base.concurrency.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * Created by acurtis on 5/1/18.
 */
public abstract class AbstractAsyncFuture<T> implements AsyncFuture<T> {
  @Override
  public <U> CompletionStage<U> thenApply(Function<? super T, ? extends U> fn) {
    return toCompletableFuture().thenApply(fn);
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
    return toCompletableFuture().thenApplyAsync(fn);
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
    return toCompletableFuture().thenApplyAsync(fn, executor);
  }

  @Override
  public CompletionStage<Void> thenAccept(Consumer<? super T> action) {
    return toCompletableFuture().thenAccept(action);
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action) {
    return toCompletableFuture().thenAcceptAsync(action);
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
    return toCompletableFuture().thenAcceptAsync(action, executor);
  }

  @Override
  public CompletionStage<Void> thenRun(Runnable action) {
    return toCompletableFuture().thenRun(action);
  }

  @Override
  public CompletionStage<Void> thenRunAsync(Runnable action) {
    return toCompletableFuture().thenRunAsync(action);
  }

  @Override
  public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
    return toCompletableFuture().thenRunAsync(action, executor);
  }

  @Override
  public <U, V> CompletionStage<V> thenCombine(
      CompletionStage<? extends U> other,
      BiFunction<? super T, ? super U, ? extends V> fn) {
    return toCompletableFuture().thenCombine(other, fn);
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(
      CompletionStage<? extends U> other,
      BiFunction<? super T, ? super U, ? extends V> fn) {
    return toCompletableFuture().thenCombineAsync(other, fn);
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(
      CompletionStage<? extends U> other,
      BiFunction<? super T, ? super U, ? extends V> fn,
      Executor executor) {
    return toCompletableFuture().thenCombineAsync(other, fn, executor);
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBoth(
      CompletionStage<? extends U> other,
      BiConsumer<? super T, ? super U> action) {
    return toCompletableFuture().thenAcceptBoth(other, action);
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(
      CompletionStage<? extends U> other,
      BiConsumer<? super T, ? super U> action) {
    return toCompletableFuture().thenAcceptBothAsync(other, action);
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(
      CompletionStage<? extends U> other,
      BiConsumer<? super T, ? super U> action,
      Executor executor) {
    return toCompletableFuture().thenAcceptBothAsync(other, action, executor);
  }

  @Override
  public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
    return toCompletableFuture().runAfterBoth(other, action);
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
    return toCompletableFuture().runAfterBothAsync(other, action);
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
    return toCompletableFuture().runAfterBothAsync(other, action, executor);
  }

  @Override
  public <U> CompletionStage<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return toCompletableFuture().applyToEither(other, fn);
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return toCompletableFuture().applyToEitherAsync(other, fn);
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(
      CompletionStage<? extends T> other,
      Function<? super T, U> fn,
      Executor executor) {
    return toCompletableFuture().applyToEitherAsync(other, fn, executor);
  }

  @Override
  public CompletionStage<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
    return toCompletableFuture().acceptEither(other, action);
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
    return toCompletableFuture().acceptEitherAsync(other, action);
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(
      CompletionStage<? extends T> other,
      Consumer<? super T> action,
      Executor executor) {
    return toCompletableFuture().acceptEitherAsync(other, action, executor);
  }

  @Override
  public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
    return toCompletableFuture().runAfterEither(other, action);
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
    return toCompletableFuture().runAfterEitherAsync(other, action);
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
    return toCompletableFuture().runAfterEitherAsync(other, action, executor);
  }

  @Override
  public <U> CompletionStage<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
    return toCompletableFuture().thenCompose(fn);
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
    return toCompletableFuture().thenComposeAsync(fn);
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      Function<? super T, ? extends CompletionStage<U>> fn,
      Executor executor) {
    return toCompletableFuture().thenComposeAsync(fn, executor);
  }

  @Override
  public CompletionStage<T> exceptionally(Function<Throwable, ? extends T> fn) {
    return toCompletableFuture().exceptionally(fn);
  }

  @Override
  public CompletionStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
    return toCompletableFuture().whenComplete(action);
  }

  @Override
  public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
    return toCompletableFuture().whenCompleteAsync(action);
  }

  @Override
  public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
    return toCompletableFuture().whenCompleteAsync(action, executor);
  }

  @Override
  public <U> CompletionStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
    return toCompletableFuture().handle(fn);
  }

  @Override
  public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
    return toCompletableFuture().handleAsync(fn);
  }

  @Override
  public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
    return toCompletableFuture().handleAsync(fn, executor);
  }

}
