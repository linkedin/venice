package com.linkedin.alpini.base.concurrency.impl;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.mockito.Mockito;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 5/3/18.
 */
public class TestAbstractAsyncFuture {
  static void verifyNoMoreInteractions(Object... mocks) {
    if (System.getProperty("java.version").startsWith("1.8.")) {
      Mockito.verifyNoMoreInteractions(mocks);
    } else {
      ArrayList<Object> test = new ArrayList<>(mocks.length);
      for (Object mock: mocks) {
        if (mock instanceof CompletableFuture) {
          // Java11 has different internal implementation which renders spy failing due to extra interactions.
          continue;
        }
        test.add(mock);
      }
      Mockito.verifyNoMoreInteractions(test.toArray());
    }
  }

  @Test(groups = "unit")
  public <T> void testThenApply() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    Function<T, ?> fn = Mockito.mock(Function.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenApply(fn);

    Mockito.verify(future).thenApply(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenApply(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenApplyAsync(fn);

    Mockito.verify(future).thenApplyAsync(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenApplyAsync(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenApplyAsync(fn, ex);

    Mockito.verify(future).thenApplyAsync(Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenApplyAsync(Mockito.same(fn), Mockito.same(ex));
    verifyNoMoreInteractions(future, completableFuture, fn, ex);
  }

  @Test(groups = "unit")
  public <T> void testThenAccept() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    Consumer<T> fn = Mockito.mock(Consumer.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenAccept(fn);

    Mockito.verify(future).thenAccept(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenAccept(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenAcceptAsync(fn);

    Mockito.verify(future).thenAcceptAsync(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenAcceptAsync(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenAcceptAsync(fn, ex);

    Mockito.verify(future).thenAcceptAsync(Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenAcceptAsync(Mockito.same(fn), Mockito.same(ex));
    verifyNoMoreInteractions(future, completableFuture, fn, ex);
  }

  @Test(groups = "unit")
  public <T> void testThenRun() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    Runnable fn = Mockito.mock(Runnable.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenRun(fn);

    Mockito.verify(future).thenRun(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenRun(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenRunAsync(fn);

    Mockito.verify(future).thenRunAsync(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenRunAsync(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenRunAsync(fn, ex);

    Mockito.verify(future).thenRunAsync(Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenRunAsync(Mockito.same(fn), Mockito.same(ex));
    verifyNoMoreInteractions(future, completableFuture, fn, ex);
  }

  @Test(groups = "unit")
  public <T, U> void testThenCombine() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    CompletionStage<U> stage = Mockito.spy(CompletableFuture.class);
    BiFunction<T, U, T> fn = Mockito.mock(BiFunction.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenCombine(stage, fn);

    Mockito.verify(future).thenCombine(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenCombine(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenCombineAsync(stage, fn);

    Mockito.verify(future).thenCombineAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenCombineAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenCombineAsync(stage, fn, ex);

    Mockito.verify(future).thenCombineAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenCombineAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn, ex);

  }

  @Test(groups = "unit")
  public <T, U> void testThenAcceptBoth() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    CompletionStage<U> stage = Mockito.spy(CompletableFuture.class);
    BiConsumer<T, U> fn = Mockito.mock(BiConsumer.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenAcceptBoth(stage, fn);

    Mockito.verify(future).thenAcceptBoth(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenAcceptBoth(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenAcceptBothAsync(stage, fn);

    Mockito.verify(future).thenAcceptBothAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenAcceptBothAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenAcceptBothAsync(stage, fn, ex);

    Mockito.verify(future).thenAcceptBothAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenAcceptBothAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn, ex);
  }

  @Test(groups = "unit")
  public <T, U> void testRunAfterBoth() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    CompletionStage<U> stage = Mockito.spy(CompletableFuture.class);
    Runnable fn = Mockito.mock(Runnable.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.runAfterBoth(stage, fn);

    Mockito.verify(future).runAfterBoth(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).runAfterBoth(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.runAfterBothAsync(stage, fn);

    Mockito.verify(future).runAfterBothAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).runAfterBothAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.runAfterBothAsync(stage, fn, ex);

    Mockito.verify(future).runAfterBothAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).runAfterBothAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn, ex);
  }

  @Test(groups = "unit")
  public <T, U> void testApplyToEither() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    CompletionStage<T> stage = Mockito.spy(CompletableFuture.class);
    Function<T, U> fn = Mockito.mock(Function.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.applyToEither(stage, fn);

    Mockito.verify(future).applyToEither(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).applyToEither(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.applyToEitherAsync(stage, fn);

    Mockito.verify(future).applyToEitherAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).applyToEitherAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.applyToEitherAsync(stage, fn, ex);

    Mockito.verify(future).applyToEitherAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).applyToEitherAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn, ex);
  }

  @Test(groups = "unit")
  public <T> void testAcceptEither() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    CompletionStage<T> stage = Mockito.spy(CompletableFuture.class);
    Consumer<T> fn = Mockito.mock(Consumer.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.acceptEither(stage, fn);

    Mockito.verify(future).acceptEither(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).acceptEither(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.acceptEitherAsync(stage, fn);

    Mockito.verify(future).acceptEitherAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).acceptEitherAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.acceptEitherAsync(stage, fn, ex);

    Mockito.verify(future).acceptEitherAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).acceptEitherAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn, ex);
  }

  @Test(groups = "unit")
  public <T> void testRunAfterEither() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    CompletionStage<?> stage = Mockito.spy(CompletableFuture.class);
    Runnable fn = Mockito.mock(Runnable.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.runAfterEither(stage, fn);

    Mockito.verify(future).runAfterEither(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).runAfterEither(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.runAfterEitherAsync(stage, fn);

    Mockito.verify(future).runAfterEitherAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).runAfterEitherAsync(Mockito.same(stage), Mockito.same(fn));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn);

    Mockito.reset(future, stage, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.runAfterEitherAsync(stage, fn, ex);

    Mockito.verify(future).runAfterEitherAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).runAfterEitherAsync(Mockito.same(stage), Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(stage).toCompletableFuture();
    verifyNoMoreInteractions(future, stage, completableFuture, fn, ex);
  }

  @Test(groups = "unit")
  public <T, U> void testThenCompose() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    Function<T, CompletionStage<U>> fn = Mockito.mock(Function.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenCompose(fn);

    Mockito.verify(future).thenCompose(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenCompose(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenComposeAsync(fn);

    Mockito.verify(future).thenComposeAsync(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenComposeAsync(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.thenComposeAsync(fn, ex);

    Mockito.verify(future).thenComposeAsync(Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).thenComposeAsync(Mockito.same(fn), Mockito.same(ex));
    verifyNoMoreInteractions(future, completableFuture, fn, ex);
  }

  @Test(groups = "unit")
  public <T> void testExceptionally() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    Function<Throwable, T> fn = Mockito.mock(Function.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.exceptionally(fn);

    Mockito.verify(future).exceptionally(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).exceptionally(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);
  }

  @Test(groups = "unit")
  public <T> void testWhenComplete() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    BiConsumer<T, Throwable> fn = Mockito.mock(BiConsumer.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.whenComplete(fn);

    Mockito.verify(future).whenComplete(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).whenComplete(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.whenCompleteAsync(fn);

    Mockito.verify(future).whenCompleteAsync(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).whenCompleteAsync(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.whenCompleteAsync(fn, ex);

    Mockito.verify(future).whenCompleteAsync(Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).whenCompleteAsync(Mockito.same(fn), Mockito.same(ex));
    verifyNoMoreInteractions(future, completableFuture, fn, ex);
  }

  @Test(groups = "unit")
  public <T, U> void testHandle() {
    AbstractAsyncFuture<T> future = Mockito.spy(AbstractAsyncFuture.class);
    CompletableFuture<T> completableFuture = Mockito.spy(CompletableFuture.class);
    BiFunction<T, Throwable, U> fn = Mockito.mock(BiFunction.class);
    Executor ex = Mockito.mock(Executor.class);

    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.handle(fn);

    Mockito.verify(future).handle(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).handle(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.handleAsync(fn);

    Mockito.verify(future).handleAsync(Mockito.same(fn));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).handleAsync(Mockito.same(fn));
    verifyNoMoreInteractions(future, completableFuture, fn);

    Mockito.reset(future, completableFuture);
    Mockito.when(future.toCompletableFuture()).thenReturn(completableFuture);

    future.handleAsync(fn, ex);

    Mockito.verify(future).handleAsync(Mockito.same(fn), Mockito.same(ex));
    Mockito.verify(future).toCompletableFuture();
    Mockito.verify(completableFuture).handleAsync(Mockito.same(fn), Mockito.same(ex));
    verifyNoMoreInteractions(future, completableFuture, fn, ex);
  }
}
