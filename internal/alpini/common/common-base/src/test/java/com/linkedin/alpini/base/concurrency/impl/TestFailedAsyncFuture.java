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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestFailedAsyncFuture {
  @DataProvider
  public Object[][] instancesToTest() {
    return new Object[][] { { new CancelledAsyncFuture<Void>(), CancellationException.class },
        { new FailedAsyncFuture<Void>(new RuntimeException()), RuntimeException.class } };
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testIsDone(AsyncFuture<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isSuccess());
    Assert.assertEquals(future.isCancelled(), exClass == CancellationException.class);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testGetCause(AsyncFuture<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Assert.assertNotNull(exClass.cast(future.getCause()));
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testGetThrows(AsyncFuture<Void> future, Class<? extends Throwable> exClass) throws Exception {
    try {
      future.get();
      Assert.fail();
    } catch (ExecutionException ex) {
      Assert.assertNotNull(exClass.cast(ex.getCause()));
    }

    try {
      future.get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      Assert.fail();
    } catch (ExecutionException ex) {
      Assert.assertNotNull(exClass.cast(ex.getCause()));
    }
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testAwait(AsyncFuture<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Assert.assertTrue(future.await(Long.MAX_VALUE, TimeUnit.MILLISECONDS));
    Assert.assertTrue(future.awaitUninterruptibly(Long.MAX_VALUE, TimeUnit.MILLISECONDS));
    Assert.assertSame(future.await(), future);
    Assert.assertSame(future.awaitUninterruptibly(), future);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testCancelFails(AsyncFuture<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Assert.assertFalse(future.cancel(false));
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testSetSuccessFails(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Assert.assertFalse(future.setSuccess(null));
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testSetFailureFails(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Assert.assertFalse(future.setFailure(new NullPointerException()));
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testGetNow(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Assert.assertNull(future.getNow());
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testThenApply(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    UnaryOperator<Void> operator = Mockito.mock(UnaryOperatorVoid.class);
    Executor executor = Mockito.mock(Executor.class);
    Assert.assertSame(future.thenApply(operator), future);
    Assert.assertSame(future.thenApplyAsync(operator), future);
    Assert.assertSame(future.thenApplyAsync(operator, executor), future);
    Mockito.verifyNoMoreInteractions(operator, executor);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testThenAccept(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Consumer<Void> operator = Mockito.mock(ConsumerVoid.class);
    Executor executor = Mockito.mock(Executor.class);
    Assert.assertSame(future.thenAccept(operator), future);
    Assert.assertSame(future.thenAcceptAsync(operator), future);
    Assert.assertSame(future.thenAcceptAsync(operator, executor), future);
    Mockito.verifyNoMoreInteractions(operator, executor);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testThenRun(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Runnable action = Mockito.mock(Runnable.class);
    Executor executor = Mockito.mock(Executor.class);
    Assert.assertSame(future.thenRun(action), future);
    Assert.assertSame(future.thenRunAsync(action), future);
    Assert.assertSame(future.thenRunAsync(action, executor), future);
    Mockito.verifyNoMoreInteractions(action, executor);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testThenCombine(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    AsyncFuture<Void> other = Mockito.mock(AsyncPromiseVoid.class);
    BiFunction<Void, Void, Void> combine = Mockito.mock(BiFunctionVoid.class);
    Executor executor = Mockito.mock(Executor.class);
    Assert.assertSame(future.thenCombine(other, combine), future);
    Assert.assertSame(future.thenCombineAsync(other, combine), future);
    Assert.assertSame(future.thenCombineAsync(other, combine, executor), future);
    Mockito.verifyNoMoreInteractions(other, combine, executor);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testThenAcceptBoth(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    AsyncFuture<Void> other = Mockito.mock(AsyncPromiseVoid.class);
    BiConsumer<Void, Void> combine = Mockito.mock(BiConsumerVoid.class);
    Executor executor = Mockito.mock(Executor.class);
    Assert.assertSame(future.thenAcceptBoth(other, combine), future);
    Assert.assertSame(future.thenAcceptBothAsync(other, combine), future);
    Assert.assertSame(future.thenAcceptBothAsync(other, combine, executor), future);
    Mockito.verifyNoMoreInteractions(other, combine, executor);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testRunAfterBoth(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    AsyncFuture<Void> other = Mockito.mock(AsyncPromiseVoid.class);
    Runnable action = Mockito.mock(Runnable.class);
    Executor executor = Mockito.mock(Executor.class);
    Assert.assertSame(future.runAfterBoth(other, action), future);
    Assert.assertSame(future.runAfterBothAsync(other, action), future);
    Assert.assertSame(future.runAfterBothAsync(other, action, executor), future);
    Mockito.verifyNoMoreInteractions(other, action, executor);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testApplyToEither(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    AsyncFuture<Void> other = Mockito.mock(AsyncPromiseVoid.class);
    Function<Void, Void> operator = Mockito.mock(UnaryOperatorVoid.class);
    Executor executor = Mockito.mock(Executor.class);
    Assert.assertSame(future.applyToEither(other, operator), future);
    Assert.assertSame(future.applyToEitherAsync(other, operator), future);
    Assert.assertSame(future.applyToEitherAsync(other, operator, executor), future);
    Mockito.verifyNoMoreInteractions(other, operator, executor);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testAcceptEither(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    AsyncFuture<Void> other = Mockito.mock(AsyncPromiseVoid.class);
    Consumer<Void> consumer = Mockito.mock(ConsumerVoid.class);
    Executor executor = Mockito.mock(Executor.class);
    Assert.assertSame(future.acceptEither(other, consumer), future);
    Assert.assertSame(future.acceptEitherAsync(other, consumer), future);
    Assert.assertSame(future.acceptEitherAsync(other, consumer, executor), future);
    Mockito.verifyNoMoreInteractions(other, consumer, executor);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testRunAfterEither(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    AsyncFuture<Void> other = Mockito.mock(AsyncPromiseVoid.class);
    Runnable action = Mockito.mock(Runnable.class);
    Executor executor = Mockito.mock(Executor.class);
    Assert.assertSame(future.runAfterEither(other, action), future);
    Assert.assertSame(future.runAfterEitherAsync(other, action), future);
    Assert.assertSame(future.runAfterEitherAsync(other, action, executor), future);
    Mockito.verifyNoMoreInteractions(other, action, executor);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testThenCompose(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Function<Void, CompletionStage<Void>> fn = Mockito.mock(ComposeVoid.class);
    Executor executor = Mockito.mock(Executor.class);
    Assert.assertSame(future.thenCompose(fn), future);
    Assert.assertSame(future.thenComposeAsync(fn), future);
    Assert.assertSame(future.thenComposeAsync(fn, executor), future);
    Mockito.verifyNoMoreInteractions(fn, executor);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testHandle(AsyncPromise<Void> future, Class<? extends Throwable> exClass) throws Exception {
    Predicate<CompletionStage<Throwable>> test =
        stage -> exClass.isInstance(CompletableFuture.completedFuture(stage).thenCompose(Function.identity()).join());
    BiFunction<Void, Throwable, Throwable> fn = (aVoid, throwable) -> throwable;

    Assert.assertTrue(test.test(future.handle(fn)));
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testAddListenerAsyncFuture(AsyncFuture<Void> future, Class<? extends Throwable> exClass)
      throws Exception {
    AsyncPromise<Void> listener = Mockito.mock(AsyncPromiseVoid.class);
    Assert.assertTrue(future.isDone());
    Assert.assertSame(future.addListener(listener), future);

    if (!CancellationException.class.isAssignableFrom(exClass))
      Mockito.verify(listener).setFailure(future.getCause());

    Mockito.verifyNoMoreInteractions(listener);
  }

  @Test(groups = "unit", dataProvider = "instancesToTest")
  public void testAddListenerAsyncFutureListener(AsyncFuture<Void> future, Class<? extends Throwable> exClass)
      throws Exception {
    AsyncFutureListener<Void> listener = Mockito.mock(AsyncFutureListenerVoid.class);
    Assert.assertTrue(future.isDone());
    Assert.assertSame(future.addListener(listener), future);

    if (!CancellationException.class.isAssignableFrom(exClass))
      Mockito.verify(listener).operationComplete(Mockito.eq(future));

    Mockito.verifyNoMoreInteractions(listener);
  }

  @Test(groups = "unit")
  public void testCancelledStatic() {
    Assert.assertSame(CancelledAsyncFuture.getInstance(), CancelledAsyncFuture.getInstance());
  }

  interface AsyncFutureListenerVoid extends AsyncFutureListener<Void> {
  }

  interface AsyncPromiseVoid extends AsyncPromise<Void> {
  }

  interface ConsumerVoid extends Consumer<Void> {
  }

  interface UnaryOperatorVoid extends UnaryOperator<Void> {
  }

  interface BiFunctionVoid extends BiFunction<Void, Void, Void> {
  }

  interface BiConsumerVoid extends BiConsumer<Void, Void> {
  }

  interface ComposeVoid extends Function<Void, CompletionStage<Void>> {
  };
}
