package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.ExceptionUtil;
import com.linkedin.alpini.base.misc.Time;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestFutures {
  @Test
  public void testAsCompletableFuture() {
    Promise<Integer> promise = ImmediateEventExecutor.INSTANCE.newPromise();

    CompletableFuture<Integer> future = Futures.asCompletableFuture(promise);

    Assert.assertFalse(future.isDone());

    promise.setSuccess(42);
    Assert.assertEquals(future.join(), (Integer) 42);

    Future<Integer> future1 = ImmediateEventExecutor.INSTANCE.newSucceededFuture(42);

    future = Futures.asCompletableFuture(future1);

    Assert.assertTrue(future.isDone());
    Assert.assertEquals(future.join(), (Integer) 42);

    promise = ImmediateEventExecutor.INSTANCE.newPromise();
    promise.cancel(false);

    future = Futures.asCompletableFuture(promise);

    Assert.assertTrue(future.isDone());
    Assert.assertTrue(future.isCancelled());

    promise = ImmediateEventExecutor.INSTANCE.newPromise();
    future = Futures.asCompletableFuture(promise);

    future.cancel(false);

    Assert.assertTrue(promise.isDone());
    Assert.assertTrue(promise.isCancelled());
  }

  @Test
  public void testAsNettyFuture() throws InterruptedException {
    CompletableFuture<Integer> future1 = new CompletableFuture<>();

    Future<Integer> future = Futures.asNettyFuture(future1);

    Assert.assertFalse(future.isDone());

    future1.complete(42);
    Assert.assertEquals(future.getNow(), (Integer) 42);

    future1 = new CompletableFuture<>();
    future1.cancel(false);

    future = Futures.asNettyFuture(future1);

    Assert.assertTrue(future.isDone());
    Assert.assertTrue(future.isCancelled());

    future1 = new CompletableFuture<>();

    future = Futures.asNettyFuture(future1);
    future.cancel(false);

    Assert.assertTrue(future1.isDone());
    Assert.assertTrue(future1.isCancelled());

    CompletableFuture<Integer> future2 = new CompletableFuture<>();
    future = Futures.asNettyFuture(future2);

    Executors.newSingleThreadExecutor().submit(() -> {
      Time.sleep(100);
      return future2.complete(42);
    });
    Assert.assertFalse(future.isDone());
    Assert.assertEquals(future.await().getNow(), (Integer) 42);
  }

  @Test
  public void testAllOf1() {
    Promise<Integer> promise1 = ImmediateEventExecutor.INSTANCE.newPromise();
    Promise<Integer> promise2 = ImmediateEventExecutor.INSTANCE.newPromise();

    Future<?> future = Futures.allOf(promise1, promise2);

    Assert.assertFalse(future.isDone());
    promise1.setSuccess(42);
    Assert.assertFalse(future.isDone());
    promise2.setSuccess(34);
    Assert.assertTrue(future.isDone());
  }

  @Test
  public void testAnyOf1() {
    Promise<Integer> promise1 = ImmediateEventExecutor.INSTANCE.newPromise();
    Promise<Integer> promise2 = ImmediateEventExecutor.INSTANCE.newPromise();

    Future<?> future = Futures.anyOf(promise1, promise2);

    Assert.assertFalse(future.isDone());

    promise1.setSuccess(42);
    promise2.setSuccess(34);

    Assert.assertEquals(future.getNow(), 42);
  }

  @Test
  public void testAnyOf2() {
    Promise<Integer> promise1 = ImmediateEventExecutor.INSTANCE.newPromise();
    Promise<Integer> promise2 = ImmediateEventExecutor.INSTANCE.newPromise();

    Future<?> future = Futures.anyOf(promise1, promise2);

    Assert.assertFalse(future.isDone());

    promise2.setSuccess(42);
    promise1.setSuccess(34);

    Assert.assertEquals(future.getNow(), 42);
  }

  @Test
  public void testComplete() throws Exception {
    Promise<Integer> promise = Mockito.mock(Promise.class);

    BiConsumer<Integer, Throwable> complete = Futures.complete(promise);

    CompletableFuture<Integer> test1 = new CompletableFuture<>();

    Assert.assertFalse(test1.whenComplete(complete).isDone());

    Mockito.verifyNoMoreInteractions(promise);

    test1.complete(42);

    Mockito.verify(promise).trySuccess(42);

    Mockito.verifyNoMoreInteractions(promise);

    Mockito.reset(promise);

    CompletableFuture<Integer> test2 = new CompletableFuture<>();

    Assert.assertFalse(test2.whenComplete(complete).isDone());

    Mockito.verifyNoMoreInteractions(promise);

    Exception ex = ExceptionUtil.withoutStackTrace(new TestException());

    test2.completeExceptionally(ex);

    Mockito.verify(promise).tryFailure(ex);
    Mockito.verify(promise).isCancelled();
    Mockito.verifyNoMoreInteractions(promise);
  }

  @Test
  public void testCompleteFuture() throws Exception {
    Promise<Integer> promise = Mockito.mock(Promise.class);
    Future<Integer> future = Mockito.mock(Future.class);

    BiConsumer<Future<Integer>, Throwable> complete = Futures.completeFuture(promise);

    CompletableFuture<Future<Integer>> test1 = new CompletableFuture<>();

    Assert.assertFalse(test1.whenComplete(complete).isDone());

    Mockito.verifyNoMoreInteractions(promise);

    test1.complete(future);

    Mockito.verifyNoMoreInteractions(promise);

    ArgumentCaptor<FutureListener<Integer>> listener1 = (ArgumentCaptor) ArgumentCaptor.forClass(FutureListener.class);

    Mockito.verify(future).addListener(listener1.capture());

    Mockito.verifyNoMoreInteractions(future);

    Mockito.when(future.isSuccess()).thenReturn(true);
    Mockito.when(future.getNow()).thenReturn(42);

    listener1.getValue().operationComplete(future);

    Mockito.verify(future).isSuccess();
    Mockito.verify(future).getNow();
    Mockito.verify(promise).trySuccess(42);

    Mockito.verifyNoMoreInteractions(future, promise);

    Mockito.reset(promise, future);

    CompletableFuture<Future<Integer>> test2 = new CompletableFuture<>();

    Assert.assertFalse(test2.whenComplete(complete).isDone());

    Mockito.verifyNoMoreInteractions(promise);

    test2.complete(future);

    Mockito.verifyNoMoreInteractions(promise);

    ArgumentCaptor<FutureListener<Integer>> listener2 = (ArgumentCaptor) ArgumentCaptor.forClass(FutureListener.class);

    Mockito.verify(future).addListener(listener2.capture());

    Mockito.verifyNoMoreInteractions(future);

    Exception ex = ExceptionUtil.withoutStackTrace(new TestException());

    Mockito.when(future.cause()).thenReturn(ex);

    listener2.getValue().operationComplete(future);

    Mockito.verify(future).isSuccess();
    Mockito.verify(future, Mockito.times(2)).cause();
    Mockito.verify(promise).tryFailure(ex);
    Mockito.verify(promise).isCancelled();

    Mockito.verifyNoMoreInteractions(future, promise);

    Mockito.reset(promise, future);

    CompletableFuture<Future<Integer>> test3 = new CompletableFuture<>();

    Assert.assertFalse(test3.whenComplete(complete).isDone());

    Mockito.verifyNoMoreInteractions(promise);

    ex = ExceptionUtil.withoutStackTrace(new UnsupportedOperationException());

    test3.completeExceptionally(ex);

    Mockito.verify(promise).tryFailure(ex);
    Mockito.verify(promise).isCancelled();

    Mockito.verifyNoMoreInteractions(future, promise);
  }

  @Test
  public void testVoidListenerPromise() throws Exception {
    Promise<Void> future = Mockito.mock(Promise.class);

    FutureListener<Integer> listener = Futures.voidListener(future);

    Future<Integer> mockFuture = Mockito.mock(Future.class);

    Mockito.when(mockFuture.isSuccess()).thenReturn(true);
    Mockito.when(mockFuture.getNow()).thenReturn(42);

    listener.operationComplete(mockFuture);

    Mockito.verify(future).trySuccess(Mockito.isNull(Void.class));
    Mockito.verifyNoMoreInteractions(future);

    Mockito.reset(future, mockFuture);

    Exception fail = ExceptionUtil.withoutStackTrace(new TestException());
    Mockito.when(mockFuture.cause()).thenReturn(fail);

    listener.operationComplete(mockFuture);

    Mockito.verify(future).tryFailure(fail);
    Mockito.verify(future).isCancelled();
    Mockito.verifyNoMoreInteractions(future);
  }

  @Test
  public void testListenerPromise() throws Exception {
    Promise<Integer> future = Mockito.mock(Promise.class);

    FutureListener<Integer> listener = Futures.listener(future);

    Future<Integer> mockFuture = Mockito.mock(Future.class);

    Mockito.when(mockFuture.isSuccess()).thenReturn(true);
    Mockito.when(mockFuture.getNow()).thenReturn(42);

    listener.operationComplete(mockFuture);

    Mockito.verify(future).trySuccess(Mockito.eq(42));
    Mockito.verifyNoMoreInteractions(future);

    Mockito.reset(future, mockFuture);

    Exception fail = ExceptionUtil.withoutStackTrace(new TestException());
    Mockito.when(mockFuture.cause()).thenReturn(fail);

    listener.operationComplete(mockFuture);

    Mockito.verify(future).tryFailure(fail);
    Mockito.verify(future).isCancelled();
    Mockito.verifyNoMoreInteractions(future);
  }

  @Test
  public void testListenerCompletableFuture() throws Exception {
    CompletableFuture<Integer> future = Mockito.mock(CompletableFuture.class);

    FutureListener<Integer> listener = Futures.listener(future);

    Future<Integer> mockFuture = Mockito.mock(Future.class);

    Mockito.when(mockFuture.isSuccess()).thenReturn(true);
    Mockito.when(mockFuture.getNow()).thenReturn(42);

    listener.operationComplete(mockFuture);

    Mockito.verify(future).complete(Mockito.eq(42));
    Mockito.verifyNoMoreInteractions(future);

    Mockito.reset(future, mockFuture);

    Exception fail = ExceptionUtil.withoutStackTrace(new TestException());
    Mockito.when(mockFuture.cause()).thenReturn(fail);

    listener.operationComplete(mockFuture);

    Mockito.verify(future).completeExceptionally(fail);
    Mockito.verify(future).isCancelled();
    Mockito.verifyNoMoreInteractions(future);
  }

  private static class TestException extends Exception {
  }
}
