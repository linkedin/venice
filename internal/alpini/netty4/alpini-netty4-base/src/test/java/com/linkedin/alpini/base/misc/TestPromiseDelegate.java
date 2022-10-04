package com.linkedin.alpini.base.misc;

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


@SuppressWarnings("unchecked")
@Test(groups = "unit")
public class TestPromiseDelegate {
  public interface ObjectPromise extends Promise<Object> {
  }

  public interface ObjectFutureListener extends FutureListener<Object> {
  }

  public void testSetSuccess() {
    Promise<Object> promise = Mockito.spy(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));
    Promise<Object> delegate = new PromiseDelegate<>(promise);

    Assert.assertSame(delegate.setSuccess(1), delegate);
    Assert.assertThrows(IllegalStateException.class, () -> delegate.setSuccess(2));

    Mockito.verify(promise).trySuccess(1);
    Mockito.verify(promise).trySuccess(2);
    Mockito.verifyNoMoreInteractions(promise);
    Assert.assertEquals(delegate.getNow(), 1);
  }

  public void testTrySuccess() {

    Promise<Object> promise = Mockito.spy(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertTrue(delegate.trySuccess(1));
    Assert.assertFalse(delegate.trySuccess(2));

    Mockito.verify(promise).trySuccess(1);
    Mockito.verify(promise).trySuccess(2);
    Mockito.verifyNoMoreInteractions(promise);
    Assert.assertEquals(delegate.getNow(), 1);
  }

  public void testSetFailure() {
    class TestException extends Exception {
    }

    Promise<Object> promise = Mockito.spy(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertSame(delegate.setFailure(new TestException()), delegate);
    Assert.assertThrows(IllegalStateException.class, () -> delegate.setFailure(new NullPointerException()));

    Mockito.verify(promise).tryFailure(Mockito.any(TestException.class));
    Mockito.verify(promise).tryFailure(Mockito.any(NullPointerException.class));
    Mockito.verifyNoMoreInteractions(promise);
    Assert.assertTrue(delegate.cause() instanceof TestException);
  }

  public void testTryFailure() {
    class TestException extends Exception {
    }

    Promise<Object> promise = Mockito.spy(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertTrue(delegate.tryFailure(new TestException()));
    Assert.assertFalse(delegate.tryFailure(new NullPointerException()));

    Mockito.verify(promise).tryFailure(Mockito.any(TestException.class));
    Mockito.verify(promise).tryFailure(Mockito.any(NullPointerException.class));
    Mockito.verifyNoMoreInteractions(promise);
    Assert.assertTrue(delegate.cause() instanceof TestException);
  }

  public void testSetUncancellable() {
    Promise<Object> promise = Mockito.spy(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertTrue(delegate.setUncancellable());
    Mockito.verify(promise).setUncancellable();
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testIsSuccess() {
    Promise<Object> promise = Mockito.spy(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertFalse(delegate.isSuccess());
    Mockito.verify(promise).isSuccess();
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testIsCancellable() {
    Promise<Object> promise = Mockito.spy(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertTrue(delegate.isCancellable());
    Mockito.verify(promise).isCancellable();
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testAddListener() {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.addListener(Mockito.any())).thenReturn(promise);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    FutureListener<Object> listener = Mockito.mock(ObjectFutureListener.class);
    Assert.assertSame(delegate.addListener(listener), delegate);
    Mockito.verify(promise).addListener(listener);
    Mockito.verifyNoMoreInteractions(promise, listener);
  }

  public void testAddListeners() {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.addListeners(Mockito.any())).thenReturn(promise);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    FutureListener<Object> listener1 = Mockito.mock(ObjectFutureListener.class);
    FutureListener<Object> listener2 = Mockito.mock(ObjectFutureListener.class);
    Assert.assertSame(delegate.addListeners(listener1, listener2), delegate);
    Mockito.verify(promise).addListeners(listener1, listener2);
    Mockito.verifyNoMoreInteractions(promise, listener1, listener2);
  }

  public void testRemoveListener() {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.addListener(Mockito.any())).thenReturn(promise);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    FutureListener<Object> listener = Mockito.mock(ObjectFutureListener.class);
    Assert.assertSame(delegate.removeListener(listener), delegate);
    Mockito.verify(promise).removeListener(listener);
    Mockito.verifyNoMoreInteractions(promise, listener);
  }

  public void testRemoveListeners() {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.addListeners(Mockito.any())).thenReturn(promise);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    FutureListener<Object> listener1 = Mockito.mock(ObjectFutureListener.class);
    FutureListener<Object> listener2 = Mockito.mock(ObjectFutureListener.class);
    Assert.assertSame(delegate.removeListeners(listener1, listener2), delegate);
    Mockito.verify(promise).removeListeners(listener1, listener2);
    Mockito.verifyNoMoreInteractions(promise, listener1, listener2);
  }

  public void testAwait() throws InterruptedException {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.await()).thenReturn(promise);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertSame(delegate.await(), delegate);
    Mockito.verify(promise).await();
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testAwaitUninterruptibly() {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.awaitUninterruptibly()).thenReturn(promise);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertSame(delegate.awaitUninterruptibly(), delegate);
    Mockito.verify(promise).awaitUninterruptibly();
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testAwaitTimeUnit() throws InterruptedException {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.await(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(true);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertTrue(delegate.await(42, TimeUnit.NANOSECONDS));
    Mockito.verify(promise).await(42, TimeUnit.NANOSECONDS);
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testAwaitMillis() throws InterruptedException {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.await(Mockito.anyLong())).thenReturn(true);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertTrue(delegate.await(4242L));
    Mockito.verify(promise).await(4242L);
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testAwaitUninterruptiblyTimeUnit() {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.awaitUninterruptibly(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(true);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertTrue(delegate.awaitUninterruptibly(42, TimeUnit.NANOSECONDS));
    Mockito.verify(promise).awaitUninterruptibly(42, TimeUnit.NANOSECONDS);
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testAwaitUninterruptiblyMillis() {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.awaitUninterruptibly(Mockito.anyLong())).thenReturn(true);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertTrue(delegate.awaitUninterruptibly(4242L));
    Mockito.verify(promise).awaitUninterruptibly(4242L);
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testGetNow() {
    Object value = Math.PI;
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.getNow()).thenReturn(value);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertSame(delegate.getNow(), value);
    Mockito.verify(promise).getNow();
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testCancel() {
    Promise<Object> promise = Mockito.spy(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertTrue(delegate.cancel(false));
    Assert.assertFalse(delegate.cancel(false));
    Mockito.verify(promise, Mockito.times(2)).cancel(false);
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testIsCancelled() {
    Promise<Object> promise = Mockito.spy(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertFalse(delegate.isCancelled());
    Assert.assertTrue(promise.cancel(false));
    Assert.assertTrue(delegate.isCancelled());
    Mockito.verify(promise, Mockito.times(2)).isCancelled();
    Mockito.verify(promise).cancel(false);
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testIsDone() {
    Promise<Object> promise = Mockito.spy(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertFalse(delegate.isDone());
    Assert.assertTrue(promise.cancel(false));
    Assert.assertTrue(delegate.isDone());
    Mockito.verify(promise, Mockito.times(2)).isDone();
    Mockito.verify(promise).cancel(false);
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testGet() throws Exception {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.get()).thenReturn(42);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertSame(delegate.get(), 42);
    Mockito.verify(promise).get();
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testGetTimeUnit() throws Exception {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.get(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(42);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertSame(delegate.get(1234, TimeUnit.NANOSECONDS), 42);
    Mockito.verify(promise).get(1234, TimeUnit.NANOSECONDS);
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testSync() throws InterruptedException {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.sync()).thenReturn(promise);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertSame(delegate.sync(), delegate);
    Mockito.verify(promise).sync();
    Mockito.verifyNoMoreInteractions(promise);
  }

  public void testSyncUninterruptibly() {
    Promise<Object> promise = Mockito.mock(ObjectPromise.class);
    Mockito.when(promise.syncUninterruptibly()).thenReturn(promise);
    Promise<Object> delegate = new PromiseDelegate<>(promise);
    Assert.assertSame(delegate.syncUninterruptibly(), delegate);
    Mockito.verify(promise).syncUninterruptibly();
    Mockito.verifyNoMoreInteractions(promise);
  }
}
