package io.netty.bootstrap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/25/18.
 */
public class TestPendingConnectPromise {
  @Test(groups = "unit")
  public void testGlobalExecutor() {
    Assert.assertSame(new PendingConnectPromise(new CompletableFuture<>()).executor(), GlobalEventExecutor.INSTANCE);
  }

  @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
  public void testSetSuccess0() {
    new PendingConnectPromise(new CompletableFuture<>()).setSuccess();
    Assert.fail();
  }

  @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
  public void testSetSuccess1() {
    new PendingConnectPromise(new CompletableFuture<>()).setSuccess(null);
    Assert.fail();
  }

  @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
  public void testTrySuccess() {
    new PendingConnectPromise(new CompletableFuture<>()).trySuccess();
    Assert.fail();
  }

  @Test(groups = "unit")
  public void testSetFailure0() {
    class Cause extends Exception {
    }
    ChannelPromise promise = new PendingConnectPromise(new CompletableFuture<>()).setFailure(new Cause());
    Assert.assertTrue(promise.isDone());
    Assert.assertFalse(promise.isCancelled());
    Assert.assertTrue(promise.cause() instanceof Cause);
  }

  @Test(groups = "unit")
  public void testSetFailure1() {
    class Cause extends Exception {
    }
    ChannelPromise promise = new PendingConnectPromise(new CompletableFuture<>()).setFailure(new Cause());

    try {
      promise.setFailure(new RuntimeException());
      Assert.fail();
    } catch (IllegalStateException ex) {
      // expected
    }
    Assert.assertTrue(promise.isDone());
    Assert.assertFalse(promise.isCancelled());
    Assert.assertTrue(promise.cause() instanceof Cause);
  }

  public static class ErrorCause extends Error {
  }

  @Test(groups = "unit", expectedExceptions = ErrorCause.class)
  public void testSyncUninterruptably() {
    new PendingConnectPromise(new CompletableFuture<>()).setFailure(new ErrorCause()).syncUninterruptibly();
  }

  @Test(groups = "unit")
  public void testAwaitUninterruptably() {
    ChannelPromise promise =
        new PendingConnectPromise(new CompletableFuture<>()).setFailure(new ErrorCause()).awaitUninterruptibly();
    Assert.assertTrue(promise.isDone());
    Assert.assertFalse(promise.isCancelled());
    Assert.assertTrue(promise.cause() instanceof ErrorCause);
  }

  @Test(groups = "unit")
  public void testAddListener() throws Exception {
    CompletableFuture<Channel> future = new CompletableFuture<>();
    ChannelPromise promise = new PendingConnectPromise(future);
    ChannelFutureListener listener = Mockito.mock(ChannelFutureListener.class);
    Assert.assertSame(promise.addListener(listener), promise);
    Mockito.verifyNoMoreInteractions(listener);

    Channel channel = Mockito.mock(Channel.class);
    EventLoop loop = Mockito.mock(EventLoop.class);
    Mockito.when(loop.inEventLoop()).thenReturn(true);
    Mockito.when(channel.eventLoop()).thenReturn(loop);

    Assert.assertTrue(future.complete(channel));
    Mockito.verify(listener).operationComplete(Mockito.same(promise));
  }

  @Test(groups = "unit")
  public void testAddRemoveListeners() throws Exception {
    CompletableFuture<Channel> future = new CompletableFuture<>();
    ChannelPromise promise = new PendingConnectPromise(future);
    ChannelFutureListener listener1 = Mockito.mock(ChannelFutureListener.class);
    ChannelFutureListener listener2 = Mockito.mock(ChannelFutureListener.class);
    ChannelFutureListener listener3 = Mockito.mock(ChannelFutureListener.class);
    ChannelFutureListener listener4 = Mockito.mock(ChannelFutureListener.class);
    Assert.assertSame(promise.addListener(listener1), promise);
    Assert.assertSame(promise.addListeners(listener2, listener3, listener4), promise);
    Mockito.verifyNoMoreInteractions(listener1, listener2, listener3, listener4);

    Assert.assertSame(promise.removeListener(listener3), promise);
    Assert.assertSame(promise.removeListeners(listener4, listener1), promise);

    Channel channel = Mockito.mock(Channel.class);
    EventLoop loop = Mockito.mock(EventLoop.class);
    Mockito.when(loop.inEventLoop()).thenReturn(true);
    Mockito.when(channel.eventLoop()).thenReturn(loop);

    Assert.assertTrue(future.complete(channel));
    Assert.assertSame(promise.syncUninterruptibly(), promise);

    Mockito.verify(listener2).operationComplete(Mockito.same(promise));
    Mockito.verifyNoMoreInteractions(listener1, listener2, listener3, listener4);
  }

  @Test(groups = "unit")
  public void testListenerException() throws Exception {
    CompletableFuture<Channel> future = new CompletableFuture<>();
    ChannelPromise promise = new PendingConnectPromise(future);
    ChannelFutureListener listener = Mockito.mock(ChannelFutureListener.class);
    Assert.assertSame(promise.addListener(listener), promise);
    Mockito.verifyNoMoreInteractions(listener);

    Channel channel = Mockito.mock(Channel.class);
    EventLoop loop = Mockito.mock(EventLoop.class);
    Mockito.when(loop.inEventLoop()).thenReturn(true);
    Mockito.when(channel.eventLoop()).thenReturn(loop);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.doAnswer(invocation -> {
      latch.countDown();
      return null;
    }).when(listener).operationComplete(Mockito.eq(promise));

    Assert.assertTrue(future.completeExceptionally(new ErrorCause()));
    latch.await(1, TimeUnit.SECONDS);
    Mockito.verify(listener).operationComplete(Mockito.same(promise));
    Mockito.verifyNoMoreInteractions(listener);
  }

  @Test(groups = "unit")
  public void testVoid() {
    CompletableFuture<Channel> future = new CompletableFuture<>();
    ChannelPromise promise = new PendingConnectPromise(future);
    Assert.assertSame(promise.unvoid(), promise);
    Assert.assertFalse(promise.isVoid());
  }
}
