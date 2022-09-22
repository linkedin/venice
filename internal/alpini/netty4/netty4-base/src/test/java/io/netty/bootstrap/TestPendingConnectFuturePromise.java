package io.netty.bootstrap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/25/18.
 */
public class TestPendingConnectFuturePromise {
  @Test(groups = "unit")
  public void testGlobalExecutor() {
    Assert.assertSame(
        new PendingConnectFuturePromise(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE)).executor(),
        GlobalEventExecutor.INSTANCE);
  }

  @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
  public void testSetSuccess0() {
    new PendingConnectFuturePromise(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE)).setSuccess();
    Assert.fail();
  }

  @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
  public void testSetSuccess1() {
    new PendingConnectFuturePromise(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE)).setSuccess(null);
    Assert.fail();
  }

  @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
  public void testTrySuccess() {
    new PendingConnectFuturePromise(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE)).trySuccess();
    Assert.fail();
  }

  @Test(groups = "unit")
  public void testSetFailure0() {
    class Cause extends Exception {
    }
    ChannelPromise promise =
        new PendingConnectFuturePromise(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE)).setFailure(new Cause());
    Assert.assertTrue(promise.isDone());
    Assert.assertFalse(promise.isCancelled());
    Assert.assertTrue(promise.cause() instanceof Cause);
  }

  @Test(groups = "unit")
  public void testSetFailure1() {
    class Cause extends Exception {
    }
    ChannelPromise promise =
        new PendingConnectFuturePromise(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE)).setFailure(new Cause());

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
    new PendingConnectFuturePromise(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE)).setFailure(new ErrorCause())
        .syncUninterruptibly();
  }

  @Test(groups = "unit", expectedExceptions = ErrorCause.class)
  public void testSync() throws InterruptedException {
    new PendingConnectFuturePromise(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE)).setFailure(new ErrorCause())
        .sync();
  }

  @Test(groups = "unit")
  public void testAwaitUninterruptably() {
    ChannelPromise promise = new PendingConnectFuturePromise(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE))
        .setFailure(new ErrorCause())
        .awaitUninterruptibly();
    Assert.assertTrue(promise.isDone());
    Assert.assertFalse(promise.isCancelled());
    Assert.assertTrue(promise.cause() instanceof ErrorCause);
  }

  @Test(groups = "unit")
  public void testAwait() throws InterruptedException {
    ChannelPromise promise = new PendingConnectFuturePromise(new DefaultPromise<>(ImmediateEventExecutor.INSTANCE))
        .setFailure(new ErrorCause())
        .await();
    Assert.assertTrue(promise.isDone());
    Assert.assertFalse(promise.isCancelled());
    Assert.assertTrue(promise.cause() instanceof ErrorCause);
  }

  @Test(groups = "unit")
  public void testAddListener() throws Exception {
    Promise<Channel> future = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
    ChannelPromise promise = new PendingConnectFuturePromise(future);
    ChannelFutureListener listener = Mockito.mock(ChannelFutureListener.class);
    Assert.assertSame(promise.addListener(listener), promise);
    Mockito.verifyNoMoreInteractions(listener);

    Channel channel = Mockito.mock(Channel.class);
    EventLoop loop = Mockito.mock(EventLoop.class);
    Mockito.when(loop.inEventLoop()).thenReturn(true);
    Mockito.when(channel.eventLoop()).thenReturn(loop);

    Assert.assertTrue(future.trySuccess(channel));
    Mockito.verify(listener).operationComplete(Mockito.same(promise));
    Assert.assertSame(promise.sync().channel(), channel);
  }

  @Test(groups = "unit")
  public void testAddRemoveListeners() throws Exception {
    Promise<Channel> future = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
    ChannelPromise promise = new PendingConnectFuturePromise(future);
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

    Assert.assertTrue(future.trySuccess(channel));
    Assert.assertSame(promise.syncUninterruptibly(), promise);

    Mockito.verify(listener2).operationComplete(Mockito.same(promise));
    Mockito.verifyNoMoreInteractions(listener1, listener2, listener3, listener4);
  }

  @Test(groups = "unit")
  public void testListenerException() throws Exception {
    Promise<Channel> future = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
    ChannelPromise promise = new PendingConnectFuturePromise(future);
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

    Assert.assertTrue(future.tryFailure(new ErrorCause()));
    latch.await(1, TimeUnit.SECONDS);
    Mockito.verify(listener).operationComplete(Mockito.same(promise));
    Mockito.verifyNoMoreInteractions(listener);
  }

  @Test(groups = "unit")
  public void testVoid() {
    Promise<Channel> future = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
    ChannelPromise promise = new PendingConnectFuturePromise(future);
    Assert.assertSame(promise.unvoid(), promise);
    Assert.assertFalse(promise.isVoid());
  }
}
