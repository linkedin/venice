package com.linkedin.alpini.netty4.http2;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestSimpleChannelPromiseAggregator {
  public void basicSuccess1() {
    Channel channel = Mockito.mock(Channel.class);
    ChannelPromise promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    SimpleChannelPromiseAggregator aggregator =
        new SimpleChannelPromiseAggregator(promise, channel, ImmediateEventExecutor.INSTANCE);
    ChannelPromise a = aggregator.newPromise();
    ChannelPromise b = aggregator.newPromise();
    a.setSuccess();
    b.setSuccess();
    Assert.assertFalse(promise.isDone());
    aggregator.doneAllocatingPromises();
    Assert.assertTrue(promise.isSuccess());
  }

  public void basicSuccess2() {
    Channel channel = Mockito.mock(Channel.class);
    ChannelPromise promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    SimpleChannelPromiseAggregator aggregator =
        new SimpleChannelPromiseAggregator(promise, channel, ImmediateEventExecutor.INSTANCE);
    ChannelPromise a = aggregator.newPromise();
    ChannelPromise b = aggregator.newPromise();
    a.setSuccess();
    aggregator.doneAllocatingPromises();
    Assert.assertFalse(promise.isDone());
    b.setSuccess();
    Assert.assertTrue(promise.isSuccess());
  }

  public void basicSuccess3() {
    Channel channel = Mockito.mock(Channel.class);
    ChannelPromise promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    SimpleChannelPromiseAggregator aggregator =
        new SimpleChannelPromiseAggregator(promise, channel, ImmediateEventExecutor.INSTANCE);
    ChannelPromise a = aggregator.newPromise();
    ChannelPromise b = aggregator.newPromise();
    Assert.assertTrue(a.trySuccess());
    Assert.assertTrue(b.trySuccess());
    Assert.assertFalse(promise.isDone());
    aggregator.doneAllocatingPromises();
    Assert.assertTrue(promise.isSuccess());
    Assert.assertFalse(a.trySuccess());
  }

  public void basicSuccess4() {
    Channel channel = Mockito.mock(Channel.class);
    ChannelPromise promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    SimpleChannelPromiseAggregator aggregator =
        new SimpleChannelPromiseAggregator(promise, channel, ImmediateEventExecutor.INSTANCE);
    ChannelPromise a = aggregator.newPromise();
    ChannelPromise b = aggregator.newPromise();
    Assert.assertTrue(a.trySuccess());
    aggregator.doneAllocatingPromises();
    Assert.assertFalse(promise.isDone());
    Assert.assertTrue(b.trySuccess());
    Assert.assertTrue(promise.isSuccess());
    Assert.assertFalse(a.trySuccess());
  }

  public void basicFailure1() {
    Exception ex = new NullPointerException();
    Channel channel = Mockito.mock(Channel.class);
    ChannelPromise promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    SimpleChannelPromiseAggregator aggregator =
        new SimpleChannelPromiseAggregator(promise, channel, ImmediateEventExecutor.INSTANCE);
    ChannelPromise a = aggregator.newPromise();
    ChannelPromise b = aggregator.newPromise();
    a.setSuccess();
    b.setFailure(ex);
    Assert.assertFalse(promise.isDone());
    aggregator.doneAllocatingPromises();
    Assert.assertTrue(promise.isDone());
    Assert.assertFalse(promise.isSuccess());
    Assert.assertSame(promise.cause(), ex);
  }

  public void basicFailure2() {
    Exception ex = new NullPointerException();
    Channel channel = Mockito.mock(Channel.class);
    ChannelPromise promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    SimpleChannelPromiseAggregator aggregator =
        new SimpleChannelPromiseAggregator(promise, channel, ImmediateEventExecutor.INSTANCE);
    ChannelPromise a = aggregator.newPromise();
    ChannelPromise b = aggregator.newPromise();
    a.setSuccess();
    aggregator.doneAllocatingPromises();
    Assert.assertFalse(promise.isDone());
    b.setFailure(ex);
    Assert.assertTrue(promise.isDone());
    Assert.assertFalse(promise.isSuccess());
    Assert.assertSame(promise.cause(), ex);
  }

  public void basicFailure3() {
    Exception ex = new NullPointerException();
    Channel channel = Mockito.mock(Channel.class);
    ChannelPromise promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    SimpleChannelPromiseAggregator aggregator =
        new SimpleChannelPromiseAggregator(promise, channel, ImmediateEventExecutor.INSTANCE);
    ChannelPromise a = aggregator.newPromise();
    ChannelPromise b = aggregator.newPromise();
    Assert.assertTrue(a.trySuccess());
    Assert.assertTrue(b.tryFailure(ex));
    Assert.assertFalse(promise.isDone());
    aggregator.doneAllocatingPromises();
    Assert.assertTrue(promise.isDone());
    Assert.assertFalse(promise.isSuccess());
    Assert.assertSame(promise.cause(), ex);
    Assert.assertFalse(a.tryFailure(ex));
  }

  public void basicFailure4() {
    Exception ex = new NullPointerException();
    Channel channel = Mockito.mock(Channel.class);
    ChannelPromise promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    SimpleChannelPromiseAggregator aggregator =
        new SimpleChannelPromiseAggregator(promise, channel, ImmediateEventExecutor.INSTANCE);
    ChannelPromise a = aggregator.newPromise();
    ChannelPromise b = aggregator.newPromise();
    Assert.assertTrue(a.trySuccess());
    aggregator.doneAllocatingPromises();
    Assert.assertFalse(promise.isDone());
    Assert.assertTrue(b.tryFailure(ex));
    Assert.assertTrue(promise.isDone());
    Assert.assertFalse(promise.isSuccess());
    Assert.assertSame(promise.cause(), ex);
    Assert.assertFalse(a.tryFailure(ex));
  }

}
