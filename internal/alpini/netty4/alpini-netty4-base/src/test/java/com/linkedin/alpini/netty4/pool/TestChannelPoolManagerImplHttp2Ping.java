package com.linkedin.alpini.netty4.pool;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.linkedin.alpini.base.monitoring.CallTrackerImpl;
import com.linkedin.alpini.base.monitoring.NullCallTracker;
import com.linkedin.alpini.consts.QOS;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import com.linkedin.venice.utils.TestUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.util.concurrent.CompleteFuture;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/*
* This class is designed to test the Http2 ping mechanism implemented in ChannelPoolManagerImpl.
*
* @author Binbing Hou <bhou@linkedin.com>
* */
public class TestChannelPoolManagerImplHttp2Ping {
  @DataProvider
  public static Object[][] http2PingModes() {
    return new Object[][] { // {useHttp2Ping, useGlobalPool, isPoolClosing}
        { true, true, false }, { false, true, false }, { true, false, false }, { false, false, false },
        { true, true, true }, { false, true, true }, { true, false, true }, { false, false, true }, };
  }

  @Test(groups = "unit", dataProvider = "http2PingModes", invocationTimeOut = 10000L)
  public void http2PingTest(boolean enableHttp2Ping, boolean useGlobalPool, boolean isPoolClosing)
      throws InterruptedException {

    final int numOfExecutors = 4;

    EventLoopGroup eventLoopGroup = NettyUtils.newEventLoopGroup(numOfExecutors, Executors.defaultThreadFactory());
    try {
      http2PingTest(enableHttp2Ping, useGlobalPool, isPoolClosing, numOfExecutors, eventLoopGroup);
      eventLoopGroup.shutdownGracefully().sync();
    } finally {
      // Must shutdown threads even in the event an exception occurs or else gradle test will not complete
      eventLoopGroup.shutdownNow();
    }
  }

  private void http2PingTest(
      boolean enableHttp2Ping,
      boolean useGlobalPool,
      boolean isPoolClosing,
      final int numOfExecutors,
      final EventLoopGroup eventLoopGroup) throws InterruptedException {
    ChannelPoolResolver channelPoolResolver = new BasicDnsResolver();
    EventExecutor eventExecutor = ImmediateEventExecutor.INSTANCE;

    ChannelPoolFactory channelPoolFactory = Mockito.mock(ChannelPoolFactory.class);
    ManagedChannelPool parentPool = prepareParentPool(true, eventExecutor, isPoolClosing);
    Http2AwareChannelPool pool = mock(
        Http2AwareChannelPool.class,
        withSettings().useConstructor(parentPool, (Consumer<Channel>) ch -> {}, (Consumer<Channel>) ch -> {})
            .defaultAnswer(CALLS_REAL_METHODS));
    Mockito.doAnswer(invocation -> pool)
        .when(channelPoolFactory)
        .construct(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    ChannelPoolManagerImpl manager = mock(
        ChannelPoolManagerImpl.class,
        withSettings()
            .useConstructor(
                eventLoopGroup,
                channelPoolFactory,
                channelPoolResolver,
                100,
                useGlobalPool,
                false,
                false,
                enableHttp2Ping ? 1 : 0)
            .defaultAnswer(CALLS_REAL_METHODS));

    Channel serverChannel = dummyServer(eventLoopGroup);
    InetSocketAddress serverAddress = (InetSocketAddress) serverChannel.localAddress();
    Future<Channel> channelFuture = manager.acquire("localhost:" + serverAddress.getPort(), "queueName", QOS.NORMAL);
    Assert.assertTrue(channelFuture.await().isSuccess());
    Assert.assertNotNull(channelFuture.await().getNow());

    manager.startPeriodicPing();
    ChannelPoolManager.PoolStats poolStats = ((ChannelPoolManager.PoolStats) manager.getPools().toArray()[0]);
    ;

    if (enableHttp2Ping) {
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(manager.enablePeriodicPing());
        Assert.assertNotNull(manager.getPeriodicPingScheduledFuture());
        Assert.assertEquals(manager.getPools().size(), 1);
        Assert.assertTrue(poolStats.http2PingCallTracker() instanceof CallTrackerImpl);
        Assert.assertEquals(poolStats.getAvgResponseTimeOfLatestPings(), 0D);
        Mockito.verify(channelPoolFactory, useGlobalPool ? Mockito.times(1) : Mockito.times(numOfExecutors))
            .construct(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
      });
    } else {
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
        Assert.assertFalse(manager.enablePeriodicPing());
        Assert.assertNull(manager.getPeriodicPingScheduledFuture());
        Assert.assertEquals(manager.getPools().size(), 1);
        Assert.assertEquals(poolStats.http2PingCallTracker(), NullCallTracker.INSTANCE);
        Assert.assertEquals(poolStats.getAvgResponseTimeOfLatestPings(), 0D);
        Mockito.verify(channelPoolFactory, useGlobalPool ? Mockito.times(1) : Mockito.times(numOfExecutors))
            .construct(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
      });
    }

    // If the http2 ping is not enabled or the pool is in the closing state, we do not acquire connections
    // to send http2 ping. In particular, for the local pool impl, it calls pool.acquire() 4 times
    // for pool initialization.
    if (!enableHttp2Ping || isPoolClosing) {
      TestUtils.waitForNonDeterministicAssertion(
          1,
          TimeUnit.SECONDS,
          () -> Mockito.verify(pool, useGlobalPool ? Mockito.never() : Mockito.times(numOfExecutors)).acquire());
    }

    // reset
    manager.stopPeriodicPing();

    Mockito.reset(manager, channelPoolFactory, pool);
  }

  private <E extends MultithreadEventLoopGroup> Channel dummyServer(EventLoopGroup eventLoopGroup) {
    ServerBootstrap bootstrap = new ServerBootstrap().channel(NettyUtils.serverSocketChannel())
        .group(eventLoopGroup)
        .localAddress(InetAddress.getLoopbackAddress(), 0)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
          }
        });

    return bootstrap.bind().syncUninterruptibly().channel();
  }

  private Future<Channel> makeFuture(boolean successfulAcquire, EventExecutor eventExecutor) {

    return new CompleteFuture<Channel>(eventExecutor) {
      @Override
      public boolean isSuccess() {
        return successfulAcquire;
      }

      @Override
      public Throwable cause() {
        return null;
      }

      @Override
      public Channel getNow() {
        EmbeddedChannel channel = new EmbeddedChannel();
        // Since the test generates a channel synthetically, this needs to be set up.
        channel.attr(Http2AwareChannelPool.STREAM_GROUP).set(new DefaultChannelGroup(channel.eventLoop(), true));
        channel.pipeline().addLast(Http2FrameCodecBuilder.forClient().build());
        ChannelHandler multiplexHandler = new Http2MultiplexHandler(mock(ChannelHandler.class));
        channel.pipeline().addLast(multiplexHandler);
        return channel;
      }
    };
  }

  private ManagedChannelPool prepareParentPool(
      boolean successfulAcquire,
      EventExecutor eventExecutor,
      boolean isPoolClosing) {
    Future<Channel> future = makeFuture(successfulAcquire, eventExecutor);
    ManagedChannelPool parentPool = mock(ManagedChannelPool.class);
    ChannelPoolHandler channelPoolHandler = mock(ChannelPoolHandler.class);
    when(parentPool.isHealthy()).thenReturn(true);
    when(parentPool.isClosing()).thenReturn(isPoolClosing);
    when(parentPool.acquire()).thenReturn(future);
    when(parentPool.handler()).thenReturn(channelPoolHandler);
    when(parentPool.name()).thenReturn("pool");
    return parentPool;
  }
}
